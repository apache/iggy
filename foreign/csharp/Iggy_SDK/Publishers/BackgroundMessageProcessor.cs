// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Threading.Channels;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Internal background processor that handles asynchronous message batching and sending.
///     Reads messages from a bounded channel and sends them in batches with retry support.
/// </summary>
internal sealed partial class BackgroundMessageProcessor : IAsyncDisposable
{
    private readonly EventAggregator<PublisherErrorEventArgs> _backgroundErrorAggregator;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<BackgroundMessageProcessor> _logger;
    private readonly EventAggregator<MessageBatchFailedEventArgs> _messageBatchErrorAggregator;
    private Task? _backgroundTask;
    private bool _canSend = true;
    private bool _disposed;

    /// <summary>
    ///     Gets the channel writer for queuing messages to be sent.
    /// </summary>
    public ChannelWriter<Message> MessageWriter { get; }

    /// <summary>
    ///     Gets the channel reader for consuming messages from the queue.
    /// </summary>
    public ChannelReader<Message> MessageReader { get; }

    /// <summary>
    ///     Gets a value indicating whether the processor is currently sending messages.
    /// </summary>
    public bool IsSending { get; private set; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="BackgroundMessageProcessor" /> class.
    /// </summary>
    /// <param name="client">The Iggy client used to send messages.</param>
    /// <param name="config">Configuration settings for the publisher.</param>
    /// <param name="loggerFactory">Logger instance for diagnostic output.</param>
    public BackgroundMessageProcessor(IIggyClient client,
        IggyPublisherConfig config,
        ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _logger = loggerFactory.CreateLogger<BackgroundMessageProcessor>();
        _cancellationTokenSource = new CancellationTokenSource();

        var options = new BoundedChannelOptions(_config.BackgroundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };

        Channel<Message> messageChannel = Channel.CreateBounded<Message>(options);
        MessageWriter = messageChannel.Writer;
        MessageReader = messageChannel.Reader;

        _backgroundErrorAggregator = new EventAggregator<PublisherErrorEventArgs>(loggerFactory);
        _messageBatchErrorAggregator = new EventAggregator<MessageBatchFailedEventArgs>(loggerFactory);
        _client.SubscribeConnectionEvents(ClientOnOnConnectionStateChanged);
    }

    /// <summary>
    ///     Disposes the background processor, cancels ongoing operations,
    ///     and waits for the background task to complete.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _client.UnsubscribeConnectionEvents(ClientOnOnConnectionStateChanged);

        await _cancellationTokenSource.CancelAsync();

        if (_backgroundTask != null)
        {
            LogWaitingForBackgroundTask();
            try
            {
                await _backgroundTask.WaitAsync(_config.BackgroundDisposalTimeout);
                LogBackgroundTaskCompleted();
            }
            catch (TimeoutException)
            {
                LogBackgroundTaskTimeout();
            }
            catch (Exception e)
            {
                LogBackgroundProcessorError(e);
            }
        }

        MessageWriter.Complete();
        _cancellationTokenSource.Dispose();

        _backgroundErrorAggregator.Clear();
        _messageBatchErrorAggregator.Clear();
        _disposed = true;
    }

    /// <summary>
    ///     Starts the background message processing task.
    ///     Does nothing if the processor is already running.
    /// </summary>
    public void Start()
    {
        if (_backgroundTask != null)
        {
            return;
        }

        _backgroundTask = RunBackgroundProcessor(_cancellationTokenSource.Token);
        LogBackgroundProcessorStarted();
    }

    /// <summary>
    ///     Subscribe to background error events
    /// </summary>
    /// <param name="handler">Callback to handle the error event</param>
    public void SubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundErrorAggregator.Subscribe(handler);
    }

    /// <summary>
    ///     Unsubscribe from background error events
    /// </summary>
    /// <param name="handler">Callback to handle the error event</param>
    public void UnsubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundErrorAggregator.Unsubscribe(handler);
    }

    /// <summary>
    ///     Subscribe to message batch failed events
    /// </summary>
    /// <param name="handler">Callback to handle the message batch failed event</param>
    public void SubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _messageBatchErrorAggregator.Subscribe(handler);
    }

    /// <summary>
    ///     Unsubscribe from message batch failed events
    /// </summary>
    /// <param name="handler">Callback to handle the message batch failed event</param>
    public void UnsubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _messageBatchErrorAggregator.Unsubscribe(handler);
    }

    /// <summary>
    ///     Main background processing loop that reads messages from the channel,
    ///     batches them, and sends them periodically or when the batch size is reached.
    /// </summary>
    /// <param name="ct">Cancellation token to stop processing.</param>
    private async Task RunBackgroundProcessor(CancellationToken ct)
    {
        var messageBatch = new List<Message>(_config.BackgroundBatchSize);
        using var timer = new PeriodicTimer(_config.BackgroundFlushInterval);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!_canSend)
                {
                    LogClientIsDisconnected();
                    if (!await timer.WaitForNextTickAsync(ct))
                    {
                        break;
                    }

                    continue;
                }

                while (messageBatch.Count < _config.BackgroundBatchSize &&
                       MessageReader.TryRead(out var message))
                {
                    messageBatch.Add(message);
                    IsSending = true;
                }

                if (messageBatch.Count == 0)
                {
                    if (!await timer.WaitForNextTickAsync(ct))
                    {
                        break;
                    }

                    continue;
                }

                await SendBatchWithRetry(messageBatch, ct);
                messageBatch.Clear();
                IsSending = false;
            }
        }
        catch (OperationCanceledException)
        {
            LogBackgroundProcessorCancelled();
        }
        catch (Exception ex)
        {
            LogBackgroundProcessorError(ex);
            _backgroundErrorAggregator.Publish(new PublisherErrorEventArgs(ex,
                "Unexpected error in background message processor"));
        }
        finally
        {
            LogBackgroundProcessorStopped();
            IsSending = false;
        }
    }

    /// <summary>
    ///     Attempts to send a batch of messages with exponential backoff retry logic.
    /// </summary>
    /// <param name="messageBatch">The list of messages to send.</param>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    private async Task SendBatchWithRetry(List<Message> messageBatch, CancellationToken ct)
    {
        if (!_config.EnableRetry)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning,
                    messageBatch.ToArray(), ct);
            }
            catch (Exception ex)
            {
                LogFailedToSendBatch(ex, messageBatch.Count);
                _messageBatchErrorAggregator.Publish(new MessageBatchFailedEventArgs(ex, messageBatch.ToArray()));
            }

            return;
        }

        Exception? lastException = null;
        var delay = _config.InitialRetryDelay;

        for (var attempt = 0; attempt < _config.MaxRetryAttempts; attempt++)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning,
                    messageBatch.ToArray(), ct);
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt < _config.MaxRetryAttempts && !ct.IsCancellationRequested)
                {
                    LogRetryingBatch(ex, messageBatch.Count, attempt + 1, _config.MaxRetryAttempts + 1,
                        delay.TotalMilliseconds);
                    await Task.Delay(delay, ct);

                    var nextDelayMs = delay.TotalMilliseconds * _config.RetryBackoffMultiplier;

                    // Check for overflow or invalid values
                    if (double.IsInfinity(nextDelayMs) || double.IsNaN(nextDelayMs) ||
                        nextDelayMs > _config.MaxRetryDelay.TotalMilliseconds)
                    {
                        delay = _config.MaxRetryDelay;
                    }
                    else
                    {
                        // Ensure we don't exceed TimeSpan.MaxValue
                        delay = TimeSpan.FromMilliseconds(Math.Min(nextDelayMs, TimeSpan.MaxValue.TotalMilliseconds));
                    }
                }
            }
        }

        LogFailedToSendBatchAfterRetries(lastException!, messageBatch.Count, _config.MaxRetryAttempts + 1);
        _messageBatchErrorAggregator.Publish(new MessageBatchFailedEventArgs(lastException!,
            messageBatch.ToArray(), _config.MaxRetryAttempts));
    }

    private Task ClientOnOnConnectionStateChanged(ConnectionStateChangedEventArgs e)
    {
        if (e.CurrentState is ConnectionState.Disconnected
            or ConnectionState.Connecting
            or ConnectionState.Connected
            or ConnectionState.Authenticating)
        {
            _canSend = false;
        }

        if (e.CurrentState == ConnectionState.Authenticated)
        {
            _canSend = true;
        }

        return Task.CompletedTask;
    }

    private void ClientOnOnConnectionStateChanged(object? sender, ConnectionStateChangedEventArgs e)
    {
        if (e.CurrentState is ConnectionState.Disconnected
            or ConnectionState.Connecting
            or ConnectionState.Connected
            or ConnectionState.Authenticating)
        {
            _canSend = false;
        }

        if (e.CurrentState == ConnectionState.Authenticated)
        {
            _canSend = true;
        }
    }
}
