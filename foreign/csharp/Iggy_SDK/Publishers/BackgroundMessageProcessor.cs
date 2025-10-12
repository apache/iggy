using System.Threading.Channels;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Internal background processor that handles asynchronous message batching and sending.
///     Reads messages from a bounded channel and sends them in batches with retry support.
/// </summary>
internal sealed partial class BackgroundMessageProcessor : IAsyncDisposable
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<BackgroundMessageProcessor> _logger;
    private readonly Channel<Message> _messageChannel;
    private Task? _backgroundTask;
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
    /// <param name="logger">Logger instance for diagnostic output.</param>
    public BackgroundMessageProcessor(IIggyClient client,
        IggyPublisherConfig config,
        ILogger<BackgroundMessageProcessor> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();

        var options = new BoundedChannelOptions(_config.BackgroundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };

        _messageChannel = Channel.CreateBounded<Message>(options);
        MessageWriter = _messageChannel.Writer;
        MessageReader = _messageChannel.Reader;
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

        _disposed = true;
    }

    /// <summary>
    ///     Event raised when an unexpected error occurs in the background processor.
    /// </summary>
    public event EventHandler<PublisherErrorEventArgs>? OnBackgroundError;

    /// <summary>
    ///     Event raised when a message batch fails to send after all retry attempts.
    /// </summary>
    public event EventHandler<MessageBatchFailedEventArgs>? OnMessageBatchFailed;

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
            OnBackgroundError?.Invoke(this,
                new PublisherErrorEventArgs(ex, "Unexpected error in background message processor"));
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
                OnMessageBatchFailed?.Invoke(this, new MessageBatchFailedEventArgs(ex, messageBatch.ToArray()));
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
                    delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * _config.RetryBackoffMultiplier,
                        _config.MaxRetryDelay.TotalMilliseconds));
                }
            }
        }

        LogFailedToSendBatchAfterRetries(lastException!, messageBatch.Count, _config.MaxRetryAttempts + 1);
        OnMessageBatchFailed?.Invoke(this,
            new MessageBatchFailedEventArgs(lastException!, messageBatch.ToArray(), _config.MaxRetryAttempts));
    }
}
