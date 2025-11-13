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

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Publishers;

/// <summary>
///     High-level publisher for sending messages to Iggy streams and topics.
///     Supports background message batching, automatic retry, encryption, and stream/topic auto-creation.
/// </summary>
public partial class IggyPublisher : IAsyncDisposable
{
    private readonly BackgroundMessageProcessor? _backgroundProcessor;
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<IggyPublisher> _logger;
    private bool _disposed;
    private bool _isInitialized;

    /// <summary>
    ///     Gets the identifier of the stream this publisher sends messages to.
    /// </summary>
    public Identifier StreamId => _config.StreamId;

    /// <summary>
    ///     Gets the identifier of the topic this publisher sends messages to.
    /// </summary>
    public Identifier TopicId => _config.TopicId;

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyPublisher" /> class.
    /// </summary>
    /// <param name="client">The Iggy client to use for communication.</param>
    /// <param name="config">Publisher configuration settings.</param>
    /// <param name="logger">Logger instance for diagnostic output.</param>
    public IggyPublisher(IIggyClient client, IggyPublisherConfig config, ILogger<IggyPublisher> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;

        if (_config.EnableBackgroundSending)
        {
            LogInitializingBackgroundSending(_config.BackgroundQueueCapacity, _config.BackgroundBatchSize);

            var loggerFactory = config.LoggerFactory ?? NullLoggerFactory.Instance;
            _backgroundProcessor = new BackgroundMessageProcessor(_client, _config, loggerFactory);
        }
    }

    /// <summary>
    ///     Disposes the publisher, stops the background processor if running,
    ///     and logs out and disposes the client if it was created by the publisher.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        LogDisposingPublisher();

        if (_backgroundProcessor != null)
        {

            await _backgroundProcessor.DisposeAsync();
        }

        if (_config.CreateIggyClient && _isInitialized)
        {
            try
            {
                await _client.LogoutUser();
                _client.Dispose();
            }
            catch (Exception e)
            {
                LogFailedToLogoutOrDispose(e);
            }
        }

        _disposed = true;
        LogPublisherDisposed();
    }

    /// <summary>
    ///     Subscribe to background error events
    /// </summary>
    /// <param name="handler">The handler to invoke on background errors</param>
    public void SubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundProcessor?.SubscribeOnBackgroundError(handler);
    }

    /// <summary>
    /// Unsubscribe from background error events
    /// </summary>
    /// <param name="handler">The handler to invoke on background errors</param>
    public void UnsubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundProcessor?.UnsubscribeOnBackgroundError(handler);
    }

    /// <summary>
    ///     Subscribe to message batch failed events
    /// </summary>
    /// <param name="handler">The handler to invoke on message batch failures</param>
    public void SubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _backgroundProcessor?.SubscribeOnMessageBatchFailed(handler);
    }

    /// <summary>
    /// Unsubscribe from message batch failed events
    /// </summary>
    /// <param name="handler">The handler to invoke on message batch failures</param>
    public void UnsubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _backgroundProcessor?.UnsubscribeOnMessageBatchFailed(handler);
    }

    /// <summary>
    ///     Initializes the publisher by authenticating, ensuring stream and topic exist,
    ///     and starting the background processor if enabled.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel initialization.</param>
    /// <exception cref="StreamNotFoundException">Thrown when the stream doesn't exist and auto-creation is disabled.</exception>
    /// <exception cref="TopicNotFoundException">Thrown when the topic doesn't exist and auto-creation is disabled.</exception>
    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            LogPublisherAlreadyInitialized();
            return;
        }

        await _client.ConnectAsync(ct);

        LogInitializingPublisher(_config.StreamId, _config.TopicId);
        if (_config.CreateIggyClient)
        {
            await _client.LoginUser(_config.Login, _config.Password, ct);
            LogUserLoggedIn(_config.Login);
        }

        await CreateStreamIfNeeded(ct);
        await CreateTopicIfNeeded(ct);

        if (_config.EnableBackgroundSending)
        {
            _backgroundProcessor?.Start();
            LogBackgroundSendingStarted();
        }

        _isInitialized = true;
        LogPublisherInitialized();
    }

    /// <summary>
    ///     Creates the stream if it doesn't exist and auto-creation is enabled in the configuration.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <exception cref="StreamNotFoundException">Thrown when the stream doesn't exist and auto-creation is disabled.</exception>
    private async Task CreateStreamIfNeeded(CancellationToken ct)
    {
        if (await _client.GetStreamByIdAsync(_config.StreamId, ct) != null)
        {
            LogStreamAlreadyExists(_config.StreamId);
            return;
        }

        if (!_config.CreateStream || string.IsNullOrEmpty(_config.StreamName))
        {
            LogStreamDoesNotExist(_config.StreamId);
            throw new StreamNotFoundException(_config.StreamId);
        }

        LogCreatingStream(_config.StreamId, _config.StreamName);

        if (_config.StreamId.Kind is IdKind.String)
        {
            await _client.CreateStreamAsync(_config.StreamId.GetString(), ct);
        }
        else
        {
            await _client.CreateStreamAsync(_config.StreamName, ct);
        }

        LogStreamCreated(_config.StreamId);
    }

    /// <summary>
    ///     Creates the topic if it doesn't exist and auto-creation is enabled in the configuration.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <exception cref="TopicNotFoundException">Thrown when the topic doesn't exist and auto-creation is disabled.</exception>
    private async Task CreateTopicIfNeeded(CancellationToken ct)
    {
        if (await _client.GetTopicByIdAsync(_config.StreamId, _config.TopicId, ct) != null)
        {
            LogTopicAlreadyExists(_config.TopicId, _config.StreamId);
            return;
        }

        if (!_config.CreateTopic || string.IsNullOrEmpty(_config.TopicName))
        {
            LogTopicDoesNotExist(_config.TopicId, _config.StreamId);
            throw new TopicNotFoundException(_config.TopicId, _config.StreamId);
        }

        LogCreatingTopic(_config.TopicId, _config.TopicName, _config.StreamId);

        if (_config.TopicId.Kind is IdKind.String)
        {
            await _client.CreateTopicAsync(_config.StreamId, _config.TopicId.GetString(),
                _config.TopicPartitionsCount, _config.TopicCompressionAlgorithm, _config.TopicReplicationFactor,
                _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }
        else
        {
            await _client.CreateTopicAsync(_config.StreamId, _config.TopicName, _config.TopicPartitionsCount,
                _config.TopicCompressionAlgorithm, _config.TopicReplicationFactor,
                _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }

        LogTopicCreated(_config.TopicId, _config.StreamId);
    }

    /// <summary>
    ///     Sends a collection of messages to the configured stream and topic.
    ///     If background sending is enabled, messages are queued for asynchronous processing.
    ///     Otherwise, messages are sent immediately.
    /// </summary>
    /// <param name="messages">The messages to send.</param>
    /// <param name="ct">Cancellation token to cancel the send operation.</param>
    /// <exception cref="PublisherNotInitializedException">Thrown when attempting to send before initialization.</exception>
    public async Task SendMessages(IList<Message> messages, CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            LogSendBeforeInitialization();
            throw new PublisherNotInitializedException();
        }

        if (messages.Count == 0)
        {
            return;
        }

        EncryptMessages(messages);

        if (_config.EnableBackgroundSending && _backgroundProcessor != null)
        {
            LogQueuingMessages(messages.Count);
            foreach (var message in messages)
            {
                await _backgroundProcessor.MessageWriter.WriteAsync(message, ct);
            }
        }
        else
        {
            await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messages, ct);
            LogSuccessfullySentMessages(messages.Count);
        }
    }

    /// <summary>
    ///     Waits until all queued messages have been sent by the background processor.
    ///     Only applicable when background sending is enabled. Returns immediately otherwise.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the wait operation.</param>
    public async Task WaitUntilAllSends(CancellationToken ct = default)
    {
        if (!_config.EnableBackgroundSending || _backgroundProcessor == null)
        {
            return;
        }

        LogWaitingForPendingMessages();

        while (_backgroundProcessor.MessageReader.Count > 0 ||
               _backgroundProcessor.IsSending)
        {
            await Task.Delay(10, ct);
        }

        LogAllPendingMessagesSent();
    }

    /// <summary>
    ///     Encrypts all messages in the list using the configured message encryptor, if available.
    ///     Updates the payload length in the message header after encryption.
    /// </summary>
    /// <param name="messages">The messages to encrypt.</param>
    private void EncryptMessages(IList<Message> messages)
    {
        if (_config.MessageEncryptor == null)
        {
            return;
        }

        foreach (var message in messages)
        {
            message.Payload = _config.MessageEncryptor.Encrypt(message.Payload);
            message.Header.PayloadLength = message.Payload.Length;
        }
    }
}
