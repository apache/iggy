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
    private readonly ILogger<IggyPublisher> _logger;
    private bool _disposed;

    /// <summary>
    ///     Gets the identifier of the stream this publisher sends messages to.
    /// </summary>
    public Identifier StreamId => Config.StreamId;

    /// <summary>
    ///     Gets the identifier of the topic this publisher sends messages to.
    /// </summary>
    public Identifier TopicId => Config.TopicId;

    /// <summary>
    ///     Gets the background processor, when background sending is enabled. Exposed so the typed publisher can
    ///     queue values directly onto it.
    /// </summary>
    private protected BackgroundMessageProcessor? BackgroundProcessor { get; }

    /// <summary>Gets the underlying client, for derived publishers' direct (non-background) send paths.</summary>
    private protected IIggyClient Client { get; }

    /// <summary>Gets the publisher configuration, for derived publishers.</summary>
    private protected IggyPublisherConfig Config { get; }

    /// <summary>Gets a value indicating whether <see cref="InitAsync" /> has completed.</summary>
    private protected bool IsInitialized { get; private set; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyPublisher" /> class.
    /// </summary>
    /// <param name="client">The Iggy client to use for communication.</param>
    /// <param name="config">Publisher configuration settings.</param>
    /// <param name="logger">Logger instance for diagnostic output.</param>
    public IggyPublisher(IIggyClient client, IggyPublisherConfig config, ILogger<IggyPublisher> logger)
    {
        Client = client;
        Config = config;
        _logger = logger;

        if (Config.EnableBackgroundSending)
        {
            LogInitializingBackgroundSending(Config.BackgroundQueueCapacity, Config.BackgroundBatchSize);

            var loggerFactory = config.LoggerFactory ?? NullLoggerFactory.Instance;
            BackgroundProcessor = new BackgroundMessageProcessor(client, config, loggerFactory);
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

        if (BackgroundProcessor != null)
        {
            await BackgroundProcessor.DisposeAsync();
        }

        if (Config.CreateIggyClient && IsInitialized)
        {
            try
            {
                await Client.LogoutUserAsync();
                Client.Dispose();
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
    ///     Subscribe to background error events. A queued message whose serializer throws at flush time is dropped
    ///     and surfaced here, carrying the original values in
    ///     <see cref="PublisherErrorEventArgs.DroppedValues" />; transport and encryption failures are reported via
    ///     <see cref="SubscribeOnMessageBatchFailed" /> instead.
    /// </summary>
    /// <param name="handler">The handler to invoke on background errors</param>
    public void SubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        BackgroundProcessor?.SubscribeOnBackgroundError(handler);
    }

    /// <summary>
    ///     Unsubscribe from background error events
    /// </summary>
    /// <param name="handler">The handler to invoke on background errors</param>
    public void UnsubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        BackgroundProcessor?.UnsubscribeOnBackgroundError(handler);
    }

    /// <summary>
    ///     Subscribe to message batch failed events, raised when a batch fails to send over the transport or to
    ///     encrypt (after retries, if enabled) with a snapshot of the failed messages for inspection or re-send.
    ///     Serializer drops are reported via <see cref="SubscribeOnBackgroundError" /> instead.
    /// </summary>
    /// <param name="handler">The handler to invoke on message batch failures</param>
    public void SubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        BackgroundProcessor?.SubscribeOnMessageBatchFailed(handler);
    }

    /// <summary>
    ///     Unsubscribe from message batch failed events
    /// </summary>
    /// <param name="handler">The handler to invoke on message batch failures</param>
    public void UnsubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        BackgroundProcessor?.UnsubscribeOnMessageBatchFailed(handler);
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
        if (IsInitialized)
        {
            LogPublisherAlreadyInitialized();
            return;
        }

        await Client.ConnectAsync(ct);

        LogInitializingPublisher(Config.StreamId, Config.TopicId);
        if (Config.CreateIggyClient)
        {
            await Client.LoginUserAsync(Config.Login, Config.Password, ct);
            LogUserLoggedIn(Config.Login);
        }

        await CreateStreamIfNeededAsync(ct);
        await CreateTopicIfNeededAsync(ct);

        if (Config.EnableBackgroundSending)
        {
            BackgroundProcessor?.Start();
            LogBackgroundSendingStarted();
        }

        IsInitialized = true;
        LogPublisherInitialized();
    }

    /// <summary>
    ///     Creates the stream if it doesn't exist and auto-creation is enabled in the configuration.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <exception cref="StreamNotFoundException">Thrown when the stream doesn't exist and auto-creation is disabled.</exception>
    private async Task CreateStreamIfNeededAsync(CancellationToken ct)
    {
        if (await Client.GetStreamByIdAsync(Config.StreamId, ct) != null)
        {
            LogStreamAlreadyExists(Config.StreamId);
            return;
        }

        if (!Config.CreateStream || string.IsNullOrEmpty(Config.StreamName))
        {
            LogStreamDoesNotExist(Config.StreamId);
            throw new StreamNotFoundException(Config.StreamId);
        }

        LogCreatingStream(Config.StreamId, Config.StreamName);

        if (Config.StreamId.Kind is IdKind.String)
        {
            await Client.CreateStreamAsync(Config.StreamId.GetString(), ct);
        }
        else
        {
            await Client.CreateStreamAsync(Config.StreamName, ct);
        }

        LogStreamCreated(Config.StreamId);
    }

    /// <summary>
    ///     Creates the topic if it doesn't exist and auto-creation is enabled in the configuration.
    /// </summary>
    /// <param name="ct">Cancellation token to cancel the operation.</param>
    /// <exception cref="TopicNotFoundException">Thrown when the topic doesn't exist and auto-creation is disabled.</exception>
    private async Task CreateTopicIfNeededAsync(CancellationToken ct)
    {
        if (await Client.GetTopicByIdAsync(Config.StreamId, Config.TopicId, ct) != null)
        {
            LogTopicAlreadyExists(Config.TopicId, Config.StreamId);
            return;
        }

        if (!Config.CreateTopic || string.IsNullOrEmpty(Config.TopicName))
        {
            LogTopicDoesNotExist(Config.TopicId, Config.StreamId);
            throw new TopicNotFoundException(Config.TopicId, Config.StreamId);
        }

        LogCreatingTopic(Config.TopicId, Config.TopicName, Config.StreamId);

        if (Config.TopicId.Kind is IdKind.String)
        {
            await Client.CreateTopicAsync(Config.StreamId, Config.TopicId.GetString(),
                Config.TopicPartitionsCount, Config.TopicCompressionAlgorithm, Config.TopicReplicationFactor,
                Config.TopicMessageExpiry, Config.TopicMaxTopicSize, ct);
        }
        else
        {
            await Client.CreateTopicAsync(Config.StreamId, Config.TopicName, Config.TopicPartitionsCount,
                Config.TopicCompressionAlgorithm, Config.TopicReplicationFactor,
                Config.TopicMessageExpiry, Config.TopicMaxTopicSize, ct);
        }

        LogTopicCreated(Config.TopicId, Config.StreamId);
    }

    /// <summary>
    ///     Sends a collection of messages to the configured stream and topic.
    ///     If background sending is enabled, messages are queued for asynchronous processing.
    ///     Otherwise, messages are sent immediately.
    /// </summary>
    /// <remarks>
    ///     When the client is configured with an encryptor, payloads (and user headers) are encrypted at the wire
    ///     boundary while the outgoing buffer is serialized: the caller's <see cref="Message" /> instances are never
    ///     mutated and keep their plaintext. A retry re-encrypts from the plaintext with a fresh nonce.
    ///     <para>
    ///         With background sending enabled this overload queues the message references (not a payload copy),
    ///         so a payload backed by caller-owned pooled memory must stay valid until
    ///         <see cref="WaitUntilAllSendsAsync" /> returns. To queue a <see cref="RentedMessageBatch" />, call
    ///         <see cref="SendAsync(RentedMessageBatch, CancellationToken)" /> instead of passing
    ///         <see cref="RentedMessageBatch.Messages" /> here and disposing the batch before the drain completes.
    ///     </para>
    /// </remarks>
    /// <param name="messages">The messages to send.</param>
    /// <param name="ct">Cancellation token to cancel the send operation.</param>
    /// <exception cref="PublisherNotInitializedException">Thrown when attempting to send before initialization.</exception>
    public async Task SendMessagesAsync(IList<Message> messages, CancellationToken ct = default)
    {
        if (!IsInitialized)
        {
            LogSendBeforeInitialization();
            throw new PublisherNotInitializedException();
        }

        if (messages.Count == 0)
        {
            return;
        }

        if (Config.EnableBackgroundSending && BackgroundProcessor != null)
        {
            LogQueuingMessages(messages.Count);
            // Snapshot so a caller mutating the list after enqueue cannot change the batch read at flush time.
            await SendReadyAsync(messages.ToArray(), null, ct);
        }
        else
        {
            await SendReadyAsync(messages, null, ct);
            LogSuccessfullySentMessages(messages.Count);
        }
    }

    /// <summary>
    ///     Sends a <see cref="RentedMessageBatch" />, taking ownership of it: its pooled buffer is returned to the
    ///     pool once the batch has been sent (immediately, or after the background flush). The caller must not
    ///     dispose the batch.
    /// </summary>
    /// <param name="batch">The rented batch to send.</param>
    /// <param name="ct">Cancellation token to cancel the send operation.</param>
    /// <exception cref="PublisherNotInitializedException">Thrown when attempting to send before initialization.</exception>
    public async Task SendAsync(RentedMessageBatch batch, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(batch);

        if (!IsInitialized)
        {
            // Ownership already transferred; release the pooled buffer before failing.
            batch.Dispose();
            LogSendBeforeInitialization();
            throw new PublisherNotInitializedException();
        }

        IList<Message> messages = batch.Messages;
        if (messages.Count == 0)
        {
            batch.Dispose();
            return;
        }

        await SendReadyAsync(messages, batch, ct);
    }

    // Background: queued as one unit, owner disposed after its flush. Direct: sent now, owner disposed after.
    private async Task SendReadyAsync(IList<Message> messages, IDisposable? owner, CancellationToken ct)
    {
        if (Config.EnableBackgroundSending && BackgroundProcessor != null)
        {
            await BackgroundProcessor.EnqueueAsync(new ReadyUnit(messages, owner), ct);
        }
        else
        {
            try
            {
                await Client.SendMessagesAsync(Config.StreamId, Config.TopicId, Config.Partitioning, messages, ct);
            }
            finally
            {
                owner?.Dispose();
            }
        }
    }

    /// <summary>
    ///     Waits until all queued messages have been sent by the background processor.
    ///     Only applicable when background sending is enabled. Returns immediately otherwise.
    /// </summary>
    /// <remarks>
    ///     Flushing is gated on the connection being authenticated, so while the client is disconnected this waits
    ///     until the connection recovers (or <paramref name="ct" /> fires) - with the default token, potentially
    ///     forever. Pass a token with a timeout if the wait must be bounded.
    ///     <para>
    ///         Do not race this with <see cref="DisposeAsync" />: disposing the publisher discards any still-queued
    ///         messages, and a concurrent wait then throws <see cref="OperationCanceledException" /> rather than
    ///         reporting a clean drain. Await this to completion before disposing.
    ///     </para>
    /// </remarks>
    /// <param name="ct">Cancellation token to cancel the wait operation.</param>
    public async Task WaitUntilAllSendsAsync(CancellationToken ct = default)
    {
        if (!Config.EnableBackgroundSending || BackgroundProcessor == null)
        {
            return;
        }

        LogWaitingForPendingMessages();

        await BackgroundProcessor.WaitForDrainAsync(ct);

        LogAllPendingMessagesSent();
    }
}
