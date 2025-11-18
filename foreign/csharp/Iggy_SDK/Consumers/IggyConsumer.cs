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

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

/// <summary>
///     High-level consumer for receiving messages from Iggy streams.
///     Provides automatic polling, offset management, and consumer group support.
/// </summary>
public partial class IggyConsumer : IAsyncDisposable
{
    private readonly Channel<ReceivedMessage> _channel;
    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly SemaphoreSlim _connectionStateSemaphore = new(1, 1);
    private readonly EventAggregator<ConsumerErrorEventArgs> _consumerErrorEvents;
    private readonly ConcurrentDictionary<int, ulong> _lastPolledOffset = new();
    private readonly ILogger<IggyConsumer> _logger;
    private readonly SemaphoreSlim _pollingSemaphore = new(1, 1);
    private string? _consumerGroupName;
    private int _disposeState;
    private volatile bool _isInitialized;
    private volatile bool _joinedConsumerGroup;
    private long _lastPolledAtMs;

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyConsumer" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Consumer configuration settings</param>
    /// <param name="loggerFactory">Logger for creating loggers</param>
    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _logger = loggerFactory.CreateLogger<IggyConsumer>();

        _channel = Channel.CreateUnbounded<ReceivedMessage>();
        _consumerErrorEvents = new EventAggregator<ConsumerErrorEventArgs>(loggerFactory);
    }

    /// <summary>
    ///     Disposes the consumer, leaving consumer groups and logging out if applicable
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) == 1)
        {
            return;
        }

        if (_isInitialized)
        {
            _client.UnsubscribeConnectionEvents(OnClientConnectionStateChanged);
        }

        if (!string.IsNullOrEmpty(_consumerGroupName) && _isInitialized)
        {
            try
            {
                await _client.LeaveConsumerGroupAsync(_config.StreamId, _config.TopicId,
                    Identifier.String(_consumerGroupName));

                LogLeftConsumerGroup(_consumerGroupName);
            }
            catch (Exception e)
            {
                LogFailedToLeaveConsumerGroup(e, _consumerGroupName);
            }
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

        _consumerErrorEvents.Clear();
        _pollingSemaphore.Dispose();
        _connectionStateSemaphore.Dispose();

    }

    /// <summary>
    ///     Initializes the consumer by logging in (if needed) and setting up consumer groups
    /// </summary>
    /// <param name="ct">Cancellation token</param>
    /// <exception cref="InvalidConsumerGroupNameException">Thrown when consumer group name is invalid</exception>
    /// <exception cref="ConsumerGroupNotFoundException">Thrown when consumer group doesn't exist and auto-creation is disabled</exception>
    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        await _connectionStateSemaphore.WaitAsync(ct);
        try
        {
            if (_isInitialized)
            {
                return;
            }

            if (_config.Consumer.Type == ConsumerType.ConsumerGroup && _config.PartitionId != null)
            {
                LogPartitionIdIsIgnoredWhenConsumerTypeIsConsumerGroup();
                _config.PartitionId = null;
            }

            await _client.ConnectAsync(ct);

            if (_config.CreateIggyClient)
            {
                await _client.LoginUser(_config.Login, _config.Password, ct);
            }

            await InitializeConsumerGroupAsync(ct);

            _client.SubscribeConnectionEvents(OnClientConnectionStateChanged);

            _isInitialized = true;
        }
        finally
        {
            _connectionStateSemaphore.Release();
        }
    }

    /// <summary>
    ///     Receives messages asynchronously from the consumer as an async stream.
    ///     Messages are automatically polled from the server and buffered in a bounded channel.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages</param>
    /// <returns>An async enumerable of received messages</returns>
    /// <exception cref="ConsumerNotInitializedException">Thrown when InitAsync has not been called</exception>
    public async IAsyncEnumerable<ReceivedMessage> ReceiveAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            throw new ConsumerNotInitializedException();
        }

        do
        {
            if (!_channel.Reader.TryRead(out var message))
            {
                await PollMessagesAsync(ct);
                continue;
            }

            yield return message;

            if (_config.AutoCommitMode == AutoCommitMode.AfterReceive)
            {
                await StoreOffsetAsync(message.CurrentOffset, message.PartitionId, ct);
            }
        } while (!ct.IsCancellationRequested);
    }

    /// <summary>
    ///     Manually stores the consumer offset for a specific partition.
    ///     Use this when auto-commit is disabled or when you need manual offset control.
    /// </summary>
    /// <param name="offset">The offset to store</param>
    /// <param name="partitionId">The partition ID</param>
    /// <param name="ct">Cancellation token</param>
    public async Task StoreOffsetAsync(ulong offset, uint partitionId, CancellationToken ct = default)
    {
        await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, offset, partitionId, ct);
    }

    /// <summary>
    ///     Deletes the stored consumer offset for a specific partition.
    ///     The next poll will start from the beginning or based on the polling strategy.
    /// </summary>
    /// <param name="partitionId">The partition ID</param>
    /// <param name="ct">Cancellation token</param>
    public async Task DeleteOffsetAsync(uint partitionId, CancellationToken ct = default)
    {
        await _client.DeleteOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, partitionId, ct);
    }

    /// <summary>
    ///     Event raised when an error occurs during polling.
    /// </summary>
    /// <param name="callback">Callback method</param>
    public void SubscribeToErrorEvents(Func<ConsumerErrorEventArgs, Task> callback)
    {
        _consumerErrorEvents.Subscribe(callback);
    }

    /// <summary>
    ///     Unsubscribe from error events
    /// </summary>
    /// <param name="callback"></param>
    public void UnsubscribeFromErrorEvents(Func<ConsumerErrorEventArgs, Task> callback)
    {
        _consumerErrorEvents.Unsubscribe(callback);
    }

    /// <summary>
    ///     Initializes consumer group if configured, creating and joining as needed
    /// </summary>
    private async Task InitializeConsumerGroupAsync(CancellationToken ct = default)
    {
        if (_joinedConsumerGroup)
        {
            return;
        }

        if (_config.Consumer.Type == ConsumerType.Consumer)
        {
            _joinedConsumerGroup = true;
            return;
        }

        _consumerGroupName = _config.Consumer.ConsumerId.Kind == IdKind.String
            ? _config.Consumer.ConsumerId.GetString()
            : _config.ConsumerGroupName;

        if (string.IsNullOrEmpty(_consumerGroupName))
        {
            throw new InvalidConsumerGroupNameException("Consumer group name is empty or null.");
        }

        try
        {
            var existingGroup = await _client.GetConsumerGroupByIdAsync(_config.StreamId, _config.TopicId,
                Identifier.String(_consumerGroupName), ct);

            if (existingGroup == null && _config.CreateConsumerGroupIfNotExists)
            {
                LogCreatingConsumerGroup(_consumerGroupName, _config.StreamId, _config.TopicId);

                var createdGroup = await TryCreateConsumerGroupAsync(_consumerGroupName, ct);

                if (createdGroup)
                {
                    LogConsumerGroupCreated(_consumerGroupName);
                }
            }
            else if (existingGroup == null)
            {
                throw new ConsumerGroupNotFoundException(_consumerGroupName);
            }

            if (_config.JoinConsumerGroup)
            {
                LogJoiningConsumerGroup(_consumerGroupName, _config.StreamId, _config.TopicId);

                await _client.JoinConsumerGroupAsync(_config.StreamId, _config.TopicId,
                    Identifier.String(_consumerGroupName), ct);

                _joinedConsumerGroup = true;
                LogConsumerGroupJoined(_consumerGroupName);
            }
        }
        catch (Exception ex)
        {
            LogFailedToInitializeConsumerGroup(ex, _consumerGroupName);
            throw;
        }
    }

    /// <summary>
    ///     Attempts to create a consumer group, handling the case where it already exists
    /// </summary>
    /// <returns>True if the group was created or already exists, false on error</returns>
    private async Task<bool> TryCreateConsumerGroupAsync(string groupName, CancellationToken ct)
    {
        try
        {
            await _client.CreateConsumerGroupAsync(_config.StreamId, _config.TopicId,
                groupName, ct);
        }
        catch (IggyInvalidStatusCodeException ex)
        {
            // 5004 - Consumer group already exists TODO: refactor errors
            if (ex.StatusCode != 5004)
            {
                LogFailedToCreateConsumerGroup(ex, groupName);
                return false;
            }

            return true;
        }

        return true;
    }

    /// <summary>
    ///     Polls messages from the server and writes them to the internal channel.
    ///     Handles decryption, offset tracking, and auto-commit logic.
    ///     Uses semaphore to ensure single concurrent polling operation.
    /// </summary>
    private async Task PollMessagesAsync(CancellationToken ct)
    {
        if (!_joinedConsumerGroup)
        {
            LogConsumerGroupNotJoinedYetSkippingPolling();
            return;
        }

        await _pollingSemaphore.WaitAsync(ct);

        try
        {
            if (_config.PollingIntervalMs > 0)
            {
                await WaitBeforePollingAsync(ct);
            }

            var messages = await _client.PollMessagesAsync(_config.StreamId, _config.TopicId,
                _config.PartitionId, _config.Consumer, _config.PollingStrategy, _config.BatchSize,
                _config.AutoCommit, ct);

            var receiveMessages = messages.Messages.Count > 0;

            if (_lastPolledOffset.TryGetValue(messages.PartitionId, out var lastPolledPartitionOffset))
            {
                messages.Messages = messages.Messages.Where(x => x.Header.Offset > lastPolledPartitionOffset).ToList();
            }

            if (messages.Messages.Count == 0
                && receiveMessages
                && _config.AutoCommitMode != AutoCommitMode.Disabled)
            {
                _logger.LogDebug("No new messages found, committing offset {Offset} for partition {PartitionId}",
                    lastPolledPartitionOffset, messages.PartitionId);
                await StoreOffsetAsync(lastPolledPartitionOffset, (uint)messages.PartitionId, ct);
            }

            if (messages.Messages.Count == 0)
            {
                return;
            }

            var currentOffset = 0ul;
            foreach (var message in messages.Messages)
            {
                var processedMessage = message;
                var status = MessageStatus.Success;
                Exception? error = null;

                if (_config.MessageEncryptor != null)
                {
                    try
                    {
                        var decryptedPayload = _config.MessageEncryptor.Decrypt(message.Payload);
                        processedMessage = new MessageResponse
                        {
                            Header = message.Header,
                            Payload = decryptedPayload,
                            UserHeaders = message.UserHeaders
                        };
                    }
                    catch (Exception ex)
                    {
                        LogFailedToDecryptMessage(ex, message.Header.Offset);
                        status = MessageStatus.DecryptionFailed;
                        error = ex;
                    }
                }

                var receivedMessage = new ReceivedMessage
                {
                    Message = processedMessage,
                    CurrentOffset = processedMessage.Header.Offset,
                    PartitionId = (uint)messages.PartitionId,
                    Status = status,
                    Error = error
                };

                await _channel.Writer.WriteAsync(receivedMessage, ct);
                currentOffset = receivedMessage.CurrentOffset;
            }

            _lastPolledOffset.AddOrUpdate(messages.PartitionId, currentOffset,
                (_, _) => currentOffset);

            if (_config.PollingStrategy.Kind == MessagePolling.Offset)
            {
                _config.PollingStrategy = PollingStrategy.Offset(currentOffset + 1);
            }
        }
        catch (Exception ex)
        {
            LogFailedToPollMessages(ex);
            _consumerErrorEvents.Publish(new ConsumerErrorEventArgs(ex, "Failed to poll messages"));
        }
        finally
        {
            _pollingSemaphore.Release();
        }
    }

    /// <summary>
    ///     Implements polling interval throttling to avoid excessive server requests.
    ///     Uses monotonic time tracking to ensure proper intervals even with clock adjustments.
    /// </summary>
    private async Task WaitBeforePollingAsync(CancellationToken ct)
    {
        var intervalMs = _config.PollingIntervalMs;
        if (intervalMs <= 0)
        {
            return;
        }

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lastPolledAtMs = Interlocked.Read(ref _lastPolledAtMs);

        if (nowMs < lastPolledAtMs)
        {
            LogMonotonicTimeWentBackwards(nowMs, lastPolledAtMs);
            await Task.Delay(intervalMs, ct);
            Interlocked.Exchange(ref _lastPolledAtMs, nowMs);
            return;
        }

        var elapsedMs = nowMs - lastPolledAtMs;
        if (elapsedMs >= intervalMs)
        {
            LogNoNeedToWaitBeforePolling(nowMs, lastPolledAtMs, elapsedMs);
            Interlocked.Exchange(ref _lastPolledAtMs, nowMs);
            return;
        }

        var remainingMs = intervalMs - elapsedMs;
        LogWaitingBeforePolling(remainingMs);

        if (remainingMs > 0)
        {
            await Task.Delay((int)remainingMs, ct);
        }

        Interlocked.Exchange(ref _lastPolledAtMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    /// <summary>
    ///     Handles connection state changes from the client.
    /// </summary>
    /// <param name="e">Event object</param>
    private async Task OnClientConnectionStateChanged(ConnectionStateChangedEventArgs e)
    {
        LogConnectionStateChanged(e.PreviousState, e.CurrentState);

        await _connectionStateSemaphore.WaitAsync();
        try
        {
            if (e.CurrentState == ConnectionState.Disconnected)
            {
                _joinedConsumerGroup = false;
            }

            if (e.CurrentState != ConnectionState.Authenticated
                || e.PreviousState == ConnectionState.Authenticated
                || _joinedConsumerGroup)
            {
                return;
            }

            await RejoinConsumerGroupOnReconnectionAsync();
        }
        finally
        {
            _connectionStateSemaphore.Release();
        }
    }

    /// <summary>
    ///     Asynchronously rejoins the consumer group after a client reconnection.
    ///     This restores the consumer group membership that was lost during the connection failure.
    /// </summary>
    private async Task RejoinConsumerGroupOnReconnectionAsync()
    {
        if (string.IsNullOrEmpty(_consumerGroupName))
        {
            LogConsumerGroupNameIsEmptySkippingRejoiningConsumerGroup();
            return;
        }

        try
        {
            await InitializeConsumerGroupAsync();
        }
        catch (Exception ex)
        {
            LogFailedToRejoinConsumerGroup(ex, _consumerGroupName);
            _consumerErrorEvents.Publish(new ConsumerErrorEventArgs(ex,
                "Failed to rejoin consumer group after reconnection"));
        }
    }
}
