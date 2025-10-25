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
    private readonly ConcurrentDictionary<int, ulong> _lastPolledOffset = new();
    private readonly ILogger<IggyConsumer> _logger;
    private string? _consumerGroupName;
    private bool _disposed;
    private bool _isInitialized;
    private long _lastPolledAtMs;

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyConsumer" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Consumer configuration settings</param>
    /// <param name="logger">Logger instance for diagnostic output</param>
    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILogger<IggyConsumer> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;

        _channel = Channel.CreateUnbounded<ReceivedMessage>();
    }

    /// <summary>
    ///     Disposes the consumer, leaving consumer groups and logging out if applicable
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
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

        _disposed = true;
    }

    /// <summary>
    ///     Fired when an error occurs during message polling
    /// </summary>
    public event EventHandler<ConsumerErrorEventArgs>? OnPollingError;

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

        if (_config.Consumer.Type == ConsumerType.ConsumerGroup && _config.PartitionId != null)
        {
            _logger.LogWarning("PartitionId is ignored when ConsumerType is ConsumerGroup");
            _config.PartitionId = null;
        }

        if (_config.CreateIggyClient)
        {
            await _client.LoginUser(_config.Login, _config.Password, ct);
        }

        await InitializeConsumerGroupAsync(ct);

        _isInitialized = true;
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
                await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                    message.CurrentOffset, message.PartitionId, ct);
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
    ///     Initializes consumer group if configured, creating and joining as needed
    /// </summary>
    private async Task InitializeConsumerGroupAsync(CancellationToken ct)
    {
        if (_config.Consumer.Type == ConsumerType.Consumer)
        {
            return;
        }

        _consumerGroupName = _config.Consumer.Id.Kind == IdKind.String
            ? _config.Consumer.Id.GetString()
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

                var createdGroup = await TryCreateConsumerGroupAsync(_consumerGroupName, _config.Consumer.Id, ct);

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
    private async Task<bool> TryCreateConsumerGroupAsync(string groupName, Identifier groupId, CancellationToken ct)
    {
        try
        {
            uint? id = groupId.Kind == IdKind.Numeric ? groupId.GetUInt32() : null;
            await _client.CreateConsumerGroupAsync(_config.StreamId, _config.TopicId,
                groupName, id, ct);
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
    /// </summary>
    private async Task PollMessagesAsync(CancellationToken ct)
    {
        try
        {
            if (_config.PollingIntervalMs > 0)
            {
                await WaitBeforePollingAsync(ct);
            }

            var messages = await _client.PollMessagesAsync(_config.StreamId, _config.TopicId,
                _config.PartitionId, _config.Consumer, _config.PollingStrategy, _config.BatchSize,
                _config.AutoCommit, ct);

            if (_lastPolledOffset.TryGetValue(messages.PartitionId, out var value))
            {
                messages.Messages = messages.Messages.Where(x => x.Header.Offset > value).ToList();
            }

            if (!messages.Messages.Any())
            {
                return;
            }

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

                _lastPolledOffset[messages.PartitionId] = message.Header.Offset;
            }

            if (_config.AutoCommitMode == AutoCommitMode.AfterPoll)
            {
                await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                    _lastPolledOffset[messages.PartitionId], (uint)messages.PartitionId, ct);
            }

            if (_config.PollingStrategy.Kind == MessagePolling.Offset)
            {
                _config.PollingStrategy = PollingStrategy.Offset(_lastPolledOffset[messages.PartitionId] + 1);
            }
        }
        catch (Exception ex)
        {
            LogFailedToPollMessages(ex);
            OnPollingError?.Invoke(this, new ConsumerErrorEventArgs(ex, "Failed to poll messages"));
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
}
