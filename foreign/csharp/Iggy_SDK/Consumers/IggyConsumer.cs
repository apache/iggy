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

public partial class IggyConsumer : IAsyncDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly ILogger<IggyConsumer> _logger;
    private bool _isInitialized;
    private readonly Channel<ReceivedMessage> _channel;
    private readonly ConcurrentDictionary<int, ulong> _lastPolledOffset = new ();
    private bool _disposed;
    private string? _consumerGroupName;
    private long _lastPolledAtMs;

    /// <summary>
    /// Fired when an error occurs during message polling
    /// </summary>
    public event EventHandler<ConsumerErrorEventArgs>? OnPollingError;

    /// <summary>
    /// Fired when message decryption fails
    /// </summary>
    public event EventHandler<MessageDecryptionFailedEventArgs>? OnMessageDecryptionFailed;

    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILogger<IggyConsumer> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;

        _channel = Channel.CreateBounded<ReceivedMessage>(new BoundedChannelOptions((int)_config.BatchSize)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        if (_config.CreateIggyClient)
        {
            await _client.LoginUser(_config.Login, _config.Password, ct);
        }

        await InitializeConsumerGroupAsync(ct);

        _isInitialized = true;
    }
    
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

    public async Task StoreOffsetAsync(ulong offset, uint partitionId, CancellationToken ct = default)
    {
        await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, offset, partitionId, ct);
    }

    public async Task DeleteOffsetAsync(uint partitionId, CancellationToken ct = default)
    {
        await _client.DeleteOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, partitionId, ct); 
    }

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

                await _client.JoinConsumerGroupAsync(_config.StreamId, _config.TopicId, Identifier.String(_consumerGroupName), ct);

                LogConsumerGroupJoined(_consumerGroupName);
            }
        }
        catch (Exception ex)
        {
            LogFailedToInitializeConsumerGroup(ex, _consumerGroupName);
            throw;
        }
    }

    private async Task<bool> TryCreateConsumerGroupAsync(string groupName, Identifier groupId,  CancellationToken ct)
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
                        OnMessageDecryptionFailed?.Invoke(this,
                            new MessageDecryptionFailedEventArgs(ex, new ReceivedMessage()
                            {
                                Message = message,
                                CurrentOffset = message.Header.Offset,
                                PartitionId = (uint)messages.PartitionId
                            }));
                        continue;
                    }
                }

                var receivedMessage = new ReceivedMessage()
                {
                    Message = processedMessage,
                    CurrentOffset = processedMessage.Header.Offset,
                    PartitionId = (uint)messages.PartitionId
                };
                
                if (!_channel.Writer.TryWrite(receivedMessage))
                {
                    break;
                }
                
                _lastPolledOffset[messages.PartitionId] = messages.Messages[^1].Header.Offset;
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
