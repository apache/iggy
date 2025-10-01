using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer : IAsyncDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly ILogger<IggyConsumer> _logger;
    private bool _isInitialized;
    private readonly Channel<ReceivedMessage> _channel;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _pollingTask;
    private bool _disposed;

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
        _cancellationTokenSource = new CancellationTokenSource();
        _channel = Channel.CreateBounded<ReceivedMessage>(new BoundedChannelOptions(config.BufferSize)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }
    
    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        await _client.LoginUser(_config.Login, _config.Password, ct);

        await InitializeConsumerGroupAsync(ct);

        _isInitialized = true;
    }

    private async Task InitializeConsumerGroupAsync(CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_config.ConsumerGroupName))
        {
            return;
        }

        var groupName = _config.ConsumerGroupName;

        try
        {
            var existingGroup = await _client.GetConsumerGroupByIdAsync(_config.StreamId, _config.TopicId,
                Identifier.String(groupName), ct);

            if (existingGroup == null && _config.CreateConsumerGroupIfNotExists)
            {
                LogCreatingConsumerGroup(groupName, _config.StreamId, _config.TopicId);

                var createdGroup = await TryCreateConsumerGroupAsync(groupName, ct);

                if (createdGroup)
                {
                    LogConsumerGroupCreated(groupName);
                }
            }
            else if (existingGroup == null)
            {
                throw new ConsumerGroupNotFoundException(_config.ConsumerGroupName);
            }

            if (_config.JoinConsumerGroup)
            {
                LogJoiningConsumerGroup(groupName, _config.StreamId, _config.TopicId);

                await _client.JoinConsumerGroupAsync(_config.StreamId, _config.TopicId, Identifier.String(groupName), ct);

                LogConsumerGroupJoined(groupName);
            }
        }
        catch (Exception ex)
        {
            LogFailedToInitializeConsumerGroup(ex, groupName);
            throw;
        }
    }

    private async Task<bool> TryCreateConsumerGroupAsync(string groupName, CancellationToken ct)
    {
        try
        {
            await _client.CreateConsumerGroupAsync(_config.StreamId, _config.TopicId,
                groupName, token: ct);
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

    public async IAsyncEnumerable<ReceivedMessage> ReceiveAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            throw new ConsumerNotInitializedException();
        }

        if (_pollingTask == null || _pollingTask.IsCompleted)
        {
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cancellationTokenSource.Token);
            _pollingTask = PollMessagesAsync(linkedCts.Token);
        }

        do
        {
            var message = await _channel.Reader.ReadAsync(ct);

            yield return message;

            if (!_config.AutoCommit || _config.AutoCommitMode != AutoCommitMode.AfterReceive)
            {
                continue;
            }

            await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                message.CurrentOffset, message.PartitionId, ct);

        } while (!ct.IsCancellationRequested);
    }

    private async Task PollMessagesAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var messages = await _client.PollMessagesAsync(_config.StreamId, _config.TopicId,
                        _config.PartitionId, _config.Consumer, _config.PollingStrategy, _config.BatchSize,
                        _config.AutoCommit, ct);

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
                        await _channel.Writer.WriteAsync(receivedMessage, ct);
                    }

                    if (messages.Messages.Any() && _config.AutoCommitMode == AutoCommitMode.AfterPoll)
                    {
                        await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                            messages.Messages[^1].Header.Offset, (uint)messages.PartitionId, ct);
                    }
                }
                catch (Exception ex)
                {
                    LogFailedToPollMessages(ex);
                    OnPollingError?.Invoke(this, new ConsumerErrorEventArgs(ex, "Failed to poll messages"));
                }

                await Task.Delay(_config.PollingIntervalMs, ct);
            }

        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            LogPollingTaskCancelled();
        }
        finally
        {
            _channel.Writer.TryComplete();
            LogMessagePollingStopped();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _cancellationTokenSource.CancelAsync();

        if (_pollingTask != null && _isInitialized)
        {
            try
            {
                await _pollingTask.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (TimeoutException)
            {
                LogPollingTaskTimeout();
            }
            catch (Exception e)
            {
                LogPollingTaskFailed(e);
            }
        }

        if (!string.IsNullOrEmpty(_config.ConsumerGroupName) && _isInitialized)
        {
            try
            {
                await _client.LeaveConsumerGroupAsync(_config.StreamId, _config.TopicId,
                    Identifier.String(_config.ConsumerGroupName));

                LogLeftConsumerGroup(_config.ConsumerGroupName);
            }
            catch (Exception e)
            {
                LogFailedToLeaveConsumerGroup(e, _config.ConsumerGroupName);
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

        _cancellationTokenSource.Dispose();

        _disposed = true;
    }
}
