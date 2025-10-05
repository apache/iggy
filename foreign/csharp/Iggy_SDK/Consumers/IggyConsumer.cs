using System.Runtime.CompilerServices;
using Apache.Iggy.Enums;
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
    private readonly BackgroundMessagePoller _backgroundPoller;
    private bool _disposed;

    /// <summary>
    /// Fired when an error occurs during message polling
    /// </summary>
    public event EventHandler<ConsumerErrorEventArgs>? OnPollingError
    {
        add => _backgroundPoller.OnPollingError += value;
        remove => _backgroundPoller.OnPollingError -= value;
    }

    /// <summary>
    /// Fired when message decryption fails
    /// </summary>
    public event EventHandler<MessageDecryptionFailedEventArgs>? OnMessageDecryptionFailed
    {
        add => _backgroundPoller.OnMessageDecryptionFailed += value;
        remove => _backgroundPoller.OnMessageDecryptionFailed -= value;
    }

    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILogger<IggyConsumer> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;

        var pollerLogger = _config.LoggerFactory?.CreateLogger<BackgroundMessagePoller>()
            ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BackgroundMessagePoller>.Instance;

        _backgroundPoller = new BackgroundMessagePoller(_client, _config, pollerLogger);
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

        _backgroundPoller.Start(ct);

        do
        {
            var message = await _backgroundPoller.MessageReader.ReadAsync(ct);

            yield return message;

            if (!_config.AutoCommit || _config.AutoCommitMode != AutoCommitMode.AfterReceive)
            {
                continue;
            }

            await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                message.CurrentOffset, message.PartitionId, ct);

        } while (!ct.IsCancellationRequested);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _backgroundPoller.DisposeAsync();

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

        _disposed = true;
    }
}
