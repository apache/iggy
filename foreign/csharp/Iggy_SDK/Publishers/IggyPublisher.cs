using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

public partial class IggyPublisher : IAsyncDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<IggyPublisher> _logger;
    private bool _isInitialized = false;

    private readonly BackgroundMessageProcessor? _backgroundProcessor;
    private bool _disposed = false;

    /// <summary>
    /// Fired when any error occurs in the background task
    /// </summary>
    public event EventHandler<PublisherErrorEventArgs>? OnBackgroundError
    {
        add
        {
            if (_backgroundProcessor != null)
            {
                _backgroundProcessor.OnBackgroundError += value;
            }
        }
        remove
        {
            if (_backgroundProcessor != null)
            {
                _backgroundProcessor.OnBackgroundError -= value;
            }
        }
    }

    /// <summary>
    /// Fired when a batch of messages fails to send
    /// </summary>
    public event EventHandler<MessageBatchFailedEventArgs>? OnMessageBatchFailed
    {
        add
        {
            if (_backgroundProcessor != null)
            {
                _backgroundProcessor.OnMessageBatchFailed += value;
            }
        }
        remove
        {
            if (_backgroundProcessor != null)
            {
                _backgroundProcessor.OnMessageBatchFailed -= value;
            }
        }
    }

    public IggyPublisher(IIggyClient client, IggyPublisherConfig config, ILogger<IggyPublisher> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;

        if (_config.EnableBackgroundSending)
        {
            LogInitializingBackgroundSending(_config.BackgroundQueueCapacity, _config.BackgroundBatchSize);

            var processorLogger = _config.LoggerFactory?.CreateLogger<BackgroundMessageProcessor>()
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<BackgroundMessageProcessor>.Instance;

            _backgroundProcessor = new BackgroundMessageProcessor(_client, _config, processorLogger);
        }
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            LogPublisherAlreadyInitialized();
            return;
        }

        LogInitializingPublisher(_config.StreamId, _config.TopicId);

        await _client.LoginUser(_config.Login, _config.Password, ct);
        LogUserLoggedIn(_config.Login);

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
            await _client.CreateStreamAsync(_config.StreamId.GetString(), null, ct);
        }
        else
        {
            await _client.CreateStreamAsync(_config.StreamName, _config.StreamId.GetUInt32(), ct);
        }

        LogStreamCreated(_config.StreamId);
    }

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
                _config.TopicPartitionsCount, _config.TopicCompressionAlgorithm, null,
                _config.TopicReplicationFactor, _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }
        else
        {
            await _client.CreateTopicAsync(_config.StreamId, _config.TopicName, _config.TopicPartitionsCount,
                _config.TopicCompressionAlgorithm, _config.TopicId.GetUInt32(), _config.TopicReplicationFactor,
                _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }

        LogTopicCreated(_config.TopicId, _config.StreamId);
    }

    public async Task SendMessages(Message[] messages, CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            LogSendBeforeInitialization();
            throw new PublisherNotInitializedException();
        }

        if (messages.Length == 0)
        {
            return;
        }

        EncryptMessages(messages);

        if (_config.EnableBackgroundSending && _backgroundProcessor != null)
        {
            LogQueuingMessages(messages.Length);
            foreach (var message in messages)
            {
                await _backgroundProcessor.MessageWriter.WriteAsync(message, ct);
            }
        }
        else
        {
            await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messages, ct);
            LogSuccessfullySentMessages(messages.Length);
        }
    }

    public async Task WaitUntilAllSends(CancellationToken ct = default)
    {
        if (!_config.EnableBackgroundSending || _backgroundProcessor == null)
        {
            return;
        }

        LogWaitingForPendingMessages();

        while (_backgroundProcessor.MessageReader.Count > 0 && !ct.IsCancellationRequested)
        {
            await Task.Delay(10, ct);
        }

        LogAllPendingMessagesSent();
    }

    private void EncryptMessages(Message[] messages)
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
}
