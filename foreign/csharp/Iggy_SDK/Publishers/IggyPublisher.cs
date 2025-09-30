using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace Apache.Iggy.Publishers;

public partial class IggyPublisher : IDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<IggyPublisher> _logger;
    private bool _isInitialized = false;

    private readonly Channel<Message>? _messageChannel;
    private readonly ChannelWriter<Message>? _messageWriter;
    private readonly ChannelReader<Message>? _messageReader;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _backgroundTask;
    private bool _disposed = false;

    /// <summary>
    /// Fired when any error occurs in the background task
    /// </summary>
    public event EventHandler<PublisherErrorEventArgs>? OnBackgroundError;
    
    /// <summary>
    /// Fired when a batch of messages fails to send
    /// </summary>
    public event EventHandler<MessageBatchFailedEventArgs>? OnMessageBatchFailed;

    public IggyPublisher(IIggyClient client, IggyPublisherConfig config, ILogger<IggyPublisher> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();

        if (_config.EnableBackgroundSending)
        {
            LogInitializingBackgroundSending(_config.BackgroundQueueCapacity, _config.BackgroundBatchSize);

            var options = new BoundedChannelOptions(_config.BackgroundQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            };

            _messageChannel = Channel.CreateBounded<Message>(options);
            _messageWriter = _messageChannel.Writer;
            _messageReader = _messageChannel.Reader;
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
            StartBackgroundTasks();
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
            throw new Exception($"Stream {_config.StreamId} not exists");
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
            throw new Exception($"Topic {_config.TopicId} does not exist");
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
            throw new InvalidOperationException("Publisher must be initialized before sending messages. Call InitAsync() first.");
        }

        EncryptMessages(messages);

        if (_config.EnableBackgroundSending && _messageWriter != null)
        {
            LogQueuingMessages(messages.Length);
            foreach (var message in messages)
            {
                await _messageWriter.WriteAsync(message, ct);
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
        if (!_config.EnableBackgroundSending || _messageReader == null)
        {
            return;
        }

        LogWaitingForPendingMessages();

        while (_messageReader.Count > 0 && !ct.IsCancellationRequested)
        {
            await Task.Delay(10, ct);
        }

        LogAllPendingMessagesSent();
    }

    private void StartBackgroundTasks()
    {
        if (_backgroundTask != null || _messageReader == null)
        {
            return;
        }

        _backgroundTask = BackgroundMessageProcessor(_cancellationTokenSource.Token);
    }

    private async Task BackgroundMessageProcessor(CancellationToken ct)
    {
        var messageBatch = new List<Message>(_config.BackgroundBatchSize);
        var timer = new PeriodicTimer(_config.BackgroundFlushInterval);

        LogBackgroundProcessorStarted();

        try
        {
            while (!ct.IsCancellationRequested && _messageReader != null)
            {
                while (messageBatch.Count < _config.BackgroundBatchSize &&
                       _messageReader.TryRead(out var message))
                {
                    messageBatch.Add(message);
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
            }
        }
        catch (OperationCanceledException)
        {
            LogBackgroundProcessorCancelled();
        }
        catch (Exception ex)
        {
            LogBackgroundProcessorError(ex);
            OnBackgroundError?.Invoke(this, new PublisherErrorEventArgs(ex, "Unexpected error in background message processor"));
        }
        finally
        {
            LogBackgroundProcessorStopped();
            timer.Dispose();
        }
    }

    private async Task SendBatchWithRetry(List<Message> messageBatch, CancellationToken ct)
    {
        if (!_config.EnableRetry)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messageBatch.ToArray(), ct);
            }
            catch (Exception ex)
            {
                LogFailedToSendBatch(ex, messageBatch.Count);
                OnMessageBatchFailed?.Invoke(this, new MessageBatchFailedEventArgs(ex, messageBatch.ToArray(), 0));
            }
            return;
        }

        Exception? lastException = null;
        var delay = _config.InitialRetryDelay;

        for (int attempt = 0; attempt <= _config.MaxRetryAttempts; attempt++)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messageBatch.ToArray(), ct);
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt < _config.MaxRetryAttempts && !ct.IsCancellationRequested)
                {
                    LogRetryingBatch(ex, messageBatch.Count, attempt + 1, _config.MaxRetryAttempts + 1, delay.TotalMilliseconds);
                    await Task.Delay(delay, ct);
                    delay = TimeSpan.FromMilliseconds(
                        Math.Min(delay.TotalMilliseconds * _config.RetryBackoffMultiplier, _config.MaxRetryDelay.TotalMilliseconds)
                    );
                }
            }
        }

        LogFailedToSendBatchAfterRetries(lastException!, messageBatch.Count, _config.MaxRetryAttempts + 1);
        OnMessageBatchFailed?.Invoke(this, new MessageBatchFailedEventArgs(lastException!, messageBatch.ToArray(), _config.MaxRetryAttempts));
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

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        LogDisposingPublisher();

        _cancellationTokenSource.Cancel();

        if (_backgroundTask != null && _isInitialized)
        {
            LogWaitingForBackgroundTask();
            try
            {
                Task.WaitAll([_backgroundTask], TimeSpan.FromSeconds(5));
                LogBackgroundTaskCompleted();
            }
            catch (AggregateException)
            {
                LogBackgroundTaskTimeout();
            }
        }

        _messageWriter?.Complete();
        _cancellationTokenSource.Dispose();

        _disposed = true;
        LogPublisherDisposed();
    }
}
