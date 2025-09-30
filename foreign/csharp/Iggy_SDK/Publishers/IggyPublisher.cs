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
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Initializing background message sending with queue capacity: {Capacity}, batch size: {BatchSize}",
                    _config.BackgroundQueueCapacity, _config.BackgroundBatchSize);
            }

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
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Publisher already initialized");
            }
            return;
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Initializing publisher for stream: {StreamId}, topic: {TopicId}", _config.StreamId, _config.TopicId);
        }

        await _client.LoginUser(_config.Login, _config.Password, ct);

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("User logged in successfully");
        }

        await CreateStreamIfNeeded(ct);
        await CreateTopicIfNeeded(ct);

        if (_config.EnableBackgroundSending)
        {
            StartBackgroundTasks();
            if (_logger.IsEnabled(LogLevel.Information))
            {
                _logger.LogInformation("Background message sending started");
            }
        }

        _isInitialized = true;
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Publisher initialized successfully");
        }
    }

    private async Task CreateStreamIfNeeded(CancellationToken ct)
    {
        if (await _client.GetStreamByIdAsync(_config.StreamId, ct) != null)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Stream {StreamId} already exists", _config.StreamId);
            }
            return;
        }

        if (!_config.CreateStream || string.IsNullOrEmpty(_config.StreamName))
        {
            _logger.LogError("Stream {StreamId} does not exist and auto-creation is disabled", _config.StreamId);
            throw new Exception($"Stream {_config.StreamId} not exists");
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Creating stream {StreamId} with name: {StreamName}", _config.StreamId, _config.StreamName);
        }

        if (_config.StreamId.Kind is IdKind.String)
        {
            await _client.CreateStreamAsync(_config.StreamId.GetString(), null, ct);
        }
        else
        {
            await _client.CreateStreamAsync(_config.StreamName, _config.StreamId.GetUInt32(), ct);
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Stream {StreamId} created successfully", _config.StreamId);
        }
    }

    private async Task CreateTopicIfNeeded(CancellationToken ct)
    {
        if (await _client.GetTopicByIdAsync(_config.StreamId, _config.TopicId, ct) != null)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Topic {TopicId} already exists in stream {StreamId}", _config.TopicId, _config.StreamId);
            }
            return;
        }

        if (!_config.CreateTopic || string.IsNullOrEmpty(_config.TopicName))
        {
            _logger.LogError("Topic {TopicId} does not exist in stream {StreamId} and auto-creation is disabled",
                _config.TopicId, _config.StreamId);
            throw new Exception($"Topic {_config.TopicId} does not exist");
        }

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Creating topic {TopicId} with name: {TopicName} in stream {StreamId}",
                _config.TopicId, _config.TopicName, _config.StreamId);
        }

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

        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Topic {TopicId} created successfully in stream {StreamId}", _config.TopicId, _config.StreamId);
        }
    }

    public async Task SendMessages(Message[] messages, CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            _logger.LogError("Attempted to send messages before publisher initialization");
            throw new InvalidOperationException("Publisher must be initialized before sending messages. Call InitAsync() first.");
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Sending {Count} messages", messages.Length);
        }

        EncryptMessages(messages);

        if (_config.EnableBackgroundSending && _messageWriter != null)
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Queuing {Count} messages for background sending", messages.Length);
            }
            foreach (var message in messages)
            {
                await _messageWriter.WriteAsync(message, ct);
            }
        }
        else
        {
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Sending {Count} messages synchronously", messages.Length);
            }
            await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messages, ct);
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Successfully sent {Count} messages", messages.Length);
            }
        }
    }

    public async Task WaitUntilAllSends(CancellationToken ct = default)
    {
        if (!_config.EnableBackgroundSending || _messageReader == null)
        {
            return;
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Waiting for all pending messages to be sent");
        }

        while (_messageReader.Count > 0 && !ct.IsCancellationRequested)
        {
            await Task.Delay(10, ct);
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("All pending messages have been sent");
        }
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

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Background message processor started");
        }

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

                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("Processing batch of {Count} messages", messageBatch.Count);
                }
                await SendBatchWithRetry(messageBatch, ct);
                messageBatch.Clear();
            }
        }
        catch (OperationCanceledException)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Background message processor cancelled");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error in background message processor");
            OnBackgroundError?.Invoke(this, new PublisherErrorEventArgs(ex, "Unexpected error in background message processor"));
        }
        finally
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Background message processor stopped");
            }
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
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Successfully sent batch of {Count} messages", messageBatch.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send batch of {Count} messages", messageBatch.Count);
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
                if (attempt > 0)
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation("Successfully sent batch of {Count} messages after {Attempts} retry attempts",
                            messageBatch.Count, attempt);
                    }
                }
                else
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug("Successfully sent batch of {Count} messages", messageBatch.Count);
                    }
                }
                return;
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt < _config.MaxRetryAttempts && !ct.IsCancellationRequested)
                {
                    if (_logger.IsEnabled(LogLevel.Warning))
                    {
                        _logger.LogWarning(ex, "Failed to send batch of {Count} messages (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}ms",
                            messageBatch.Count, attempt + 1, _config.MaxRetryAttempts + 1, delay.TotalMilliseconds);
                    }
                    await Task.Delay(delay, ct);
                    delay = TimeSpan.FromMilliseconds(
                        Math.Min(delay.TotalMilliseconds * _config.RetryBackoffMultiplier, _config.MaxRetryDelay.TotalMilliseconds)
                    );
                }
            }
        }

        _logger.LogError(lastException, "Failed to send batch of {Count} messages after {Attempts} attempts",
            messageBatch.Count, _config.MaxRetryAttempts + 1);
        OnMessageBatchFailed?.Invoke(this, new MessageBatchFailedEventArgs(lastException!, messageBatch.ToArray(), _config.MaxRetryAttempts));
    }

    private void EncryptMessages(Message[] messages)
    {
        if (_config.MessageEncryptor == null)
        {
            return;
        }

        if (_logger.IsEnabled(LogLevel.Trace))
        {
            _logger.LogTrace("Encrypting {Count} messages", messages.Length);
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

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Disposing publisher");
        }

        _cancellationTokenSource.Cancel();

        if (_backgroundTask != null && _isInitialized)
        {
            if (_logger.IsEnabled(LogLevel.Debug))
            {
                _logger.LogDebug("Waiting for background task to complete");
            }
            try
            {
                Task.WaitAll([_backgroundTask], TimeSpan.FromSeconds(5));
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("Background task completed");
                }
            }
            catch (AggregateException)
            {
                if (_logger.IsEnabled(LogLevel.Warning))
                {
                    _logger.LogWarning("Background task did not complete within timeout");
                }
            }
        }

        _messageWriter?.Complete();
        _cancellationTokenSource.Dispose();

        _disposed = true;
        if (_logger.IsEnabled(LogLevel.Information))
        {
            _logger.LogInformation("Publisher disposed");
        }
    }
}
