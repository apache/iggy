using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace Apache.Iggy.Publishers;

internal sealed partial class BackgroundMessageProcessor : IAsyncDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly ILogger<BackgroundMessageProcessor> _logger;
    private readonly Channel<Message> _messageChannel;
    private readonly ChannelWriter<Message> _messageWriter;
    private readonly ChannelReader<Message> _messageReader;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _backgroundTask;
    private bool _disposed = false;
    private bool _sending;

    public event EventHandler<PublisherErrorEventArgs>? OnBackgroundError;
    public event EventHandler<MessageBatchFailedEventArgs>? OnMessageBatchFailed;

    public ChannelWriter<Message> MessageWriter => _messageWriter;
    public ChannelReader<Message> MessageReader => _messageReader;

    public BackgroundMessageProcessor(
        IIggyClient client,
        IggyPublisherConfig config,
        ILogger<BackgroundMessageProcessor> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();

        var options = new BoundedChannelOptions(_config.BackgroundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };

        _messageChannel = Channel.CreateBounded<Message>(options);
        _messageWriter = _messageChannel.Writer;
        _messageReader = _messageChannel.Reader;
    }

    public void Start()
    {
        if (_backgroundTask != null)
        {
            return;
        }

        _backgroundTask = RunBackgroundProcessor(_cancellationTokenSource.Token);
        LogBackgroundProcessorStarted();
    }
    
    public bool IsSending => _sending;

    private async Task RunBackgroundProcessor(CancellationToken ct)
    {
        var messageBatch = new List<Message>(_config.BackgroundBatchSize);
        using var timer = new PeriodicTimer(_config.BackgroundFlushInterval);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                while (messageBatch.Count < _config.BackgroundBatchSize &&
                       _messageReader.TryRead(out var message))
                {
                    messageBatch.Add(message);
                    _sending = true;
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
                _sending = false;
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
            _sending = false;
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

        for (var attempt = 0; attempt < _config.MaxRetryAttempts; attempt++)
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

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _cancellationTokenSource.Cancel();

        if (_backgroundTask != null)
        {
            LogWaitingForBackgroundTask();
            try
            {
                await _backgroundTask.WaitAsync(TimeSpan.FromSeconds(5));
                LogBackgroundTaskCompleted();
            }
            catch (TimeoutException)
            {
                LogBackgroundTaskTimeout();
            }
            catch (Exception e)
            {
                LogBackgroundProcessorError(e);
            }
        }

        _messageWriter.Complete();
        _cancellationTokenSource.Dispose();

        _disposed = true;
    }

    // Logging methods
    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Debug,
        Message = "Background message processor started")]
    private partial void LogBackgroundProcessorStarted();

    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Debug,
        Message = "Background message processor cancelled")]
    private partial void LogBackgroundProcessorCancelled();

    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Debug,
        Message = "Background message processor stopped")]
    private partial void LogBackgroundProcessorStopped();

    [LoggerMessage(
        EventId = 15,
        Level = LogLevel.Debug,
        Message = "Waiting for background task to complete")]
    private partial void LogWaitingForBackgroundTask();

    [LoggerMessage(
        EventId = 16,
        Level = LogLevel.Debug,
        Message = "Background task completed")]
    private partial void LogBackgroundTaskCompleted();

    [LoggerMessage(
        EventId = 300,
        Level = LogLevel.Warning,
        Message = "Failed to send batch of {Count} messages (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}ms")]
    private partial void LogRetryingBatch(Exception exception, int count, int attempt, int maxAttempts, double delay);

    [LoggerMessage(
        EventId = 301,
        Level = LogLevel.Warning,
        Message = "Background task did not complete within timeout")]
    private partial void LogBackgroundTaskTimeout();

    [LoggerMessage(
        EventId = 403,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages")]
    private partial void LogFailedToSendBatch(Exception exception, int count);

    [LoggerMessage(
        EventId = 404,
        Level = LogLevel.Error,
        Message = "Unexpected error in background message processor")]
    private partial void LogBackgroundProcessorError(Exception exception);

    [LoggerMessage(
        EventId = 405,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages after {Attempts} attempts")]
    private partial void LogFailedToSendBatchAfterRetries(Exception exception, int count, int attempts);
}
