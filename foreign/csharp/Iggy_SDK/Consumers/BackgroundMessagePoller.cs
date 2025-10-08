using System.Threading.Channels;
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

internal sealed partial class BackgroundMessagePoller : IAsyncDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly ILogger<BackgroundMessagePoller> _logger;
    private readonly Channel<ReceivedMessage> _channel;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private Task? _pollingTask;
    private bool _disposed;
    private PollingStrategy _currentPollingStrategy;

    public event EventHandler<ConsumerErrorEventArgs>? OnPollingError;
    public event EventHandler<MessageDecryptionFailedEventArgs>? OnMessageDecryptionFailed;

    public ChannelReader<ReceivedMessage> MessageReader => _channel.Reader;

    public BackgroundMessagePoller(
        IIggyClient client,
        IggyConsumerConfig config,
        ILogger<BackgroundMessagePoller> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _cancellationTokenSource = new CancellationTokenSource();
        _channel = Channel.CreateBounded<ReceivedMessage>(new BoundedChannelOptions(config.ChannelBufferSize)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
        _currentPollingStrategy = config.PollingStrategy;
    }

    public void Start(CancellationToken externalCt = default)
    {
        if (_pollingTask is { IsCompleted: false })
        {
            return;
        }

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(externalCt, _cancellationTokenSource.Token);
        _pollingTask = PollMessagesAsync(linkedCts.Token);
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
                        _config.PartitionId, _config.Consumer, _currentPollingStrategy, _config.BatchSize,
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

                    if (messages.Messages.Any())
                    {
                        var lastOffset = messages.Messages[^1].Header.Offset;
                        _currentPollingStrategy = PollingStrategy.Offset(lastOffset + 1);
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

        if (_pollingTask != null)
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

        _cancellationTokenSource.Dispose();

        _disposed = true;
    }

    // Logging methods
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Polling task cancelled")]
    private partial void LogPollingTaskCancelled();

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Message polling stopped")]
    private partial void LogMessagePollingStopped();

    [LoggerMessage(
        EventId = 300,
        Level = LogLevel.Warning,
        Message = "Polling task timed out")]
    private partial void LogPollingTaskTimeout();

    [LoggerMessage(
        EventId = 301,
        Level = LogLevel.Warning,
        Message = "Polling task failed with exception")]
    private partial void LogPollingTaskFailed(Exception exception);

    [LoggerMessage(
        EventId = 402,
        Level = LogLevel.Error,
        Message = "Failed to decrypt message with offset {Offset}")]
    private partial void LogFailedToDecryptMessage(Exception exception, ulong offset);

    [LoggerMessage(
        EventId = 403,
        Level = LogLevel.Error,
        Message = "Failed to poll messages")]
    private partial void LogFailedToPollMessages(Exception exception);
}
