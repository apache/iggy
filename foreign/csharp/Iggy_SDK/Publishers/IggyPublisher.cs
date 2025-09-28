using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using System.Threading.Channels;

namespace Apache.Iggy.Publishers;

public class IggyPublisher : IDisposable
{
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private bool _isInitialized = false;

    private readonly Channel<Message>? _messageChannel;
    private readonly ChannelWriter<Message>? _messageWriter;
    private readonly ChannelReader<Message>? _messageReader;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task[]? _backgroundTasks;
    private readonly SemaphoreSlim _flushSemaphore;
    private bool _disposed = false;

    public IggyPublisher(IIggyClient client, IggyPublisherConfig config)
    {
        _client = client;
        _config = config;
        _cancellationTokenSource = new CancellationTokenSource();
        _flushSemaphore = new SemaphoreSlim(1, 1);

        if (_config.EnableBackgroundSending)
        {
            var options = new BoundedChannelOptions(_config.BackgroundQueueCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false
            };

            _messageChannel = Channel.CreateBounded<Message>(options);
            _messageWriter = _messageChannel.Writer;
            _messageReader = _messageChannel.Reader;

            _backgroundTasks = new Task[_config.BackgroundWorkerCount];
        }
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        await _client.LoginUser(_config.Login, _config.Password, ct);
        await CreateStreamIfNeeded(ct);
        await CreateTopicIfNeeded(ct);

        if (_config.EnableBackgroundSending && _backgroundTasks != null)
        {
            StartBackgroundTasks();
        }

        _isInitialized = true;
    }

    private async Task CreateStreamIfNeeded(CancellationToken ct)
    {
        if (await _client.GetStreamByIdAsync(_config.StreamId, ct) != null)
        {
            return;
        }

        if (!_config.CreateStream || string.IsNullOrEmpty(_config.StreamName))
        {
            throw new Exception($"Stream {_config.StreamId} not exists");
        }
        
        if (_config.StreamId.Kind is IdKind.String)
        {
            await _client.CreateStreamAsync(_config.StreamId.GetString(), null, ct);    
        }
        else
        {
            await _client.CreateStreamAsync(_config.StreamName, _config.StreamId.GetUInt32(), ct);
        }
    }

    private async Task CreateTopicIfNeeded(CancellationToken ct)
    {
        if (await _client.GetTopicByIdAsync(_config.StreamId, _config.TopicId, ct) != null)
        {
            return;
        }

        if (!_config.TopicStream || string.IsNullOrEmpty(_config.TopicName))
        {
            throw new Exception($"Topic {_config.TopicId} not exists");
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
    }

    public async Task SendMessages(Message[] messages, CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            throw new InvalidOperationException("Publisher must be initialized before sending messages. Call InitAsync() first.");
        }

        EncryptMessages(messages);
        
        if (_config.EnableBackgroundSending && _messageWriter != null)
        {
            foreach (var message in messages)
            {
                await _messageWriter.WriteAsync(message, ct);
            }
        }
        else
        {
            var messagesList = messages.ToArray();

            await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messagesList.ToArray(), ct);
        }
    }

    public async Task WaitUntilAllSends(CancellationToken ct = default)
    {
        if (!_config.EnableBackgroundSending || _messageReader == null)
        {
            return;
        }

        while (_messageReader.Count > 0 && !ct.IsCancellationRequested)
        {
            await Task.Delay(10, ct);
        }
    }

    private void StartBackgroundTasks()
    {
        if (_backgroundTasks == null || _messageReader == null)
        {
            return;
        }

        for (var i = 0; i < _backgroundTasks.Length; i++)
        {
            _backgroundTasks[i] = Task.Run(async () => await BackgroundMessageProcessor(_cancellationTokenSource.Token));
        }
    }

    private async Task BackgroundMessageProcessor(CancellationToken ct)
    {
        var messageBatch = new List<Message>(_config.BackgroundBatchSize);
        var timer = new PeriodicTimer(_config.BackgroundFlushInterval);

        try
        {
            while (!ct.IsCancellationRequested && _messageReader != null)
            {
                var hasMessage = false;

                while (messageBatch.Count < _config.BackgroundBatchSize &&
                       _messageReader.TryRead(out var message))
                {
                    messageBatch.Add(message);
                    hasMessage = true;
                }

                if (!hasMessage && messageBatch.Count == 0)
                {
                    if (!await timer.WaitForNextTickAsync(ct))
                    {
                        break;
                    }
                    continue;
                }

                if (messageBatch.Count > 0)
                {
                    try
                    {
                        await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, messageBatch.ToArray(), ct);
                        messageBatch.Clear();
                    }
                    catch (Exception)
                    {
                        // Log error or handle failed messages
                        messageBatch.Clear();
                    }
                }

                if (!hasMessage)
                {
                    await timer.WaitForNextTickAsync(ct);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
        finally
        {
            timer.Dispose();
        }
    }

    private void EncryptMessages(Message[] messages)
    {
        if (_config.Encryptor == null)
        {
            return;
        }

        foreach (var message in messages)
        {
            message.Payload = _config.Encryptor(message.Payload);
            message.Header.PayloadLength = message.Payload.Length;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _cancellationTokenSource.Cancel();

        if (_backgroundTasks != null && _isInitialized)
        {
            try
            {
                Task.WaitAll(_backgroundTasks, TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // Ignore timeout or cancellation exceptions
            }
        }

        _messageWriter?.Complete();
        _cancellationTokenSource.Dispose();
        _flushSemaphore.Dispose();
        _disposed = true;
    }
}
