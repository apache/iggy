using Apache.Iggy.Configuration;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Publishers;

public class IggyPublisherBuilder
{
    public IggyPublisherConfig Config { get; set; } = new ();
    public IIggyClient? IggyClient { get; set; }

    private EventHandler<PublisherErrorEventArgs>? _onBackgroundError;
    private EventHandler<MessageBatchFailedEventArgs>? _onMessageBatchFailed;

    public static IggyPublisherBuilder Create(IIggyClient iggyClient,  Identifier streamId, Identifier topicId)
    {
        return new IggyPublisherBuilder
        {
            Config = new IggyPublisherConfig
            {
                StreamId = streamId,
                TopicId = topicId
            },
            IggyClient = iggyClient
        };
    }
    
    public static IggyPublisherBuilder Create(Identifier streamId, Identifier topicId)
    {
        return new IggyPublisherBuilder
        {
            Config = new IggyPublisherConfig
            {
                StreamId = streamId,
                TopicId = topicId
            }
        };
    }

    public IggyPublisherBuilder WithConnection(Protocol protocol, string address, string login, string password,
        int receiveBufferSize = 4096, int sendBufferSize = 4096)
    {
        Config.Protocol = protocol;
        Config.Address = address;
        Config.Login = login;
        Config.Password = password;
        Config.ReceiveBufferSize = receiveBufferSize;
        Config.SendBufferSize = sendBufferSize;
    
        return this;
    }

    public IggyPublisherBuilder WithPartitioning(Partitioning partitioning)
    {
        Config.Partitioning = partitioning;

        return this;
    }

    public IggyPublisherBuilder CreateStreamIfNotExists(string name)
    {
        Config.CreateStream = true;
        Config.StreamName = name;

        return this;
    }

    public IggyPublisherBuilder CreateTopicIfNotExists(string name, Partitioning partitioning, uint topicPartitionsCount = 1,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        ulong messageExpiry = 0, ulong maxTopicSize = 0)
    {
        Config.CreateTopic = true;
        Config.TopicName = name;
        Config.Partitioning = partitioning;
        Config.TopicPartitionsCount = topicPartitionsCount;
        Config.TopicCompressionAlgorithm = compressionAlgorithm;
        Config.TopicReplicationFactor = replicationFactor;
        Config.TopicMessageExpiry = messageExpiry;
        Config.TopicMaxTopicSize = maxTopicSize;
        
        return this;
    }
    
    public IggyPublisherBuilder WithEncryptor(IMessageEncryptor encryptor)
    {
        Config.MessageEncryptor = encryptor;

        return this;
    }

    public IggyPublisherBuilder OnBackgroundError(EventHandler<PublisherErrorEventArgs> handler)
    {
        _onBackgroundError = handler;
        return this;
    }

    public IggyPublisherBuilder OnMessageBatchFailed(EventHandler<MessageBatchFailedEventArgs> handler)
    {
        _onMessageBatchFailed = handler;
        return this;
    }

    public IggyPublisherBuilder WithRetry(bool enabled = true, int maxAttempts = 3,
        TimeSpan? initialDelay = null, TimeSpan? maxDelay = null, double backoffMultiplier = 2.0)
    {
        Config.EnableRetry = enabled;
        Config.MaxRetryAttempts = maxAttempts;
        Config.InitialRetryDelay = initialDelay ?? TimeSpan.FromMilliseconds(100);
        Config.MaxRetryDelay = maxDelay ?? TimeSpan.FromSeconds(10);
        Config.RetryBackoffMultiplier = backoffMultiplier;
        return this;
    }

    public IggyPublisherBuilder WithBackgroundSending(bool enabled = true, int queueCapacity = 10000,
        int batchSize = 100, TimeSpan? flushInterval = null)
    {
        Config.EnableBackgroundSending = enabled;
        Config.BackgroundQueueCapacity = queueCapacity;
        Config.BackgroundBatchSize = batchSize;
        Config.BackgroundFlushInterval = flushInterval ?? TimeSpan.FromMilliseconds(100);
        return this;
    }

    public IggyPublisher Build()
    {
        IggyClient ??= IggyClientFactory.CreateClient(new IggyClientConfigurator()
        {
            Protocol = Config.Protocol,
            BaseAdress = Config.Address,
            ReceiveBufferSize = Config.ReceiveBufferSize,
            SendBufferSize = Config.SendBufferSize
        });

        var publisher = new IggyPublisher(IggyClient, Config);

        if (_onBackgroundError != null)
        {
            publisher.OnBackgroundError += _onBackgroundError;
        }

        if (_onMessageBatchFailed != null)
        {
            publisher.OnMessageBatchFailed += _onMessageBatchFailed;
        }

        return publisher;
    }
}
