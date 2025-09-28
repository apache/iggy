using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Consumers;

public class IggyConsumerBuilder
{
    public IggyConsumerConfig Config { get; set; } = new ();
    public IIggyClient? IggyClient { get; private set; }
    
    /// <summary>
    /// Create consumer builder
    /// </summary>
    /// <param name="streamId">Stream id</param>
    /// <param name="topicId">Topic id</param>
    /// <returns></returns>
    public static IggyConsumerBuilder Create(Identifier streamId, Identifier topicId)
    {
        return new IggyConsumerBuilder
        {
            Config = new IggyConsumerConfig
            {
                CreateIggyClient = true,
                StreamId = streamId,
                TopicId = topicId
            }
        };
    }

    /// <summary>
    /// Create consumer builder with existing iggy client. It's not needed to pass connectin data after that
    /// </summary>
    /// <param name="streamId">Stream id</param>
    /// <param name="topicId">Topic id</param>
    /// <param name="iggyClient">Iggy client</param>
    /// <returns></returns>
    public static IggyConsumerBuilder Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId)
    {
        return new IggyConsumerBuilder()
        {
            Config = new IggyConsumerConfig()
            {
                StreamId = streamId,
                TopicId = topicId
            },
            IggyClient = iggyClient
        };
    }

    /// <summary>
    /// Configures the connection settings for the consumer.
    /// </summary>
    /// <param name="protocol">The protocol to use for the connection (e.g., TCP, UDP).</param>
    /// <param name="address">The address of the server to connect to.</param>
    /// <param name="login">The login username for authentication.</param>
    /// <param name="password">The password for authentication.</param>
    /// <param name="receiveBufferSize">The size of the receive buffer.</param>
    /// <param name="sendBufferSize">The size of the send buffer.</param>
    /// <returns>The current instance of the consumer builder with the updated connection settings.</returns>
    public IggyConsumerBuilder WithConnection(Protocol protocol, string address, string login, string password,
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
    
    public IggyConsumerBuilder WithDecryptor(Func<byte[], byte[]> decryptor)
    {
        Config.Decryptor = decryptor;
        
        return this;
    }

    public IggyConsumerBuilder WithPartition(uint partitionId)
    {
        Config.PartitionId = partitionId;

        return this;
    }

    public IggyConsumerBuilder WithConsumer(Kinds.Consumer consumer)
    {
        Config.Consumer = consumer;

        return this;
    }

    public IggyConsumerBuilder WithPollingStrategy(PollingStrategy pollingStrategy)
    {
        Config.PollingStrategy = pollingStrategy;

        return this;
    }

    public IggyConsumerBuilder WithBatchSize(uint batchSize)
    {
        Config.BatchSize = batchSize;

        return this;
    }

    public IggyConsumerBuilder WithAutoCommit(AutoCommitMode autoCommit)
    {
        Config.AutoCommit = autoCommit == AutoCommitMode.Auto;
        Config.AutoCommitMode = autoCommit;

        return this;
    }
    
    /// <summary>
    /// Set size of internal bounded channel
    /// </summary>
    /// <param name="bufferSize"></param>
    /// <returns></returns>
    public IggyConsumerBuilder WithBufferSize(int bufferSize)
    {
        Config.BufferSize = bufferSize;

        return this;
    }

    public IggyConsumerBuilder WithLogger(ILoggerFactory loggerFactory)
    {
        Config.LoggerFactory = loggerFactory;

        return this;
    }

    /// <summary>
    /// Configures consumer group settings for the consumer.
    /// </summary>
    /// <param name="groupName">The name of the consumer group.</param>
    /// <param name="createIfNotExists">Whether to create the consumer group if it doesn't exist.</param>
    /// <param name="joinGroup">Whether to join the consumer group after creation/verification.</param>
    /// <returns>The current instance of the consumer builder with the updated consumer group settings.</returns>
    public IggyConsumerBuilder WithConsumerGroup(string groupName, bool createIfNotExists = true, bool joinGroup = true)
    {
        Config.ConsumerGroupName = groupName;
        Config.CreateConsumerGroupIfNotExists = createIfNotExists;
        Config.JoinConsumerGroup = joinGroup;

        return this;
    }

    public IggyConsumer Build()
    {
        if (Config.CreateIggyClient)
        {
            IggyClient = IggyClientFactory.CreateClient(new IggyClientConfigurator()
            {
                Protocol = Config.Protocol,
                BaseAdress = Config.Address,
                ReceiveBufferSize = Config.ReceiveBufferSize,
                SendBufferSize = Config.SendBufferSize
            });
        }

        if (Config.BufferSize == 0)
        {
            Config.BufferSize = Config.BatchSize <= int.MaxValue ? (int)Config.BatchSize : int.MaxValue;
        }

        return new IggyConsumer(IggyClient!, Config,
            Config.LoggerFactory?.CreateLogger<IggyConsumer>() ??
            NullLoggerFactory.Instance.CreateLogger<IggyConsumer>());
    }
}
