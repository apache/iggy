using Apache.Iggy.Configuration;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Consumers;

public interface IDeserializer<out T>
{
    T Deserialize(byte[] data);
}

public class IggyConsumerBuilder<T> : IggyConsumerBuilder
{
    public static IggyConsumerBuilder<T> Create(Identifier streamId, Identifier topicId, Consumer consumer, IDeserializer<T> deserializer)
    {
        return new IggyConsumerBuilder<T>
        {
            Config = new IggyConsumerConfig<T>
            {
                CreateIggyClient = true,
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer,
                Deserializer = deserializer
            },
        };
    }
    
    public static IggyConsumerBuilder<T> Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId, Consumer consumer, IDeserializer<T> deserializer)
    {
        return new IggyConsumerBuilder<T>()
        {
            Config = new IggyConsumerConfig<T>()
            {
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer,
                Deserializer = deserializer
            },
            IggyClient = iggyClient
        };
    }

    public new IggyConsumer<T> Build()
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

        if (Config is not IggyConsumerConfig<T> config)
        {
            throw new InvalidOperationException("Invalid consumer config");
        }
        
        var consumer = new IggyConsumer<T>(IggyClient!, config,
            Config.LoggerFactory?.CreateLogger<IggyConsumer<T>>() ??
            NullLoggerFactory.Instance.CreateLogger<IggyConsumer<T>>());

        if (_onPollingError != null)
        {
            consumer.OnPollingError += _onPollingError;
        }

        return consumer;
    } 
}

public class IggyConsumerBuilder
{
    internal IggyConsumerConfig Config { get; set; } = new ();
    internal IIggyClient? IggyClient { get; set; }

    protected EventHandler<ConsumerErrorEventArgs>? _onPollingError;

    /// <summary>
    /// Create consumer builder
    /// </summary>
    /// <param name="streamId">Stream id</param>
    /// <param name="topicId">Topic id</param>
    /// <param name="consumer">Consumer</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining</returns>
    public static IggyConsumerBuilder Create(Identifier streamId, Identifier topicId, Consumer consumer)
    {
        return new IggyConsumerBuilder
        {
            Config = new IggyConsumerConfig
            {
                CreateIggyClient = true,
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer
            }
        };
    }

    /// <summary>
    /// Create consumer builder with existing iggy client. It's not needed to pass connection data after that
    /// </summary>
    /// <param name="streamId">Stream id</param>
    /// <param name="topicId">Topic id</param>
    /// <param name="iggyClient">Iggy client</param>
    /// <param name="consumer">Consumer</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining</returns>
    public static IggyConsumerBuilder Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId, Consumer consumer)
    {
        return new IggyConsumerBuilder()
        {
            Config = new IggyConsumerConfig()
            {
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer
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
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
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

    /// <summary>
    /// Sets a message decryptor for the consumer, enabling decryption of incoming messages.
    /// </summary>
    /// <param name="decryptor">The decryptor implementation to handle message decryption.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithDecryptor(IMessageEncryptor decryptor)
    {
        Config.MessageEncryptor = decryptor;

        return this;
    }

    /// <summary>
    /// Specifies the partition for the consumer to consume messages from.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition to consume from.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithPartitionId(uint partitionId)
    {
        Config.PartitionId = partitionId;

        return this;
    }

    /// <summary>
    /// Configures the consumer builder with a specified polling strategy.
    /// A polling strategy defines the starting point for message consumption.
    /// After first poll, poll strategy is updated to the next message offset.
    /// </summary>
    /// <param name="pollingStrategy">A strategy that defines how and from where messages are polled.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithPollingStrategy(PollingStrategy pollingStrategy)
    {
        Config.PollingStrategy = pollingStrategy;

        return this;
    }

    /// <summary>
    /// Sets the batch size for the consumer. Default is 100.
    /// </summary>
    /// <param name="batchSize">The size of the batch to be consumed at one time.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithBatchSize(uint batchSize)
    {
        Config.BatchSize = batchSize;

        return this;
    }

    /// <summary>
    /// Configures the consumer builder with the specified auto-commit mode.
    /// </summary>
    /// <param name="autoCommit">The auto-commit mode to set for the consumer.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithAutoCommitMode(AutoCommitMode autoCommit)
    {
        Config.AutoCommit = autoCommit == AutoCommitMode.Auto;
        Config.AutoCommitMode = autoCommit;

        return this;
    }

    /// <summary>
    /// Sets the logger factory for the consumer builder.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to be used for logging.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithLogger(ILoggerFactory loggerFactory)
    {
        Config.LoggerFactory = loggerFactory;

        return this;
    }

    /// <summary>
    /// Configures consumer group settings for the consumer.
    /// </summary>
    /// <param name="groupName">The name of the consumer group if consumer kind is numeric</param>
    /// <param name="createIfNotExists">Whether to create the consumer group if it doesn't exist.</param>
    /// <param name="joinGroup">Whether to join the consumer group after creation/verification.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConsumerGroup(string groupName, bool createIfNotExists = true, bool joinGroup = true)
    {
        Config.ConsumerGroupName = groupName;
        Config.CreateConsumerGroupIfNotExists = createIfNotExists;
        Config.JoinConsumerGroup = joinGroup;

        return this;
    }

    /// <summary>
    /// Sets an event handler for handling polling errors in the consumer.
    /// </summary>
    /// <param name="handler">The event handler to handle polling errors.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder"/> to allow method chaining.</returns>
    public IggyConsumerBuilder OnPollingError(EventHandler<ConsumerErrorEventArgs> handler)
    {
        _onPollingError = handler;
        return this;
    }


    /// <summary>
    /// Builds and returns an instance of <see cref="IggyConsumer"/> configured with the specified options.
    /// </summary>
    /// <returns>An instance of <see cref="IggyConsumer"/> based on the current builder configuration.</returns>
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

        var consumer = new IggyConsumer(IggyClient!, Config,
            Config.LoggerFactory?.CreateLogger<IggyConsumer>() ??
            NullLoggerFactory.Instance.CreateLogger<IggyConsumer>());

        if (_onPollingError != null)
        {
            consumer.OnPollingError += _onPollingError;
        }

        return consumer;
    }
}
