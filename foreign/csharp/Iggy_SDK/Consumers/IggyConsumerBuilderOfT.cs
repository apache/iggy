using Apache.Iggy.Configuration;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Builder for creating typed <see cref="IggyConsumer{T}" /> instances with fluent configuration
/// </summary>
/// <typeparam name="T">The type to deserialize messages to</typeparam>
public class IggyConsumerBuilder<T> : IggyConsumerBuilder
{
    /// <summary>
    ///     Creates a new typed consumer builder that will create its own Iggy client
    /// </summary>
    /// <param name="streamId">The stream identifier to consume from</param>
    /// <param name="topicId">The topic identifier to consume from</param>
    /// <param name="consumer">Consumer configuration (single or group)</param>
    /// <param name="deserializer">The deserializer for converting payloads to type T</param>
    /// <returns>A new instance of <see cref="IggyConsumerBuilder{T}" /></returns>
    public static IggyConsumerBuilder<T> Create(Identifier streamId, Identifier topicId, Consumer consumer,
        IDeserializer<T> deserializer)
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
            }
        };
    }

    /// <summary>
    ///     Creates a new typed consumer builder using an existing Iggy client
    /// </summary>
    /// <param name="iggyClient">The existing Iggy client to use</param>
    /// <param name="streamId">The stream identifier to consume from</param>
    /// <param name="topicId">The topic identifier to consume from</param>
    /// <param name="consumer">Consumer configuration (single or group)</param>
    /// <param name="deserializer">The deserializer for converting payloads to type T</param>
    /// <returns>A new instance of <see cref="IggyConsumerBuilder{T}" /></returns>
    public static IggyConsumerBuilder<T> Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId,
        Consumer consumer, IDeserializer<T> deserializer)
    {
        return new IggyConsumerBuilder<T>
        {
            Config = new IggyConsumerConfig<T>
            {
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer,
                Deserializer = deserializer
            },
            IggyClient = iggyClient
        };
    }

    /// <summary>
    ///     Builds and returns a typed <see cref="IggyConsumer{T}" /> instance with the configured settings
    /// </summary>
    /// <returns>A configured instance of <see cref="IggyConsumer{T}" /></returns>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid</exception>
    public new IggyConsumer<T> Build()
    {
        if (Config.CreateIggyClient)
        {
            IggyClient = IggyClientFactory.CreateClient(new IggyClientConfigurator
            {
                Protocol = Config.Protocol,
                BaseAddress = Config.Address,
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
