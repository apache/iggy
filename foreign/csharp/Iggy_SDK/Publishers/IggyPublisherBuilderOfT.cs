using Apache.Iggy.Configuration;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Builder for creating typed <see cref="IggyPublisher{T}" /> instances with fluent configuration
/// </summary>
/// <typeparam name="T">The type to serialize to message payloads</typeparam>
public class IggyPublisherBuilder<T> : IggyPublisherBuilder
{
    /// <summary>
    ///     Creates a new typed publisher builder that will create its own Iggy client
    /// </summary>
    /// <param name="streamId">The stream identifier to publish to</param>
    /// <param name="topicId">The topic identifier to publish to</param>
    /// <param name="serializer">The serializer for converting objects to byte arrays</param>
    /// <returns>A new instance of <see cref="IggyPublisherBuilder{T}" /></returns>
    public static IggyPublisherBuilder<T> Create(Identifier streamId, Identifier topicId, ISerializer<T> serializer)
    {
        return new IggyPublisherBuilder<T>
        {
            Config = new IggyPublisherConfig<T>
            {
                CreateIggyClient = true,
                StreamId = streamId,
                TopicId = topicId,
                Serializer = serializer
            }
        };
    }

    /// <summary>
    ///     Creates a new typed publisher builder using an existing Iggy client
    /// </summary>
    /// <param name="iggyClient">The existing Iggy client to use</param>
    /// <param name="streamId">The stream identifier to publish to</param>
    /// <param name="topicId">The topic identifier to publish to</param>
    /// <param name="serializer">The serializer for converting objects to byte arrays</param>
    /// <returns>A new instance of <see cref="IggyPublisherBuilder{T}" /></returns>
    public static IggyPublisherBuilder<T> Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId,
        ISerializer<T> serializer)
    {
        return new IggyPublisherBuilder<T>
        {
            Config = new IggyPublisherConfig<T>
            {
                CreateIggyClient = false,
                StreamId = streamId,
                TopicId = topicId,
                Serializer = serializer
            },
            IggyClient = iggyClient
        };
    }

    /// <summary>
    ///     Builds and returns a typed <see cref="IggyPublisher{T}" /> instance with the configured settings
    /// </summary>
    /// <returns>A configured instance of <see cref="IggyPublisher{T}" /></returns>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid</exception>
    public new IggyPublisher<T> Build()
    {
        Validate();

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

        if (Config is not IggyPublisherConfig<T> config)
        {
            throw new InvalidOperationException("Invalid publisher config");
        }

        var publisher = new IggyPublisher<T>(IggyClient!, config,
            Config.LoggerFactory?.CreateLogger<IggyPublisher<T>>() ??
            NullLoggerFactory.Instance.CreateLogger<IggyPublisher<T>>());

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

    /// <summary>
    ///     Validates the typed publisher configuration, including serializer validation.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid.</exception>
    protected override void Validate()
    {
        base.Validate();

        if (Config is IggyPublisherConfig<T> typedConfig)
        {
            if (typedConfig.Serializer == null)
            {
                throw new InvalidOperationException(
                    $"Serializer must be provided for typed publisher IggyPublisher<{typeof(T).Name}>.");
            }
        }
        else
        {
            throw new InvalidOperationException(
                $"Config must be of type IggyPublisherConfig<{typeof(T).Name}>.");
        }
    }
}
