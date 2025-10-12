﻿using System.Runtime.CompilerServices;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Typed consumer that automatically deserializes message payloads to type T.
///     Extends <see cref="IggyConsumer" /> with deserialization capabilities.
/// </summary>
/// <typeparam name="T">The type to deserialize message payloads to</typeparam>
public class IggyConsumer<T> : IggyConsumer
{
    private readonly IggyConsumerConfig<T> _typedConfig;
    private readonly ILogger<IggyConsumer<T>> _typedLogger;

    /// <summary>
    ///     Initializes a new instance of the typed <see cref="IggyConsumer{T}" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Typed consumer configuration including deserializer</param>
    /// <param name="logger">Logger instance for diagnostic output</param>
    public IggyConsumer(IIggyClient client, IggyConsumerConfig<T> config, ILogger<IggyConsumer<T>> logger) : base(
        client, config, logger)
    {
        _typedConfig = config;
        _typedLogger = logger;
    }

    /// <summary>
    ///     Receives and deserializes messages from the consumer
    /// </summary>
    /// <param name="ct">Cancellation token</param>
    /// <returns>Async enumerable of deserialized messages with status</returns>
    public async IAsyncEnumerable<ReceivedMessage<T>> ReceiveDeserializedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var message in ReceiveAsync(ct))
        {
            if (message.Status != MessageStatus.Success)
            {
                yield return new ReceivedMessage<T>
                {
                    Data = default,
                    Message = message.Message,
                    CurrentOffset = message.CurrentOffset,
                    PartitionId = message.PartitionId,
                    Status = message.Status,
                    Error = message.Error
                };
                continue;
            }

            T? deserializedPayload = default;
            Exception? deserializationError = null;
            var status = MessageStatus.Success;

            try
            {
                deserializedPayload = Deserialize(message.Message.Payload);
            }
            catch (Exception ex)
            {
                _typedLogger.LogError(ex, "Failed to deserialize message at offset {Offset}", message.CurrentOffset);
                status = MessageStatus.DeserializationFailed;
                deserializationError = ex;
            }

            yield return new ReceivedMessage<T>
            {
                Data = deserializedPayload,
                Message = message.Message,
                CurrentOffset = message.CurrentOffset,
                PartitionId = message.PartitionId,
                Status = status,
                Error = deserializationError
            };
        }
    }

    /// <summary>
    ///     Deserializes a message payload using the configured deserializer
    /// </summary>
    /// <param name="payload">The raw byte array payload to deserialize</param>
    /// <returns>The deserialized object of type T</returns>
    public T Deserialize(byte[] payload)
    {
        return _typedConfig.Deserializer.Deserialize(payload);
    }
}
