// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Runtime.CompilerServices;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
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
    public IggyConsumer(IIggyClient client, IggyConsumerConfig<T> config, ILoggerFactory logger) : base(client, config,
        logger)
    {
        _typedConfig = config;
        _typedLogger = logger.CreateLogger<IggyConsumer<T>>();
    }

    /// <summary>
    ///     Receives and deserializes messages one at a time, as <see cref="IggyConsumer.ReceiveAsync" /> receives them.
    ///     A payload that does not deserialize comes with <see cref="MessageStatus.DeserializationFailed" /> and its
    ///     error.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages</param>
    /// <exception cref="ConsumerNotInitializedException">Thrown when InitAsync has not been called</exception>
    public async IAsyncEnumerable<ReceivedMessage<T>> ReceiveDeserializedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var received in ReceiveAsync(ct))
        {
            T? data = default;
            Dictionary<HeaderKey, HeaderValue>? headers = null;
            var status = MessageStatus.Success;
            Exception? error = null;
            try
            {
                data = Deserialize(received.Message.Payload);
                headers = received.Message.UserHeaders;
            }
            catch (Exception exception)
            {
                _typedLogger.LogError(exception, "Failed to deserialize message at offset {Offset}",
                    received.Message.Header.Offset);
                status = MessageStatus.DeserializationFailed;
                error = exception;
            }

            yield return new ReceivedMessage<T>
            {
                Data = data,
                Header = received.Message.Header,
                UserHeaders = headers,
                CurrentOffset = received.CurrentOffset,
                PartitionId = received.PartitionId,
                Context = received.Context,
                Checkpoint = received.Checkpoint,
                Status = status,
                Error = error
            };
        }
    }

    /// <summary>Deserializes a payload with the deserializer of the configuration.</summary>
    public T Deserialize(ReadOnlyMemory<byte> payload)
    {
        return _typedConfig.Deserializer.Deserialize(payload);
    }
}
