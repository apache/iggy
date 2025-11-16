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

        if (Config is not IggyConsumerConfig<T> config)
        {
            throw new InvalidOperationException("Invalid consumer config");
        }

        var loggerFactory = Config.LoggerFactory ?? NullLoggerFactory.Instance;
        var consumer = new IggyConsumer<T>(IggyClient!, config, loggerFactory);

        if (OnPollingError != null)
        {
            consumer.SubscribeToErrorEvents(OnPollingError);
        }

        return consumer;
    }

    /// <summary>
    ///     Validates the typed consumer configuration, including deserializer validation.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid.</exception>
    protected override void Validate()
    {
        base.Validate();

        if (Config is IggyConsumerConfig<T> typedConfig)
        {
            if (typedConfig.Deserializer == null)
            {
                throw new InvalidOperationException(
                    $"Deserializer must be provided for typed consumer IggyConsumer<{typeof(T).Name}>.");
            }
        }
        else
        {
            throw new InvalidOperationException(
                $"Config must be of type IggyConsumerConfig<{typeof(T).Name}>.");
        }
    }
}
