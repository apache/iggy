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

        if (OnBackgroundError != null)
        {
            publisher.SubscribeOnBackgroundError(OnBackgroundError);
        }

        if (OnMessageBatchFailed != null)
        {
            publisher.SubscribeOnMessageBatchFailed(OnMessageBatchFailed);
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
