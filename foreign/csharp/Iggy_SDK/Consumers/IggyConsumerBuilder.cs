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
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Builder for creating <see cref="IggyConsumer" /> instances with fluent configuration API
/// </summary>
public class IggyConsumerBuilder
{
    internal Func<ConsumerErrorEventArgs, Task>? OnPollingError { get; set; }
    internal IggyConsumerConfig Config { get; set; } = new();
    internal IIggyClient? IggyClient { get; set; }

    /// <summary>
    ///     Creates a new consumer builder that will create its own Iggy client.
    ///     You must configure connection settings using <see cref="WithConnection" />.
    /// </summary>
    /// <param name="streamId">The stream identifier to consume from</param>
    /// <param name="topicId">The topic identifier to consume from</param>
    /// <param name="consumer">Consumer configuration (single consumer or consumer group)</param>
    /// <returns>A new instance of <see cref="IggyConsumerBuilder" /> to allow method chaining</returns>
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
    ///     Creates a new consumer builder using an existing Iggy client.
    ///     Connection settings are not needed as the client is already configured.
    /// </summary>
    /// <param name="iggyClient">The existing Iggy client instance to use</param>
    /// <param name="streamId">The stream identifier to consume from</param>
    /// <param name="topicId">The topic identifier to consume from</param>
    /// <param name="consumer">Consumer configuration (single consumer or consumer group)</param>
    /// <returns>A new instance of <see cref="IggyConsumerBuilder" /> to allow method chaining</returns>
    public static IggyConsumerBuilder Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId,
        Consumer consumer)
    {
        return new IggyConsumerBuilder
        {
            Config = new IggyConsumerConfig
            {
                StreamId = streamId,
                TopicId = topicId,
                Consumer = consumer
            },
            IggyClient = iggyClient
        };
    }

    /// <summary>
    ///     Configures the connection settings for the consumer.
    /// </summary>
    /// <param name="protocol">The protocol to use for the connection (e.g., TCP, UDP).</param>
    /// <param name="address">The address of the server to connect to.</param>
    /// <param name="login">The login username for authentication.</param>
    /// <param name="password">The password for authentication.</param>
    /// <param name="receiveBufferSize">The size of the receive buffer.</param>
    /// <param name="sendBufferSize">The size of the send buffer.</param>
    /// <param name="reconnectionSettings">Reconnection settings for the client.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConnection(Protocol protocol, string address, string login, string password,
        int receiveBufferSize = 4096, int sendBufferSize = 4096, ReconnectionSettings? reconnectionSettings = null)
    {
        Config.Protocol = protocol;
        Config.Address = address;
        Config.Login = login;
        Config.Password = password;
        Config.ReceiveBufferSize = receiveBufferSize;
        Config.SendBufferSize = sendBufferSize;
        Config.ReconnectionSettings = reconnectionSettings;

        return this;
    }

    /// <summary>
    ///     Sets a message decryptor for the consumer, enabling decryption of incoming messages.
    /// </summary>
    /// <param name="decryptor">The decryptor implementation to handle message decryption.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithDecryptor(IMessageEncryptor decryptor)
    {
        Config.MessageEncryptor = decryptor;

        return this;
    }

    /// <summary>
    ///     Specifies the partition for the consumer to consume messages from.
    /// </summary>
    /// <param name="partitionId">The identifier of the partition to consume from.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithPartitionId(uint partitionId)
    {
        Config.PartitionId = partitionId;

        return this;
    }

    /// <summary>
    ///     Configures the consumer builder with a specified polling strategy.
    ///     A polling strategy defines the starting point for message consumption.
    ///     After first poll, poll strategy is updated to the next message offset.
    /// </summary>
    /// <param name="pollingStrategy">A strategy that defines how and from where messages are polled.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithPollingStrategy(PollingStrategy pollingStrategy)
    {
        Config.PollingStrategy = pollingStrategy;

        return this;
    }

    /// <summary>
    ///     Sets the batch size for the consumer. Default is 100.
    /// </summary>
    /// <param name="batchSize">The size of the batch to be consumed at one time.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithBatchSize(uint batchSize)
    {
        Config.BatchSize = batchSize;

        return this;
    }

    /// <summary>
    ///     Configures the consumer builder with the specified auto-commit mode.
    /// </summary>
    /// <param name="autoCommit">The auto-commit mode to set for the consumer.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithAutoCommitMode(AutoCommitMode autoCommit)
    {
        Config.AutoCommit = autoCommit == AutoCommitMode.Auto;
        Config.AutoCommitMode = autoCommit;

        return this;
    }

    /// <summary>
    ///     Sets the logger factory for the consumer builder.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to be used for logging.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithLogger(ILoggerFactory loggerFactory)
    {
        Config.LoggerFactory = loggerFactory;

        return this;
    }

    /// <summary>
    ///     Configures consumer group settings for the consumer.
    /// </summary>
    /// <param name="groupName">The name of the consumer group if consumer kind is numeric</param>
    /// <param name="createIfNotExists">Whether to create the consumer group if it doesn't exist.</param>
    /// <param name="joinGroup">Whether to join the consumer group after creation/verification.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConsumerGroup(string groupName, bool createIfNotExists = true, bool joinGroup = true)
    {
        Config.ConsumerGroupName = groupName;
        Config.CreateConsumerGroupIfNotExists = createIfNotExists;
        Config.JoinConsumerGroup = joinGroup;

        return this;
    }

    /// <summary>
    ///     Sets the interval between polling attempts to control the rate of server requests
    /// </summary>
    /// <param name="interval">The polling interval as a TimeSpan. Set to TimeSpan.Zero to disable throttling.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining</returns>
    public IggyConsumerBuilder WithPollingInterval(TimeSpan interval)
    {
        Config.PollingIntervalMs = (int)interval.TotalMilliseconds;
        return this;
    }

    /// <summary>
    ///     Sets an event handler for handling polling errors in the consumer.
    /// </summary>
    /// <param name="handler">The event handler to handle polling errors.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder SubscribeOnPollingError(Func<ConsumerErrorEventArgs, Task> handler)
    {
        OnPollingError = handler;
        return this;
    }


    /// <summary>
    ///     Builds and returns an instance of <see cref="IggyConsumer" /> configured with the specified options.
    /// </summary>
    /// <returns>An instance of <see cref="IggyConsumer" /> based on the current builder configuration.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid.</exception>
    public IggyConsumer Build()
    {
        Validate();

        if (Config.CreateIggyClient)
        {
            IggyClient = IggyClientFactory.CreateClient(new IggyClientConfigurator
            {
                Protocol = Config.Protocol,
                BaseAddress = Config.Address,
                ReceiveBufferSize = Config.ReceiveBufferSize,
                SendBufferSize = Config.SendBufferSize,
                ReconnectionSettings = Config.ReconnectionSettings ?? new(),
                LoggerFactory = Config.LoggerFactory ?? NullLoggerFactory.Instance
            });
        }

        var loggerFactory = Config.LoggerFactory ?? NullLoggerFactory.Instance;
        var consumer = new IggyConsumer(IggyClient!, Config, loggerFactory);

        if (OnPollingError != null)
        {
            consumer.SubscribeToErrorEvents(OnPollingError);
        }

        return consumer;
    }

    /// <summary>
    ///     Validates the consumer configuration and throws if invalid.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid.</exception>
    protected virtual void Validate()
    {
        if (Config.CreateIggyClient)
        {
            if (string.IsNullOrWhiteSpace(Config.Address))
            {
                throw new InvalidOperationException("Address must be provided when CreateIggyClient is true.");
            }

            if (string.IsNullOrWhiteSpace(Config.Login))
            {
                throw new InvalidOperationException("Login must be provided when CreateIggyClient is true.");
            }

            if (string.IsNullOrWhiteSpace(Config.Password))
            {
                throw new InvalidOperationException("Password must be provided when CreateIggyClient is true.");
            }
        }
        else
        {
            if (IggyClient == null)
            {
                throw new InvalidOperationException(
                    "IggyClient must be provided when CreateIggyClient is false.");
            }
        }

        if (Config.ReceiveBufferSize <= 0)
        {
            throw new InvalidOperationException("ReceiveBufferSize must be greater than 0.");
        }

        if (Config.SendBufferSize <= 0)
        {
            throw new InvalidOperationException("SendBufferSize must be greater than 0.");
        }

        if (Config.BatchSize == 0)
        {
            throw new InvalidOperationException("BatchSize must be greater than 0.");
        }

        if (Config.PollingIntervalMs < 0)
        {
            throw new InvalidOperationException("PollingIntervalMs cannot be negative.");
        }

        if (Config.Consumer.Type == ConsumerType.ConsumerGroup)
        {
            if (Config.Consumer.ConsumerId.Kind == IdKind.Numeric &&
                Config.CreateConsumerGroupIfNotExists &&
                string.IsNullOrWhiteSpace(Config.ConsumerGroupName))
            {
                throw new InvalidOperationException(
                    "ConsumerGroupName must be provided when using numeric consumer group ID with CreateConsumerGroupIfNotExists enabled.");
            }
        }
    }
}
