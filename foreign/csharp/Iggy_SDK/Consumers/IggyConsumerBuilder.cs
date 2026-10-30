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
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
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
    private protected IMessageEncryptor? _encryptor;

    internal Func<ConsumerErrorEventArgs, Task>? OnPollingError { get; set; }
    internal IggyConsumerConfig Config { get; set; } = new();
    internal IIggyClient? IggyClient { get; set; }

    /// <summary>
    ///     Creates a new consumer builder that will create its own Iggy client.
    ///     You must configure connection settings using <c>WithConnection</c>.
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
    /// <param name="protocol">The protocol to use for the connection (TCP or HTTP).</param>
    /// <param name="address">The address of the server to connect to.</param>
    /// <param name="login">The login username for authentication.</param>
    /// <param name="password">The password for authentication.</param>
    /// <param name="receiveBufferSize">
    ///     The size of the receive buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_rmem</c>.
    /// </param>
    /// <param name="sendBufferSize">
    ///     The size of the send buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_wmem</c>.
    /// </param>
    /// <param name="reconnectionSettings">Reconnection settings for the client.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConnection(Protocol protocol, string address, string login, string password,
        int? receiveBufferSize = null, int? sendBufferSize = null, ReconnectionSettings? reconnectionSettings = null)
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
    ///     Configures the connection settings using a personal access token instead of a username and password.
    /// </summary>
    /// <param name="protocol">The protocol to use for the connection (TCP or HTTP).</param>
    /// <param name="address">The address of the server to connect to.</param>
    /// <param name="personalAccessToken">The personal access token to authenticate with.</param>
    /// <param name="receiveBufferSize">
    ///     The size of the receive buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_rmem</c>.
    /// </param>
    /// <param name="sendBufferSize">
    ///     The size of the send buffer in bytes. When null, the size is not set on the socket, so the
    ///     operating system default is used. On Linux, this keeps TCP auto-tuning enabled. The initial size
    ///     is the middle value in <c>/proc/sys/net/ipv4/tcp_wmem</c>.
    /// </param>
    /// <param name="reconnectionSettings">Reconnection settings for the client.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConnection(Protocol protocol, string address, string personalAccessToken,
        int? receiveBufferSize = null, int? sendBufferSize = null, ReconnectionSettings? reconnectionSettings = null)
    {
        Config.Protocol = protocol;
        Config.Address = address;
        Config.PersonalAccessToken = personalAccessToken;
        Config.ReceiveBufferSize = receiveBufferSize;
        Config.SendBufferSize = sendBufferSize;
        Config.ReconnectionSettings = reconnectionSettings;

        return this;
    }

    /// <summary>
    ///     Configures message encryption on the client this builder creates. Encryption is a client-level concern:
    ///     the encryptor decrypts on poll (and encrypts on send) for the whole connection. Only valid when the
    ///     builder creates its own client. When an external client is supplied, configure the encryptor on that
    ///     client via <see cref="Configuration.IggyClientConfigurator.MessageEncryptor" /> instead.
    ///     The caller owns the encryptor and must dispose it after the consumer is disposed. The builder never
    ///     disposes it (the same instance may be shared across clients).
    /// </summary>
    /// <param name="encryptor">The message encryptor to use.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithEncryptor(IMessageEncryptor encryptor)
    {
        _encryptor = encryptor;

        return this;
    }

    /// <summary>
    ///     The partitions of a standalone consumer. A consumer group reads the assignment of its member.
    /// </summary>
    /// <param name="partitions">The partitions to consume from.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithPartitions(params uint[] partitions)
    {
        Config.Partitions = [.. partitions.Distinct()];

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
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining.</returns>
    public IggyConsumerBuilder WithConsumerGroup(string groupName, bool createIfNotExists = true)
    {
        Config.ConsumerGroupName = groupName;
        Config.CreateConsumerGroupIfNotExists = createIfNotExists;

        return this;
    }

    /// <summary>
    ///     Sets the limits of each read and the commit policy. See <see cref="IggyConsumerConfig.PollOptions" />.
    /// </summary>
    /// <param name="options">The limits of each read and the commit policy.</param>
    /// <returns>The current instance of <see cref="IggyConsumerBuilder" /> to allow method chaining</returns>
    public IggyConsumerBuilder WithPollOptions(PollOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Config.PollOptions = options;
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
                ReconnectionSettings = Config.ReconnectionSettings ?? new ReconnectionSettings(),
                HeartbeatInterval = Config.HeartbeatInterval,
                AutoLoginSettings = AutoLogin(),
                LoggerFactory = Config.LoggerFactory ?? NullLoggerFactory.Instance,
                MessageEncryptor = _encryptor
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
    ///     The credentials given to WithConnection must reach the client and not only the explicit login
    ///     performed at startup: a reconnect or a leader redirect drops the session, and without them the
    ///     client would come back unauthenticated.
    /// </summary>
    private protected AutoLoginSettings AutoLogin()
    {
        return string.IsNullOrEmpty(Config.PersonalAccessToken)
            ? AutoLoginSettings.For(Config.Login, Config.Password)
            : AutoLoginSettings.ForPersonalAccessToken(Config.PersonalAccessToken);
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

            if (string.IsNullOrWhiteSpace(Config.PersonalAccessToken))
            {
                if (string.IsNullOrWhiteSpace(Config.Login))
                {
                    throw new InvalidOperationException(
                        "Login or PersonalAccessToken must be provided when CreateIggyClient is true.");
                }

                if (string.IsNullOrWhiteSpace(Config.Password))
                {
                    throw new InvalidOperationException("Password must be provided when CreateIggyClient is true.");
                }
            }
        }
        else
        {
            if (IggyClient == null)
            {
                throw new InvalidOperationException("IggyClient must be provided when CreateIggyClient is false.");
            }

            if (_encryptor != null)
            {
                throw new InvalidOperationException(
                    "WithEncryptor is only valid when the builder creates its own client. " +
                    "Configure IggyClientConfigurator.MessageEncryptor on the supplied client instead.");
            }
        }

        var hasEncryptor = _encryptor != null || IggyClient?.MessageEncryptor != null;
        if (hasEncryptor && Config.PollOptions?.Commit.Mode == CommitMode.OnRead)
        {
            throw new InvalidOperationException(
                "Storing the offset on read with a message encryptor loses a batch that does not decrypt. Commit with CommitPolicy.Manual or CommitPolicy.Auto instead.");
        }

        if (Config.ReceiveBufferSize is <= 0)
        {
            throw new InvalidOperationException("ReceiveBufferSize must be greater than 0 when set.");
        }

        if (Config.SendBufferSize is <= 0)
        {
            throw new InvalidOperationException("SendBufferSize must be greater than 0 when set.");
        }

        if (Config.PollOptions is null)
        {
            throw new InvalidOperationException("PollOptions must be set.");
        }

        try
        {
            Config.PollOptions.Validate();
        }
        catch (IggyInvalidStatusCodeException error)
        {
            throw new InvalidOperationException($"PollOptions are invalid: {error.Message}", error);
        }

        if (Config.Consumer.Type == ConsumerType.Consumer && Config.Partitions.Count == 0)
        {
            throw new InvalidOperationException("A standalone consumer needs partitions, set them with WithPartitions.");
        }

        if (Config.Consumer.Type == ConsumerType.ConsumerGroup && Config.Partitions.Count > 0)
        {
            throw new InvalidOperationException("A consumer group reads the partitions of its assignment.");
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
