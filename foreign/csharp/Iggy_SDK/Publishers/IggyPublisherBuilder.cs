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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Fluent builder for creating and configuring <see cref="IggyPublisher" /> instances.
///     Provides a convenient API for setting up publishers with various configuration options
///     including connection settings, partitioning, encryption, retry logic, and background sending.
/// </summary>
public class IggyPublisherBuilder
{
    internal Func<PublisherErrorEventArgs, Task>? OnBackgroundError { get; set; }
    internal Func<MessageBatchFailedEventArgs, Task>? OnMessageBatchFailed { get; set; }

    /// <summary>
    ///     Gets or sets the publisher configuration.
    /// </summary>
    internal IggyPublisherConfig Config { get; set; } = new();

    /// <summary>
    ///     Gets or sets the Iggy client instance to use.
    ///     When null and <see cref="IggyPublisherConfig.CreateIggyClient" /> is true, a new client will be created during
    ///     build.
    /// </summary>
    internal IIggyClient? IggyClient { get; set; }

    /// <summary>
    ///     Creates a new publisher builder using an existing Iggy client instance.
    /// </summary>
    /// <param name="iggyClient">The existing Iggy client to use.</param>
    /// <param name="streamId">The identifier of the target stream.</param>
    /// <param name="topicId">The identifier of the target topic.</param>
    /// <returns>A new instance of <see cref="IggyPublisherBuilder" />.</returns>
    public static IggyPublisherBuilder Create(IIggyClient iggyClient, Identifier streamId, Identifier topicId)
    {
        return new IggyPublisherBuilder
        {
            Config = new IggyPublisherConfig
            {
                CreateIggyClient = false,
                StreamId = streamId,
                TopicId = topicId
            },
            IggyClient = iggyClient
        };
    }

    /// <summary>
    ///     Creates a new publisher builder that will create its own Iggy client.
    /// </summary>
    /// <param name="streamId">The identifier of the target stream.</param>
    /// <param name="topicId">The identifier of the target topic.</param>
    /// <returns>A new instance of <see cref="IggyPublisherBuilder" />.</returns>
    public static IggyPublisherBuilder Create(Identifier streamId, Identifier topicId)
    {
        return new IggyPublisherBuilder
        {
            Config = new IggyPublisherConfig
            {
                CreateIggyClient = true,
                StreamId = streamId,
                TopicId = topicId
            }
        };
    }

    /// <summary>
    ///     Creates a new publisher builder using an existing configuration.
    /// </summary>
    /// <param name="config">The configuration to use for the publisher.</param>
    /// <returns>A new instance of <see cref="IggyPublisherBuilder" />.</returns>
    public static IggyPublisherBuilder Create(IggyPublisherConfig config)
    {
        return new IggyPublisherBuilder { Config = config };
    }


    /// <summary>
    ///     Configures the connection settings for the publisher's Iggy client.
    ///     Only used when the builder creates its own client.
    /// </summary>
    /// <param name="protocol">The protocol to use (TCP, QUIC, or HTTP).</param>
    /// <param name="address">The server address to connect to (format depends on protocol).</param>
    /// <param name="login">The login username for authentication.</param>
    /// <param name="password">The password for authentication.</param>
    /// <param name="receiveBufferSize">The size of the receive buffer in bytes. Default is 4096.</param>
    /// <param name="sendBufferSize">The size of the send buffer in bytes. Default is 4096.</param>
    /// <param name="reconnectionSettings">Reconnection settings for the client.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithConnection(Protocol protocol, string address, string login, string password,
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
    ///     Configures the partitioning strategy for messages sent by the publisher.
    ///     Determines how messages are distributed across topic partitions.
    /// </summary>
    /// <param name="partitioning">
    ///     The partitioning configuration to use (e.g., balanced, partition-specific, or message-key
    ///     based).
    /// </param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithPartitioning(Partitioning partitioning)
    {
        Config.Partitioning = partitioning;

        return this;
    }

    /// <summary>
    ///     Enables automatic stream creation if the target stream does not exist.
    /// </summary>
    /// <param name="name">The name to use when creating the stream.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder CreateStreamIfNotExists(string name)
    {
        Config.CreateStream = true;
        Config.StreamName = name;

        return this;
    }

    /// <summary>
    ///     Enables automatic topic creation if the target topic does not exist.
    /// </summary>
    /// <param name="name">The name to use when creating the topic.</param>
    /// <param name="topicPartitionsCount">The number of partitions for the topic. Default is 1.</param>
    /// <param name="compressionAlgorithm">The compression algorithm to use for messages in the topic. Default is None.</param>
    /// <param name="replicationFactor">The replication factor for the topic. Null means server default.</param>
    /// <param name="messageExpiry">The message expiry time in seconds (0 for no expiry). Default is 0.</param>
    /// <param name="maxTopicSize">The maximum size of the topic in bytes (0 for unlimited). Default is 0.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder CreateTopicIfNotExists(string name, uint topicPartitionsCount = 1,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        ulong messageExpiry = 0, ulong maxTopicSize = 0)
    {
        Config.CreateTopic = true;
        Config.TopicName = name;
        Config.TopicPartitionsCount = topicPartitionsCount;
        Config.TopicCompressionAlgorithm = compressionAlgorithm;
        Config.TopicReplicationFactor = replicationFactor;
        Config.TopicMessageExpiry = messageExpiry;
        Config.TopicMaxTopicSize = maxTopicSize;

        return this;
    }

    /// <summary>
    ///     Configures message encryption using the specified encryptor.
    /// </summary>
    /// <param name="encryptor">The message encryptor to use for encrypting message payloads.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithEncryptor(IMessageEncryptor encryptor)
    {
        Config.MessageEncryptor = encryptor;

        return this;
    }

    /// <summary>
    ///     Registers an event handler for background processing errors.
    ///     Only invoked when background sending is enabled.
    /// </summary>
    /// <param name="handler">The event handler to invoke when background errors occur.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder SubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        OnBackgroundError = handler;
        return this;
    }

    /// <summary>
    ///     Registers an event handler for failed message batch sends.
    ///     Invoked when a batch of messages fails to send after all retry attempts are exhausted.
    /// </summary>
    /// <param name="handler">The event handler to invoke when message batches fail to send.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder SubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        OnMessageBatchFailed = handler;
        return this;
    }



    /// <summary>
    ///     Configures retry behavior for failed message sends.
    ///     Uses exponential backoff with configurable parameters.
    /// </summary>
    /// <param name="enabled">Whether retry is enabled. Default is true.</param>
    /// <param name="maxAttempts">The maximum number of retry attempts. Default is 3.</param>
    /// <param name="initialDelay">The initial delay before the first retry. Default is 100ms.</param>
    /// <param name="maxDelay">The maximum delay between retries. Default is 10 seconds.</param>
    /// <param name="backoffMultiplier">The multiplier for exponential backoff. Default is 2.0.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithRetry(bool enabled = true, int maxAttempts = 3,
        TimeSpan? initialDelay = null, TimeSpan? maxDelay = null, double backoffMultiplier = 2.0)
    {
        Config.EnableRetry = enabled;
        Config.MaxRetryAttempts = maxAttempts;
        Config.InitialRetryDelay = initialDelay ?? TimeSpan.FromMilliseconds(100);
        Config.MaxRetryDelay = maxDelay ?? TimeSpan.FromSeconds(10);
        Config.RetryBackoffMultiplier = backoffMultiplier;
        return this;
    }

    /// <summary>
    ///     Configures background message sending for asynchronous, batched message delivery.
    ///     When enabled, messages are queued and sent in batches for improved throughput.
    /// </summary>
    /// <param name="enabled">Whether background sending is enabled. Default is true.</param>
    /// <param name="queueCapacity">The maximum number of messages that can be queued. Default is 10,000.</param>
    /// <param name="batchSize">The number of messages to send in each batch. Default is 100.</param>
    /// <param name="flushInterval">The interval at which to flush pending messages. Default is 100ms.</param>
    /// <param name="disposalTimeout">
    ///     The timeout to wait for the background processor to complete during disposal. Default is
    ///     5 seconds.
    /// </param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithBackgroundSending(bool enabled = true, int queueCapacity = 10000,
        int batchSize = 100, TimeSpan? flushInterval = null, TimeSpan? disposalTimeout = null)
    {
        Config.EnableBackgroundSending = enabled;
        Config.BackgroundQueueCapacity = queueCapacity;
        Config.BackgroundBatchSize = batchSize;
        Config.BackgroundFlushInterval = flushInterval ?? TimeSpan.FromMilliseconds(100);
        Config.BackgroundDisposalTimeout = disposalTimeout ?? TimeSpan.FromSeconds(5);
        return this;
    }

    /// <summary>
    ///     Configures the logger factory for diagnostic logging.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to use for creating loggers.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IggyPublisherBuilder WithLogger(ILoggerFactory loggerFactory)
    {
        Config.LoggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    ///     Builds and returns a configured <see cref="IggyPublisher" /> instance.
    ///     Creates the Iggy client if needed, wires up all event handlers, and initializes the publisher.
    /// </summary>
    /// <returns>A fully configured <see cref="IggyPublisher" /> instance ready to send messages.</returns>
    /// <exception cref="ArgumentNullException">Thrown when IggyClient is null and CreateIggyClient is false.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the configuration is invalid.</exception>
    public IggyPublisher Build()
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
                LoggerFactory = Config.LoggerFactory ?? NullLoggerFactory.Instance
            });
        }

        var publisher = new IggyPublisher(IggyClient!, Config,
            Config.LoggerFactory?.CreateLogger<IggyPublisher>() ??
            NullLoggerFactory.Instance.CreateLogger<IggyPublisher>());

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
    ///     Validates the publisher configuration and throws if invalid.
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
                throw new InvalidOperationException("IggyClient must be provided when CreateIggyClient is false.");
            }
        }

        if (Config.CreateStream && string.IsNullOrWhiteSpace(Config.StreamName))
        {
            throw new InvalidOperationException("StreamName must be provided when CreateStream is true.");
        }

        if (Config.CreateTopic)
        {
            if (string.IsNullOrWhiteSpace(Config.TopicName))
            {
                throw new InvalidOperationException("TopicName must be provided when CreateTopic is true.");
            }

            if (Config.TopicPartitionsCount == 0)
            {
                throw new InvalidOperationException("TopicPartitionsCount must be greater than 0.");
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

        if (Config.EnableBackgroundSending)
        {
            if (Config.BackgroundQueueCapacity <= 0)
            {
                throw new InvalidOperationException(
                    "BackgroundQueueCapacity must be greater than 0 when EnableBackgroundSending is true.");
            }

            if (Config.BackgroundBatchSize <= 0)
            {
                throw new InvalidOperationException(
                    "BackgroundBatchSize must be greater than 0 when EnableBackgroundSending is true.");
            }

            if (Config.BackgroundFlushInterval <= TimeSpan.Zero)
            {
                throw new InvalidOperationException(
                    "BackgroundFlushInterval must be greater than zero when EnableBackgroundSending is true.");
            }

            if (Config.BackgroundDisposalTimeout <= TimeSpan.Zero)
            {
                throw new InvalidOperationException(
                    "BackgroundDisposalTimeout must be greater than zero when EnableBackgroundSending is true.");
            }
        }

        if (Config.EnableRetry)
        {
            if (Config.MaxRetryAttempts <= 0)
            {
                throw new InvalidOperationException(
                    "MaxRetryAttempts must be greater than 0 when EnableRetry is true.");
            }

            if (Config.InitialRetryDelay <= TimeSpan.Zero)
            {
                throw new InvalidOperationException(
                    "InitialRetryDelay must be greater than zero when EnableRetry is true.");
            }

            if (Config.MaxRetryDelay <= TimeSpan.Zero)
            {
                throw new InvalidOperationException(
                    "MaxRetryDelay must be greater than zero when EnableRetry is true.");
            }

            if (Config.InitialRetryDelay > Config.MaxRetryDelay)
            {
                throw new InvalidOperationException("InitialRetryDelay must be less than or equal to MaxRetryDelay.");
            }
        }
    }
}
