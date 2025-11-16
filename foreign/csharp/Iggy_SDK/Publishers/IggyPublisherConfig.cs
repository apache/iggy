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
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Configuration for a typed Iggy publisher that serializes objects of type T to messages
/// </summary>
/// <typeparam name="T">The type to serialize to messages</typeparam>
public class IggyPublisherConfig<T> : IggyPublisherConfig
{
    /// <summary>
    ///     Gets or sets the serializer used to convert objects of type T to message payloads.
    ///     This property is required and cannot be null. The builder will validate that a serializer
    ///     is provided before creating the publisher instance.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown during publisher build if this property is null.
    /// </exception>
    public required ISerializer<T> Serializer { get; set; }
}

/// <summary>
///     Configuration settings for <see cref="IggyPublisher" />.
///     Provides comprehensive options for configuring message publishing behavior including
///     connection settings, stream/topic management, background processing, and retry policies.
/// </summary>
public class IggyPublisherConfig
{
    /// <summary>
    ///     Gets or sets a value indicating whether the publisher should create its own Iggy client.
    ///     When true, the publisher will instantiate and manage its own client instance.
    ///     When false, an external client must be provided.
    /// </summary>
    public bool CreateIggyClient { get; set; }

    /// <summary>
    ///     Gets or sets the protocol to use for communication (TCP, QUIC, or HTTP).
    ///     Only used when <see cref="CreateIggyClient" /> is true.
    /// </summary>
    public Protocol Protocol { get; set; }

    /// <summary>
    ///     Gets or sets the server address to connect to.
    ///     Format depends on protocol (e.g., "localhost:8090" for TCP/QUIC, "http://localhost:3000" for HTTP).
    ///     Only used when <see cref="CreateIggyClient" /> is true.
    /// </summary>
    public string Address { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the login username for authentication.
    ///     Required if the Iggy server has authentication enabled.
    /// </summary>
    public string Login { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the password for authentication.
    ///     Required if the Iggy server has authentication enabled.
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the identifier of the target stream.
    ///     Can be either a numeric ID or a string name.
    /// </summary>
    public Identifier StreamId { get; set; }

    /// <summary>
    ///     Gets or sets the identifier of the target topic.
    ///     Can be either a numeric ID or a string name.
    /// </summary>
    public Identifier TopicId { get; set; }

    /// <summary>
    ///     Gets or sets the size of the receive buffer in bytes.
    ///     Default is 4096 bytes (4 KB).
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 4096;

    /// <summary>
    ///     Gets or sets the size of the send buffer in bytes.
    ///     Default is 4096 bytes (4 KB).
    /// </summary>
    public int SendBufferSize { get; set; } = 4096;

    /// <summary>
    ///     Gets or sets the partitioning strategy for messages.
    ///     Determines how messages are distributed across topic partitions.
    ///     Default is <see cref="Partitioning.None()" /> (balanced partitioning).
    /// </summary>
    public Partitioning Partitioning { get; set; } = Partitioning.None();

    /// <summary>
    ///     Gets or sets a value indicating whether to automatically create the stream if it doesn't exist.
    ///     When true, requires <see cref="StreamName" /> to be set.
    /// </summary>
    public bool CreateStream { get; set; }

    /// <summary>
    ///     Gets or sets the name to use when creating the stream.
    ///     Required when <see cref="CreateStream" /> is true.
    /// </summary>
    public string? StreamName { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether to automatically create the topic if it doesn't exist.
    ///     When true, requires <see cref="TopicName" /> to be set.
    /// </summary>
    public bool CreateTopic { get; set; }

    /// <summary>
    ///     Gets or sets the name to use when creating the topic.
    ///     Required when <see cref="CreateTopic" /> is true.
    /// </summary>
    public string? TopicName { get; set; }

    /// <summary>
    ///     Gets or sets the message encryptor for encrypting message payloads.
    ///     When set, all message payloads will be encrypted before sending.
    /// </summary>
    public IMessageEncryptor? MessageEncryptor { get; set; } = null;

    /// <summary>
    ///     Gets or sets the logger factory for diagnostic logging.
    ///     When set, enables detailed logging of publisher operations.
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; set; } = null;

    /// <summary>
    ///     Gets or sets the number of partitions to create when creating the topic.
    ///     Only used when <see cref="CreateTopic" /> is true.
    /// </summary>
    public uint TopicPartitionsCount { get; set; }

    /// <summary>
    ///     Gets or sets the compression algorithm to use for the topic.
    ///     Only used when <see cref="CreateTopic" /> is true.
    /// </summary>
    public CompressionAlgorithm TopicCompressionAlgorithm { get; set; }

    /// <summary>
    ///     Gets or sets the replication factor for the topic.
    ///     Determines how many replicas of each partition are maintained.
    ///     Only used when <see cref="CreateTopic" /> is true.
    /// </summary>
    public byte? TopicReplicationFactor { get; set; }

    /// <summary>
    ///     Gets or sets the message expiry time in seconds (0 for no expiry).
    ///     Messages older than this will be automatically deleted.
    ///     Only used when <see cref="CreateTopic" /> is true.
    /// </summary>
    public ulong TopicMessageExpiry { get; set; }

    /// <summary>
    ///     Gets or sets the maximum size of the topic in bytes (0 for unlimited).
    ///     When the topic reaches this size, oldest messages will be deleted.
    ///     Only used when <see cref="CreateTopic" /> is true.
    /// </summary>
    public ulong TopicMaxTopicSize { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether background sending is enabled.
    ///     When enabled, messages are queued and sent asynchronously in batches.
    ///     Default is false (synchronous sending).
    /// </summary>
    public bool EnableBackgroundSending { get; set; } = false;

    /// <summary>
    ///     Gets or sets the capacity of the background message queue.
    ///     When the queue is full, new messages will block until space becomes available.
    ///     Only used when <see cref="EnableBackgroundSending" /> is true.
    ///     Default is 10,000 messages.
    /// </summary>
    public int BackgroundQueueCapacity { get; set; } = 10000;

    /// <summary>
    ///     Gets or sets the number of messages to send in each batch.
    ///     Larger batches improve throughput but increase latency.
    ///     Only used when <see cref="EnableBackgroundSending" /> is true.
    ///     Default is 100 messages.
    /// </summary>
    public int BackgroundBatchSize { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the interval at which to flush pending messages.
    ///     Messages are sent either when the batch size is reached or this interval expires.
    ///     Only used when <see cref="EnableBackgroundSending" /> is true.
    ///     Default is 100 milliseconds.
    /// </summary>
    public TimeSpan BackgroundFlushInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Gets or sets the timeout to wait for the background processor to complete during disposal.
    ///     Only used when <see cref="EnableBackgroundSending" /> is true.
    ///     Default is 5 seconds.
    /// </summary>
    public TimeSpan BackgroundDisposalTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets a value indicating whether retry is enabled for failed sends.
    ///     When enabled, failed send operations will be retried according to the retry policy.
    ///     Default is true.
    /// </summary>
    public bool EnableRetry { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts.
    ///     After this many failed attempts, the operation will fail permanently.
    ///     Only used when <see cref="EnableRetry" /> is true.
    ///     Default is 3 attempts.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the initial delay before the first retry.
    ///     Subsequent retries use exponential backoff based on <see cref="RetryBackoffMultiplier" />.
    ///     Only used when <see cref="EnableRetry" /> is true.
    ///     Default is 100 milliseconds.
    /// </summary>
    public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Gets or sets the maximum delay between retries.
    ///     Prevents exponential backoff from growing indefinitely.
    ///     Only used when <see cref="EnableRetry" /> is true.
    ///     Default is 10 seconds.
    /// </summary>
    public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    ///     Gets or sets the multiplier for exponential backoff retry delays.
    ///     Each retry delay is calculated as: min(InitialRetryDelay * (multiplier ^ attempt), MaxRetryDelay).
    ///     Only used when <see cref="EnableRetry" /> is true.
    ///     Default is 2.0 (doubles the delay each time).
    /// </summary>
    public double RetryBackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    ///     Gets or sets the reconnection settings to control the behavior of the iggy client
    ///     in case of a disconnect or network failure.
    ///     This property is optional and can be null. If null, reconnection will be disabled.
    /// </summary>
    public ReconnectionSettings? ReconnectionSettings { get; set; }
}
