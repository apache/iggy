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
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Configuration for a typed Iggy consumer that deserializes messages to type T
/// </summary>
/// <typeparam name="T">The type to deserialize messages to</typeparam>
public class IggyConsumerConfig<T> : IggyConsumerConfig
{
    /// <summary>
    ///     Gets or sets the deserializer used to convert message payloads to type T.
    ///     This property is required and cannot be null. The builder will validate that a deserializer
    ///     is provided before creating the consumer instance.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown during consumer build if this property is null.
    /// </exception>
    public required IDeserializer<T> Deserializer { get; set; }
}

/// <summary>
///     Configuration settings for an Iggy consumer
/// </summary>
public class IggyConsumerConfig
{
    /// <summary>
    ///     Whether to create a new Iggy client internally. If false, an existing client should be provided.
    /// </summary>
    public bool CreateIggyClient { get; set; }

    /// <summary>
    ///     The protocol to use for communication (TCP, QUIC, HTTP)
    /// </summary>
    public Protocol Protocol { get; set; }

    /// <summary>
    ///     The server address to connect to
    /// </summary>
    public string Address { get; set; } = string.Empty;

    /// <summary>
    ///     The username for authentication
    /// </summary>
    public string Login { get; set; } = string.Empty;

    /// <summary>
    ///     The password for authentication
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    ///     The size of the receive buffer in bytes. Default is 4096.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 4096;

    /// <summary>
    ///     The size of the send buffer in bytes. Default is 4096.
    /// </summary>
    public int SendBufferSize { get; set; } = 4096;

    /// <summary>
    ///     The identifier of the stream to consume from
    /// </summary>
    public Identifier StreamId { get; set; }

    /// <summary>
    ///     The identifier of the topic to consume from
    /// </summary>
    public Identifier TopicId { get; set; }

    /// <summary>
    ///     Optional message encryptor for decrypting messages
    /// </summary>
    public IMessageEncryptor? MessageEncryptor { get; set; } = null;

    /// <summary>
    ///     Optional partition ID to consume from. If null, consumes from all partitions.
    ///     Note: This is ignored when using consumer groups.
    /// </summary>
    public uint? PartitionId { get; set; }

    /// <summary>
    ///     The consumer configuration (single consumer or consumer group)
    /// </summary>
    public Consumer Consumer { get; set; }

    /// <summary>
    ///     The polling strategy defining from where to start consuming messages
    /// </summary>
    public PollingStrategy PollingStrategy { get; set; }

    /// <summary>
    ///     The maximum number of messages to fetch in a single poll. Default is 100.
    /// </summary>
    public uint BatchSize { get; set; } = 100;

    /// <summary>
    ///     Whether to enable automatic offset committing
    /// </summary>
    public bool AutoCommit { get; set; }

    /// <summary>
    ///     The auto-commit mode determining when offsets are stored
    /// </summary>
    public AutoCommitMode AutoCommitMode { get; set; }

    /// <summary>
    ///     The name of the consumer group (used when consumer ID is numeric)
    /// </summary>
    public string? ConsumerGroupName { get; set; }

    /// <summary>
    ///     Whether to create the consumer group if it doesn't exist. Default is true.
    /// </summary>
    public bool CreateConsumerGroupIfNotExists { get; set; } = true;

    /// <summary>
    ///     Whether to automatically join the consumer group. Default is true.
    /// </summary>
    public bool JoinConsumerGroup { get; set; } = true;

    /// <summary>
    ///     The interval in milliseconds between message polls. Default is 100ms.
    /// </summary>
    public int PollingIntervalMs { get; set; } = 100;

    /// <summary>
    ///     Optional logger factory for creating loggers
    /// </summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    /// <summary>
    ///     Gets or sets the reconnection settings to control the behavior of the iggy client
    ///     in case of a disconnect or network failure.
    ///     This property is optional and can be null. If null, reconnection will be disabled.
    /// </summary>
    public ReconnectionSettings? ReconnectionSettings { get; set; }
}
