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


using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Information about a topic.
/// </summary>
public sealed class TopicResponse
{
    /// <summary>
    ///     Topic identifier.
    /// </summary>
    public required uint Id { get; init; }

    /// <summary>
    ///     Topic creation date.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    ///     Unique topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    ///     Compression algorithm used for the topic.
    /// </summary>
    public CompressionAlgorithm CompressionAlgorithm { get; set; }

    /// <summary>
    ///     Topic size in bytes.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong Size { get; init; }

    /// <summary>
    ///     Message expiry in milliseconds.
    /// </summary>
    public ulong MessageExpiry { get; init; }

    /// <summary>
    ///     Maximum topic size in bytes.
    /// </summary>
    public required ulong MaxTopicSize { get; init; }

    /// <summary>
    ///     Number of messages in the topic.
    /// </summary>
    public required ulong MessagesCount { get; init; }

    /// <summary>
    ///     Number of partitions in the topic.
    /// </summary>
    public required uint PartitionsCount { get; init; }

    /// <summary>
    ///     Replication factor of the topic.
    /// </summary>
    public required byte? ReplicationFactor { get; init; }

    /// <summary>
    ///     List of partitions in the topic.
    /// </summary>
    public IEnumerable<PartitionResponse>? Partitions { get; init; }
}
