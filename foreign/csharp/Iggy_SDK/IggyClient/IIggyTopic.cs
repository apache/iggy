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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for managing topics within streams in an Iggy client.
///     Topics are the primary publishing and consuming unit within a stream, organizing messages into partitions.
/// </summary>
public interface IIggyTopic
{
    /// <summary>
    ///     Retrieves information about all topics in a specified stream.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topics (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns a read-only collection of topic information.</returns>
    Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId, CancellationToken token = default);

    /// <summary>
    ///     Retrieves detailed information about a specific topic by its identifier.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to retrieve (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the topic information, or null if the topic was
    ///     not found.
    /// </returns>
    Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);

    /// <summary>
    ///     Creates a new topic in a specified stream with the given configuration.
    /// </summary>
    /// <remarks>
    ///     The topic name must be unique within the stream. Topics contain one or more partitions for distributing messages.
    ///     Additional parameters control message expiry, compression, replication, and maximum size.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream where the topic will be created (numeric ID or name).</param>
    /// <param name="name">The unique name of the topic (max 255 characters).</param>
    /// <param name="partitionsCount">The number of partitions for the topic (max 1000).</param>
    /// <param name="compressionAlgorithm">The compression algorithm to use for messages (default: None).</param>
    /// <param name="replicationFactor">The replication factor for the topic (optional).</param>
    /// <param name="messageExpiry">The message expiry period in milliseconds (0 = never expire).</param>
    /// <param name="maxTopicSize">The maximum size of the topic in bytes (0 = unlimited).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the created topic information, or null if
    ///     creation failed.
    /// </returns>
    Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        ulong messageExpiry = 0, ulong maxTopicSize = 0, CancellationToken token = default);

    /// <summary>
    ///     Updates the configuration of an existing topic.
    /// </summary>
    /// <remarks>
    ///     This method allows updating topic properties such as name, compression algorithm, size limits, message expiry, and
    ///     replication factor.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to update (numeric ID or name).</param>
    /// <param name="name">The new name for the topic (max 255 characters).</param>
    /// <param name="compressionAlgorithm">The new compression algorithm to use (default: None).</param>
    /// <param name="maxTopicSize">The new maximum size of the topic in bytes (0 = unlimited).</param>
    /// <param name="messageExpiry">The new message expiry period in milliseconds (0 = never expire).</param>
    /// <param name="replicationFactor">The new replication factor (optional).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task UpdateTopicAsync(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, ulong maxTopicSize = 0,
        ulong messageExpiry = 0, byte? replicationFactor = null, CancellationToken token = default);

    /// <summary>
    ///     Deletes an existing topic and all its associated messages and partitions.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to delete (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);

    /// <summary>
    ///     Purges all messages from a topic while keeping the topic and its partitions intact.
    /// </summary>
    /// <remarks>
    ///     This operation removes all messages from all partitions within the topic.
    ///     The topic structure remains intact, allowing new messages to be published afterwards.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to purge (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
}
