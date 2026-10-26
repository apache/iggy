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
    ///     <paramref name="options" /> controls compression, message expiry, maximum size, durability and the other
    ///     settings of the server's catalog. Call <see cref="IIggySystem.DescribeOptionsAsync" /> to see which keys a
    ///     server accepts.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream where the topic will be created (numeric ID or name).</param>
    /// <param name="name">The unique name of the topic (max 255 UTF-8 bytes).</param>
    /// <param name="partitionsCount">The number of partitions for the topic (max 1000).</param>
    /// <param name="options">The topic's settings. Null sends both durability policies as replicated and leaves the rest to the server defaults.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the created topic information, or null if
    ///     creation failed.
    /// </returns>
    Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        TopicOptions? options = null, CancellationToken token = default);

    /// <summary>
    ///     Applies the changes in <paramref name="update" /> to an existing topic, leaving every other setting as is.
    /// </summary>
    /// <remarks>
    ///     An update renames the topic, sets its message expiry and size cap, and resets those two to the server
    ///     defaults. Settings fixed at creation have no property on <see cref="TopicUpdate" />, and a reset of one
    ///     is refused before anything is sent.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to update (numeric ID or name).</param>
    /// <param name="update">The keys to set and reset. At least one is required.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task UpdateTopicAsync(Identifier streamId, Identifier topicId, TopicUpdate update,
        CancellationToken token = default);

    /// <summary>
    ///     Deletes an existing topic and all its associated messages and partitions.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to delete (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default);
}
