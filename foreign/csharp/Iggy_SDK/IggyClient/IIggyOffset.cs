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
using Apache.Iggy.Kinds;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for managing consumer offsets in an Iggy client.
///     Offsets track the position of a consumer in a topic's partition, enabling resumption of message consumption.
/// </summary>
public interface IIggyOffset
{
    /// <summary>
    ///     Stores the current offset for a consumer at a specific position in a topic partition.
    /// </summary>
    /// <remarks>
    ///     Over TCP, the write goes to the partition primary under the context the partition's route reports. After
    ///     another client deleted and recreated the partition, it can fail once with status 87, or 5009 for a
    ///     consumer group, from a route this client cached before. The client does not retry it: the failed route
    ///     is dropped and the next call routes again.
    /// </remarks>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="offset">The offset value to store (message index position).</param>
    /// <param name="partitionId">
    ///     The specific partition for the offset. For consumer group can be null, it will be save offset
    ///     for assigned partition.
    /// </param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset, uint? partitionId,
        CancellationToken token = default);

    /// <summary>
    ///     Stores an offset under the context of the poll that delivered it. The server refuses the store with
    ///     status 87 when the partition was deleted and recreated since that poll, and with status 5009 when the
    ///     partition moved to another member of the consumer group.
    /// </summary>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="offset">The offset value to store (message index position).</param>
    /// <param name="partitionId">The partition that delivered the offset.</param>
    /// <param name="context">The <see cref="PolledMessages.Context" /> of the poll that delivered the offset.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset, uint partitionId,
        PartitionContext context, CancellationToken token = default);

    /// <summary>
    ///     Retrieves the current offset for a consumer in a specified topic.
    /// </summary>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="partitionId">
    ///     The specific partition for the offset. For consumer group can be null, it will return offset
    ///     for assigned partition.
    /// </param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the offset information, or null if not found.</returns>
    Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default);

    /// <summary>
    ///     Deletes the stored offset for a consumer in a specified topic.
    /// </summary>
    /// <remarks>
    ///     Over TCP, the write goes to the partition primary under the context the partition's route reports. After
    ///     another client deleted and recreated the partition, it can fail once with status 87, or 5009 for a
    ///     consumer group, from a route this client cached before. The client does not retry it: the failed route
    ///     is dropped and the next call routes again.
    /// </remarks>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="partitionId">
    ///     The specific partition for the offset. For consumer group can be null, it will return offset
    ///     for assigned partition.
    /// </param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default);
}
