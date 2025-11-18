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
///     Defines methods for managing consumer groups in an Iggy client.
///     Consumer groups enable multiple consumers to coordinate message consumption and share offset tracking.
/// </summary>
public interface IIggyConsumerGroup
{
    /// <summary>
    ///     Retrieves information about all consumer groups for a specified topic.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns a read-only collection of consumer group
    ///     information.
    /// </returns>
    Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId, Identifier topicId,
        CancellationToken token = default);

    /// <summary>
    ///     Retrieves detailed information about a specific consumer group.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the consumer group information, or null if not
    ///     found.
    /// </returns>
    Task<ConsumerGroupResponse?> GetConsumerGroupByIdAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default);

    /// <summary>
    ///     Creates a new consumer group for a specified topic.
    /// </summary>
    /// <remarks>
    ///     Consumer groups allow multiple consumers to coordinate message consumption and share offset tracking.
    ///     Members of the same group consume from different partitions to load-balance message processing.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="name">The unique name of the consumer group.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the created consumer group information, or null
    ///     if creation failed.
    /// </returns>
    Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId, string name,
        CancellationToken token = default);

    /// <summary>
    ///     Deletes an existing consumer group.
    /// </summary>
    /// <remarks>
    ///     This operation removes the consumer group and its associated offset tracking.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group to delete (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default);

    /// <summary>
    ///     Joins a consumer group, registering the current client as a member.
    /// </summary>
    /// <remarks>
    ///     When a consumer joins a group, partitions may be rebalanced among group members to distribute the load. Available
    ///     only for TCP.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group to join (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default);

    /// <summary>
    ///     Leaves a consumer group, unregistering the current client as a member.
    /// </summary>
    /// <remarks>
    ///     When a consumer leaves a group, partitions may be rebalanced among remaining members. Available only for TCP.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group to leave (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default);
}
