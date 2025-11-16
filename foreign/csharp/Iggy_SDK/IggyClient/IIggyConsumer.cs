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
///     Defines methods for consuming messages from topics in an Iggy client.
/// </summary>
public interface IIggyConsumer
{
    /// <summary>
    ///     Polls messages from a specified topic and partition with a given polling strategy.
    /// </summary>
    /// <remarks>
    ///     This method retrieves messages from a topic based on the specified consumer and polling strategy.
    ///     The polling strategy determines where to start reading messages (e.g., from a specific offset, latest, earliest).
    ///     If a partition ID is not specified, messages can be consumed from any partition.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to consume from (numeric ID or name).</param>
    /// <param name="partitionId">The specific partition to consume from, or null to consume from any partition.</param>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="pollingStrategy">The strategy for determining where to start reading messages.</param>
    /// <param name="count">The maximum number of messages to retrieve.</param>
    /// <param name="autoCommit">If true, automatically commit the offset after polling.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the polled messages.</returns>
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit,
        CancellationToken token = default);

    /// <summary>
    ///     Polls messages from a specified topic using a pre-constructed request.
    /// </summary>
    /// <remarks>
    ///     This is a convenience method that wraps the full PollMessagesAsync method using a request object.
    /// </remarks>
    /// <param name="request">The message fetch request containing all polling parameters.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the polled messages.</returns>
    Task<PolledMessages> PollMessagesAsync(MessageFetchRequest request, CancellationToken token = default)
    {
        return PollMessagesAsync(request.StreamId, request.TopicId, request.PartitionId, request.Consumer,
            request.PollingStrategy, request.Count, request.AutoCommit, token);
    }
}
