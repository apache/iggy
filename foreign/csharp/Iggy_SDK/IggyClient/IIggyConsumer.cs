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
using Apache.Iggy.Exceptions;
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
    ///     <para>
    ///         Over TCP, a poll without <see cref="PollingStrategy.Context" /> is served under the context the
    ///         partition's route reports. After another client deleted and recreated the partition, it can fail once
    ///         with status 87, or 5009 for a consumer group, from a route this client cached before. The client does
    ///         not retry it: the failed route is dropped and the next poll routes again.
    ///     </para>
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to consume from (numeric ID or name).</param>
    /// <param name="partitionId">The specific partition to consume from, or null to consume from any partition.</param>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="pollingStrategy">The strategy for determining where to start reading messages.</param>
    /// <param name="count">The maximum number of messages to retrieve.</param>
    /// <param name="autoCommit">If true, automatically commit the offset after polling.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the polled messages. A consumer-group
    ///     poll whose member currently owns no partition returns an empty batch whose
    ///     <see cref="PolledMessages.PartitionId" /> is <see cref="PolledMessages.NoAssignedPartition" />; do
    ///     not key offsets on it, back off and poll again.
    /// </returns>
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit,
        CancellationToken token = default);

    /// <summary>
    ///     Polls messages from a specified topic and partition while renting the payload buffers from a shared pool
    ///     instead of copying them into byte arrays.
    /// </summary>
    /// <remarks>
    ///     The returned rental must be disposed when the caller is done reading the payload and raw header memory.
    ///     Payload and raw header slices are invalidated once the rental is disposed. A consumer-group poll whose
    ///     member currently owns no partition returns an empty batch whose
    ///     <see cref="PolledMessagesRental.PartitionId" /> is <see cref="PolledMessages.NoAssignedPartition" />;
    ///     do not key offsets on it, back off and poll again.
    ///     <para>
    ///         Over TCP, a poll without <see cref="PollingStrategy.Context" /> is served under the context the
    ///         partition's route reports. After another client deleted and recreated the partition, it can fail once
    ///         with status 87, or 5009 for a consumer group, from a route this client cached before. The client does
    ///         not retry it: the failed route is dropped and the next poll routes again.
    ///     </para>
    /// </remarks>
    Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit,
        CancellationToken token = default);

    /// <summary>
    ///     Polls messages with the strategy chosen once the partition is known. A consumer-group poll without a
    ///     partition picks one of the member's assigned partitions first and then asks
    ///     <paramref name="pollingStrategyFor" /> for it, so a caller can continue every partition from its own
    ///     position. Any other poll asks it for the given partition, or for 0 when none was given.
    /// </summary>
    /// <remarks>
    ///     The default implementation is for clients that cannot pick a partition client-side: a consumer-group poll
    ///     without a partition throws <see cref="FeatureUnavailableException" />.
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic to consume from (numeric ID or name).</param>
    /// <param name="partitionId">The specific partition to consume from, or null to consume from any partition.</param>
    /// <param name="consumer">The consumer identifier (group ID or member ID).</param>
    /// <param name="pollingStrategyFor">The strategy for the partition the poll reads.</param>
    /// <param name="count">The maximum number of messages to retrieve.</param>
    /// <param name="autoCommit">If true, automatically commit the offset after polling.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        CancellationToken token = default)
    {
        return PollMessagesAsync(streamId, topicId, partitionId, consumer,
            StrategyForKnownPartition(partitionId, consumer, pollingStrategyFor), count, autoCommit, token);
    }

    /// <summary>
    ///     <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, Func{uint, PollingStrategy}, uint, bool, CancellationToken)" />
    ///     with the payload buffers rented from a shared pool, as
    ///     <see cref="PollMessagesRentedAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool, CancellationToken)" />
    ///     rents them.
    /// </summary>
    Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        CancellationToken token = default)
    {
        return PollMessagesRentedAsync(streamId, topicId, partitionId, consumer,
            StrategyForKnownPartition(partitionId, consumer, pollingStrategyFor), count, autoCommit, token);
    }

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

    /// <summary>
    ///     Polls messages from a specified topic using a pre-constructed request while renting the payload buffers
    ///     from a shared pool.
    /// </summary>
    Task<PolledMessagesRental> PollMessagesRentedAsync(MessageFetchRequest request, CancellationToken token = default)
    {
        return PollMessagesRentedAsync(request.StreamId, request.TopicId, request.PartitionId, request.Consumer,
            request.PollingStrategy, request.Count, request.AutoCommit, token);
    }

    private static PollingStrategy StrategyForKnownPartition(uint? partitionId, Consumer consumer,
        Func<uint, PollingStrategy> pollingStrategyFor)
    {
        if (consumer.Type == ConsumerType.ConsumerGroup && partitionId is null)
        {
            throw new FeatureUnavailableException();
        }

        return pollingStrategyFor(partitionId ?? 0);
    }
}
