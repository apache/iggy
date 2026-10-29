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
    ///     A deferred (long) poll: waits until <see cref="DeferredPollOptions.MinCount" /> messages are readable, or
    ///     until <see cref="DeferredPollOptions.MaxWait" /> passes, and returns up to <paramref name="count" />
    ///     messages. An expired wait returns what is readable, which can be nothing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         <see cref="DeferredPollOptions.RequestTimeout" /> replaces the operation timeout of the client for this
    ///         call. A poll that can move an offset (<paramref name="autoCommit" /> or <see cref="PollingStrategy.Next" />)
    ///         is not sent again after a lost reply, as with
    ///         <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool, CancellationToken)" />.
    ///         While it waits, the offset stores and deletes of the same client on its partition wait behind it, and
    ///         its sends do not.
    ///         A canceled poll is also canceled on the server.
    ///     </para>
    ///     <para>
    ///         Over TCP, a consumer-group poll needs a partition, and one without a partition throws
    ///         <see cref="IggyInvalidStatusCodeException" /> with status 3 (InvalidCommand): it would wait on one
    ///         partition while others hold messages.
    ///         A member fetches its partitions with <see cref="FetchMessagesAsync" /> instead.
    ///     </para>
    ///     <para>
    ///         The default implementation throws <see cref="FeatureUnavailableException" />: a client must opt in, so
    ///         a wait never turns into an immediate poll without notice.
    ///     </para>
    /// </remarks>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 4009, 3 or 80 when <paramref name="options" /> break the rules of
    ///     <see cref="DeferredPollOptions" />; nothing is sent then.
    /// </exception>
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit, DeferredPollOptions options,
        CancellationToken token = default)
    {
        return Task.FromException<PolledMessages>(new FeatureUnavailableException());
    }

    /// <summary>
    ///     <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool, DeferredPollOptions, CancellationToken)" />
    ///     with the payload buffers rented from a shared pool, as
    ///     <see cref="PollMessagesRentedAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool, CancellationToken)" />
    ///     rents them.
    /// </summary>
    Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit, DeferredPollOptions options,
        CancellationToken token = default)
    {
        return Task.FromException<PolledMessagesRental>(new FeatureUnavailableException());
    }

    /// <summary>
    ///     <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool, DeferredPollOptions, CancellationToken)" />
    ///     with the strategy chosen once the partition is known, as
    ///     <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, Func{uint, PollingStrategy}, uint, bool, CancellationToken)" />
    ///     chooses it.
    /// </summary>
    Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        DeferredPollOptions options, CancellationToken token = default)
    {
        return PollMessagesAsync(streamId, topicId, partitionId, consumer,
            StrategyForKnownPartition(partitionId, consumer, pollingStrategyFor), count, autoCommit, options, token);
    }

    /// <summary>
    ///     <see cref="PollMessagesAsync(Identifier, Identifier, uint?, Consumer, Func{uint, PollingStrategy}, uint, bool, DeferredPollOptions, CancellationToken)" />
    ///     with the payload buffers rented from a shared pool.
    /// </summary>
    Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        DeferredPollOptions options, CancellationToken token = default)
    {
        return PollMessagesRentedAsync(streamId, topicId, partitionId, consumer,
            StrategyForKnownPartition(partitionId, consumer, pollingStrategyFor), count, autoCommit, options, token);
    }

    /// <summary>
    ///     Fetches several partitions of one readiness group in one deferred request. The server waits until
    ///     <see cref="DeferredPollOptions.MinCount" /> messages are readable over all targets, or until
    ///     <see cref="DeferredPollOptions.MaxWait" /> passes, and returns one section per target that it read or
    ///     refused. An expired wait returns what is readable, which can be nothing.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Pass the partitions of one group of <see cref="ReadinessGroupsAsync" />: the call goes to the primary
    ///         of the first target. Each target starts at <see cref="PollingStrategy.Offset" /> or
    ///         <see cref="PollingStrategy.Next" /> of <paramref name="consumer" />, under
    ///         <see cref="PollingStrategy.Context" /> or the context of its route. The server reads the targets in
    ///         the given order, each up to <see cref="DeferredPollOptions.MaxBytes" />, while the reply holds fewer
    ///         than <paramref name="count" /> messages and <see cref="DeferredPollOptions.MaxBytes" /> bytes. So
    ///         move the targets that got messages to the end of the next call, so that every target gets its turn.
    ///     </para>
    ///     <para>
    ///         Send each target next as <see cref="PollingStrategy.Offset" /> of
    ///         <see cref="FetchedSection.NextOffset" /> with <see cref="FetchedSection.Context" />. A section with a
    ///         nonzero <see cref="FetchedSection.Status" /> read nothing, and a target without a section was not read,
    ///         so both keep the position they were sent with. A section or a refusal as not primary (89) makes the
    ///         next <see cref="ReadinessGroupsAsync" /> look the routes up again.
    ///     </para>
    ///     <para>
    ///         A fetch that can move an offset (<paramref name="autoCommit" /> or a
    ///         <see cref="PollingStrategy.Next" /> target) is not sent again after a lost reply. While it waits, the
    ///         offset stores and deletes of the same client on its partitions wait behind it, and its sends do not.
    ///         A fetch with <paramref name="autoCommit" /> cannot be canceled once it was sent, because the server
    ///         can commit its messages before a cancel arrives: it ends with its reply or its request timeout. Any
    ///         other canceled fetch is also canceled on the server.
    ///         An unknown committing outcome throws <see cref="VsrFetchProgressUncertainException" /> and blocks
    ///         affected partitions until explicit abandonment. Partial replies put that error in the affected
    ///         sections' <see cref="FetchedSection.ProgressError" /> while preserving healthy sections.
    ///     </para>
    ///     <para>
    ///         The caller disposes the reply. The default implementation is for clients without the command, such as
    ///         HTTP: it throws <see cref="FeatureUnavailableException" />.
    ///     </para>
    /// </remarks>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="targets">1 to 1000 partitions of one readiness group, each with the position it starts at.</param>
    /// <param name="consumer">The consumer whose position <see cref="PollingStrategy.Next" /> means.</param>
    /// <param name="count">The most messages of the whole reply.</param>
    /// <param name="autoCommit">If true, the server commits the offset of each section it returns.</param>
    /// <param name="options">The wait, the minimum count, the byte limit and the request timeout of the call.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 4009, 3 or 80 when <paramref name="options" /> break the rules of
    ///     <see cref="DeferredPollOptions" />, or 3 when the request timeout is at most one second or does not cover
    ///     the wait plus one second,
    ///     for 0 or more than 1000 targets, for a partition named twice, or for another start point; nothing is sent
    ///     then.
    /// </exception>
    Task<FetchedMessages> FetchMessagesAsync(Identifier streamId, Identifier topicId,
        IReadOnlyList<(uint PartitionId, PollingStrategy Strategy)> targets, Consumer consumer, uint count,
        bool autoCommit, DeferredPollOptions options, CancellationToken token = default)
    {
        return Task.FromException<FetchedMessages>(new FeatureUnavailableException());
    }

    /// <summary>
    ///     Groups <paramref name="partitions" /> by the primary that serves them, each group small enough for one
    ///     <see cref="FetchMessagesAsync" />, and keeps their order within each group. The default implementation
    ///     is for clients without primaries, such as HTTP: it throws <see cref="FeatureUnavailableException" />.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="partitions">The partitions to group.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    Task<IReadOnlyList<IReadOnlyList<uint>>> ReadinessGroupsAsync(Identifier streamId, Identifier topicId,
        IReadOnlyList<uint> partitions, CancellationToken token = default)
    {
        return Task.FromException<IReadOnlyList<IReadOnlyList<uint>>>(new FeatureUnavailableException());
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
