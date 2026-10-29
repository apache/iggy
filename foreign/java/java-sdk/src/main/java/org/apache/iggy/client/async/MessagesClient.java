/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.client.async;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async client interface for message operations (producing and consuming).
 *
 * <p>This is the core interface for interacting with Iggy's message streaming
 * capabilities. It supports both sending (producing) and polling (consuming)
 * messages asynchronously via {@link CompletableFuture}.
 *
 * <h2>Producing Messages</h2>
 * <pre>{@code
 * MessagesClient messages = client.messages();
 *
 * // Send messages with balanced partitioning (round-robin)
 * var msgs = List.of(Message.of("order-created"), Message.of("order-updated"));
 * messages.sendMessages(streamId, topicId, Partitioning.balanced(), msgs)
 *     .thenRun(() -> System.out.println("Messages sent"));
 *
 * // Send with message key partitioning (ensures ordering per key)
 * messages.sendMessages(streamId, topicId, Partitioning.messagesKey("user-123"), msgs);
 * }</pre>
 *
 * <h2>Consuming Messages</h2>
 * <pre>{@code
 * // Poll from the beginning
 * messages.pollMessages(streamId, topicId, Optional.empty(),
 *         Consumer.of(1L), PollingStrategy.first(), 100L, true)
 *     .thenAccept(polled -> {
 *         for (var msg : polled.messages()) {
 *             System.out.println(new String(msg.payload(), StandardCharsets.UTF_8));
 *         }
 *     });
 * }</pre>
 *
 * @see Partitioning
 * @see PollingStrategy
 * @see Consumer
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#messages()
 */
public interface MessagesClient {

    /**
     * Polls messages from a topic partition asynchronously.
     *
     * <p>Messages are retrieved according to the specified {@link PollingStrategy}, which
     * controls where in the partition to start reading (e.g., from the beginning, end,
     * a specific offset, or a timestamp).
     *
     * <p>When {@code autoCommit} is {@code true}, the server submits an offset commit
     * before returning the poll response, without waiting for application processing.
     * A failure can therefore leave the stored offset ahead of processed messages.
     *
     * <p>A poll without a caller context takes the partition context from the client's
     * route to the partition, and the client caches that route. After another client
     * deletes and recreates the partition, such a poll can fail once with error 87
     * (history unavailable), or 5009 (partition not owned) for a group consumer. The
     * client returns that error and does not retry it. It drops the failed route, so the
     * next call routes again. A group poll without a partition ID is the exception for
     * 5009: it syncs the assignment again and retries once, unless its strategy has a
     * context.
     *
     * <p>To continue at an offset from an earlier poll, pass the
     * {@link PolledMessages#context()} of that poll with {@link PollingStrategy#withContext}.
     * Then a poll of a recreated partition is refused instead of reading it at an offset
     * from the old incarnation.
     *
     * @param streamId    the stream identifier (numeric or string-based)
     * @param topicId     the topic identifier (numeric or string-based)
     * @param partitionId optional partition ID to poll from; when empty and polling with a
     *                    group consumer, the client selects the partition round-robin from
     *                    the member's synced group assignment (the group must be joined
     *                    first)
     * @param consumer    the consumer identity, either individual ({@link Consumer#of(Long)})
     *                    or group ({@link Consumer#group(Long)})
     * @param strategy    the polling strategy controlling where to start reading
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether the server should automatically commit the consumer offset
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     *         containing the retrieved messages and their metadata
     */
    CompletableFuture<PolledMessages> pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit);

    /**
     * Polls messages from a topic partition asynchronously using numeric identifiers.
     *
     * <p>This is a convenience overload that accepts raw numeric IDs instead of typed
     * identifier objects. See {@link #pollMessages(StreamId, TopicId, Optional, Consumer,
     * PollingStrategy, Long, boolean)} for full documentation.
     *
     * @param streamId    the numeric stream ID
     * @param topicId     the numeric topic ID
     * @param partitionId optional partition ID
     * @param consumerId  the numeric consumer ID
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether to auto-commit offsets
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     */
    default CompletableFuture<PolledMessages> pollMessages(
            Long streamId,
            Long topicId,
            Optional<Long> partitionId,
            Long consumerId,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {
        return pollMessages(
                StreamId.of(streamId),
                TopicId.of(topicId),
                partitionId,
                Consumer.of(consumerId),
                strategy,
                count,
                autoCommit);
    }

    /**
     * Polls messages asynchronously, letting the server hold the request until data is ready.
     *
     * <p>Unlike {@link #pollMessages}, which reads whatever is resident and returns, this
     * waits up to {@link DeferredPollOptions#maxWait()} for {@link DeferredPollOptions#minCount()}
     * messages, caps the encoded response at {@link DeferredPollOptions#maxBytes()}, and bounds
     * the whole exchange by {@link DeferredPollOptions#requestTimeout()}. An expired readiness
     * wait can still return partial or empty data; an expired request timeout is an error.
     *
     * <p>The server must support command 106. An older one answers
     * {@link org.apache.iggy.exception.IggyErrorCode#INVALID_COMMAND}, which also covers an
     * operator wait limit below the requested wait and a request this client encoded wrongly.
     * Never retry it and never infer the cause from the code alone.
     *
     * <p>A group poll must name its partition. Without one, the TCP client fails with
     * {@link org.apache.iggy.exception.IggyErrorCode#INVALID_COMMAND} before it sends anything.
     * {@link #readinessGroups} and {@link #fetchMessages} read the whole assignment of a group.
     *
     * @param streamId    the stream identifier (numeric or string-based)
     * @param topicId     the topic identifier (numeric or string-based)
     * @param partitionId the partition ID to poll from, which only a plain consumer can leave empty
     * @param consumer    the consumer identity, either individual or group
     * @param strategy    the polling strategy controlling where to start reading
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether the server should automatically commit the consumer offset
     * @param options     the readiness, byte and request-time limits
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    default CompletableFuture<PolledMessages> pollMessagesDeferred(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        return CompletableFuture.failedFuture(new IggyOperationNotSupportedException(
                "pollMessagesDeferred", getClass().getSimpleName()));
    }

    /**
     * Polls messages asynchronously with a deferred wait using numeric identifiers.
     *
     * <p>See {@link #pollMessagesDeferred(StreamId, TopicId, Optional, Consumer, PollingStrategy,
     * Long, boolean, DeferredPollOptions)} for full documentation.
     *
     * @param streamId    the numeric stream ID
     * @param topicId     the numeric topic ID
     * @param partitionId optional partition ID
     * @param consumerId  the numeric consumer ID
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether to auto-commit offsets
     * @param options     the readiness, byte and request-time limits
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    default CompletableFuture<PolledMessages> pollMessagesDeferred(
            Long streamId,
            Long topicId,
            Optional<Long> partitionId,
            Long consumerId,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        return pollMessagesDeferred(
                StreamId.of(streamId),
                TopicId.of(topicId),
                partitionId,
                Consumer.of(consumerId),
                strategy,
                count,
                autoCommit,
                options);
    }

    /**
     * Groups {@code partitions} by their primary, with at most 1000 partitions in a group, so that a
     * caller can keep one {@link #fetchMessages} in flight per group.
     *
     * <p>A partition without a known route costs a route lookup. The groups show the routes at the
     * time of the call. If a fetch reports NotPrimary for a partition, its route is marked stale;
     * call this method again to regroup the partitions before the next fetch.
     *
     * @param streamId   the stream identifier (numeric or string-based)
     * @param topicId    the topic identifier (numeric or string-based)
     * @param partitions partition IDs from 0 to 4294967295
     * @return a {@link CompletableFuture} that completes with the groups, each in the order of
     *         {@code partitions}
     */
    default CompletableFuture<List<List<Long>>> readinessGroups(
            StreamId streamId, TopicId topicId, List<Long> partitions) {
        return CompletableFuture.failedFuture(new IggyOperationNotSupportedException(
                "readinessGroups", getClass().getSimpleName()));
    }

    /**
     * Fetches messages asynchronously from several partitions of one primary in one request,
     * letting the server hold the request until the partitions hold data.
     *
     * <p>The TCP client sends one fetch to the primary of the first partition of {@code targets},
     * so take the partitions of one group of {@link #readinessGroups}. The server waits up to
     * {@link DeferredPollOptions#maxWait()} until the targets hold
     * {@link DeferredPollOptions#minCount()} messages. Then it reads the targets in the order of
     * {@code targets}, and each one gets up to {@link DeferredPollOptions#maxBytes()} of records
     * or no section. All sections together hold at most {@code count} messages and
     * {@code maxBytes} of records. To give every partition its turn, put the partitions of the
     * served sections last in the next fetch.
     *
     * <p>A section with the status {@link FetchedMessages#STATUS_OK} names the
     * {@link FetchedMessages#nextOffset()} where the next fetch of its partition starts, also
     * after a Next start point. Send it as {@link PollingStrategy#offset} with the
     * {@link FetchedMessages#context()} of the section. A section with another status holds the
     * error code of its partition and no position, and a partition without a section was not
     * read, so both keep the start point that they had. A NOT_PRIMARY section or refusal makes
     * the next {@link #readinessGroups} look up the routes again. The records stay undecoded
     * until {@link FetchedMessages#decode()}, so the next fetch can go out first.
     *
     * <p>A fetch with {@code autoCommit} or a Next start point keeps its place among the offset
     * stores and the other progress polls of its partitions, and the client does not send it
     * again after a lost reply. A retained mutation blocking a target fails the fetch with
     * {@link org.apache.iggy.exception.IggyErrorCode#PARTITION_LANE_BLOCKED}. Its
     * {@link org.apache.iggy.exception.IggyOperationException} names the blocked partition and
     * the operation to resolve or abandon. The client never cancels a fetch with {@code autoCommit}: a
     * close waits for its reply. Cancelling its future can discard committed messages and does
     * not cancel the server work.
     *
     * <p>An unknown committing outcome retains a local recovery id and blocks progress on the
     * affected partitions across reconnects. A whole unknown outcome fails with
     * {@link org.apache.iggy.exception.IggyErrorCode#FETCH_PROGRESS_UNCERTAIN}; partial replies
     * keep healthy sections and expose {@link FetchedMessages#recoveryError()} on uncertain ones.
     * GetOffset remains available for inspection, but cannot prove that the old fetch ended.
     * Resolve never replays a fetch. Explicit abandon accepts the uncertainty and releases its
     * local blocks without cancelling or fencing the old server work.
     *
     * <p>Unlike a deferred poll, a fetch needs a {@link DeferredPollOptions#requestTimeout()} of at
     * least {@code maxWait} plus {@link DeferredPollOptions#REPLY_MARGIN}, also for a zero
     * {@code maxWait}. A fresh call also needs more than the reply margin to leave time for routing.
     * The TCP client lowers {@code maxBytes} until the whole reply fits in the
     * largest frame that its connections read. The server must support command 109. An older one
     * answers {@link org.apache.iggy.exception.IggyErrorCode#INVALID_COMMAND}.
     *
     * @param streamId   the stream identifier (numeric or string-based)
     * @param topicId    the topic identifier (numeric or string-based)
     * @param targets    1 to 1000 partition IDs from 0 to 4294967295, each with an Offset or Next
     *                   strategy, in the order that the server reads them
     * @param consumer   the consumer identity, either individual or group
     * @param count      the maximum number of messages of all sections together
     * @param autoCommit whether the server commits the consumer offset of each read partition
     * @param options    the wait, message count, byte and request time limits
     * @return a {@link CompletableFuture} that completes with the sections of the reply, in its
     *         order
     */
    default CompletableFuture<List<FetchedMessages>> fetchMessages(
            StreamId streamId,
            TopicId topicId,
            Map<Long, PollingStrategy> targets,
            Consumer consumer,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        return CompletableFuture.failedFuture(new IggyOperationNotSupportedException(
                "fetchMessages", getClass().getSimpleName()));
    }

    /**
     * Sends messages to a topic asynchronously.
     *
     * <p>Messages are routed to partitions according to the specified {@link Partitioning}
     * strategy:
     * <ul>
     *   <li>{@link Partitioning#balanced()} — round-robin distribution across partitions</li>
     *   <li>{@link Partitioning#partitionId(Long)} — send to a specific partition</li>
     *   <li>{@link Partitioning#messagesKey(String)} — hash-based routing that guarantees
     *       messages with the same key always go to the same partition, preserving order</li>
     * </ul>
     *
     * <p>This TCP client resolves balanced and key-based partitioning to a concrete
     * partition before sending (round-robin cursor and
     * {@code xxh32(key) % partitionCount} respectively).
     *
     * <p>Messages are batched into a single network request for efficiency. For high
     * throughput, accumulate messages and send them in larger batches rather than one
     * at a time.
     *
     * @param streamId     the stream identifier (numeric or string-based)
     * @param topicId      the topic identifier (numeric or string-based)
     * @param partitioning the partitioning strategy for routing messages
     * @param messages     the list of messages to send
     * @return a {@link CompletableFuture} that completes with the {@link SendMessagesResponse}
     *         confirming the partition and base offset of the committed batch
     * @throws org.apache.iggy.exception.IggyException if the stream or topic does not exist
     */
    CompletableFuture<SendMessagesResponse> sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);

    /**
     * Sends messages to a topic asynchronously using numeric identifiers.
     *
     * <p>This is a convenience overload that accepts raw numeric IDs. See
     * {@link #sendMessages(StreamId, TopicId, Partitioning, List)} for full documentation.
     *
     * @param streamId     the numeric stream ID
     * @param topicId      the numeric topic ID
     * @param partitioning the partitioning strategy
     * @param messages     the list of messages to send
     * @return a {@link CompletableFuture} that completes with the {@link SendMessagesResponse}
     */
    default CompletableFuture<SendMessagesResponse> sendMessages(
            Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        return sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }
}
