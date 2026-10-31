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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.message.CommitErrorScope;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One fetch (code 109) over the partitions of one readiness group, sent to the primary of its first partition. Each
 * target carries its own fence, so the request header carries none. A target whose strategy has no context takes the
 * context of its current route, as a deferred poll does. NOT_PRIMARY of the request does not say which partition moved,
 * so it marks the routes of all targets moved, and a NOT_PRIMARY section marks the route of its partition moved. The
 * next readiness grouping then looks them up again.
 */
final class FetchMessages {
    static final int MAX_PARTITIONS_PER_REQUEST = 1000;
    static final long MAX_PARTITION_ID = 0xFFFF_FFFFL;
    // The wait, the request timeout, min_count, count, max_bytes, auto_commit and the target count, which follow the
    // consumer, stream and topic.
    static final int LIMITS_BYTES = 2 * Long.BYTES + 4 * Integer.BYTES + Byte.BYTES;
    // A partition id, the kind and value of a polling strategy, max_bytes, then the incarnation and owner generation of
    // the fence of the partition.
    static final int TARGET_BYTES = 2 * Integer.BYTES + Byte.BYTES + 3 * Long.BYTES;
    // From the wait, which opens the limits.
    static final int MIN_COUNT_OFFSET = 2 * Long.BYTES;
    static final int COUNT_OFFSET = MIN_COUNT_OFFSET + Integer.BYTES;
    static final int MAX_BYTES_OFFSET = COUNT_OFFSET + Integer.BYTES;
    static final int AUTO_COMMIT_OFFSET = MAX_BYTES_OFFSET + Integer.BYTES;
    static final int TARGETS_COUNT_OFFSET = AUTO_COMMIT_OFFSET + Byte.BYTES;
    // From the start of a target.
    static final int STRATEGY_KIND_OFFSET = Integer.BYTES;
    static final int TARGET_MAX_BYTES_OFFSET = STRATEGY_KIND_OFFSET + Byte.BYTES + Long.BYTES;
    static final int INCARNATION_OFFSET = TARGET_MAX_BYTES_OFFSET + Integer.BYTES;

    private FetchMessages() {}

    /**
     * Fetches {@code targets}, in their order, and returns the sections of the reply in its order. Cancelling the
     * returned future cancels the route lookups or the fetch.
     *
     * @param consumer the encoded consumer
     * @param topic    the encoded stream and topic identifiers
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    static CompletableFuture<List<FetchedMessages>> start(
            PollRouter router,
            byte[] consumer,
            byte[] topic,
            Map<Long, PollingStrategy> targets,
            long count,
            boolean autoCommit,
            DeferredPollOptions options,
            long deadline,
            AsyncTcpConnection.TransientFailoverState state) {
        List<Long> partitions = List.copyOf(targets.keySet());
        ByteBuf payload = payload(consumer, topic, targets, count, autoCommit, options);
        CompletableFuture<ByteBuf> reply = send(router, payload, topic, partitions, deadline, state);
        return AsyncTcpConnection.mapResponse(reply, body -> {
            try {
                List<FetchedMessages> sections = attachRecovery(BytesDeserializer.readFetchedMessages(body), state);
                markMoved(router, topic, sections);
                return sections;
            } finally {
                body.release();
            }
        });
    }

    /**
     * Settles an attempt of a poll or fetch that stores the offsets that it reads. A refusal that proves nothing stored
     * frees its partitions, any other failure retains them blocked until an abandon, and a fetch reply retains the
     * targets of its uncertain sections.
     */
    static RuntimeException completeAttempt(
            VsrExchange exchange, ByteBuf response, Throwable error, boolean failedByLink) {
        AsyncTcpConnection.TransientFailoverState state = exchange.failoverState();
        if (state.ticket() == null || !state.ticket().commitsOnRead()) {
            return null;
        }
        IggyServerException refusal = AsyncTcpConnection.findServerError(error);
        if (error != null) {
            if (!failedByLink
                    && refusal != null
                    && CommitErrorScope.REQUEST.provesNothingStored(refusal.getRawErrorCode())) {
                state.ticket().settleSent(false);
                return null;
            }
            return state.retainTicket();
        }
        if (exchange.commandCode() != CommandCode.Messages.FETCH_MESSAGES.getValue()) {
            state.ticket().settleFetch(List.of());
            return null;
        }
        try {
            classifyReply(state, response);
            return null;
        } catch (RuntimeException malformed) {
            return state.retainTicket();
        }
    }

    static void classifyReply(AsyncTcpConnection.TransientFailoverState state, ByteBuf body) {
        List<FetchedMessages> sections = BytesDeserializer.readFetchedMessages(body.duplicate());
        var requested = new HashSet<Long>();
        state.lanes().forEach(lane -> requested.add(lane.partition()));
        List<Long> uncertain = new ArrayList<>();
        for (FetchedMessages section : sections) {
            if (!requested.remove(section.partitionId())) {
                throw new IggyInvalidArgumentException("Fetch reply contains an unexpected or duplicate partition");
            }
            if (section.status() != FetchedMessages.STATUS_OK
                    && !CommitErrorScope.SECTION.provesNothingStored(section.status())) {
                uncertain.add(section.partitionId());
            }
        }
        state.ticket().settleFetch(uncertain);
    }

    private static List<FetchedMessages> attachRecovery(
            List<FetchedMessages> sections, AsyncTcpConnection.TransientFailoverState state) {
        if (state.ticket() == null || !state.ticket().commitsOnRead()) {
            return sections;
        }
        return sections.stream()
                .map(section -> section.status() != FetchedMessages.STATUS_OK
                                && !CommitErrorScope.SECTION.provesNothingStored(section.status())
                        ? new FetchedMessages(
                                section.partitionId(),
                                section.status(),
                                section.currentOffset(),
                                section.count(),
                                section.context(),
                                section.nextOffset(),
                                section.records(),
                                OptionalLong.of(state.ticket().operationId()))
                        : section)
                .toList();
    }

    /**
     * Refuses what the server refuses in a fetch request that the typed call encodes, before any lookup. The partition
     * ids of {@code targets} are checked by the caller.
     */
    static void validate(Map<Long, PollingStrategy> targets, long count, DeferredPollOptions options) {
        if (targets.isEmpty() || targets.size() > FetchMessages.MAX_PARTITIONS_PER_REQUEST) {
            throw new IggyInvalidArgumentException("A fetch needs 1 to " + FetchMessages.MAX_PARTITIONS_PER_REQUEST
                    + " partitions, not " + targets.size());
        }
        for (PollingStrategy strategy : targets.values()) {
            if (strategy.kind() != PollingKind.Offset && strategy.kind() != PollingKind.Next) {
                throw new IggyInvalidArgumentException(
                        "A fetch starts at an offset or at the next offset, not at " + strategy.kind());
            }
        }
        options.validate(count);
        if (!canStart(options)) {
            throw new IggyInvalidArgumentException("Fetch requestTimeout " + options.requestTimeout()
                    + " must exceed the reply margin " + DeferredPollOptions.REPLY_MARGIN + " and cover maxWait "
                    + options.maxWait() + " plus that margin");
        }
    }

    /** A fresh fetch needs time for routing before the mandatory server reply margin. */
    static boolean canStart(DeferredPollOptions options) {
        return options.requestTimeout().compareTo(DeferredPollOptions.REPLY_MARGIN) > 0 && keepsReplyMargin(options);
    }

    /**
     * Whether the request timeout leaves the reply margin after the wait. Unlike a deferred poll, a fetch needs the
     * margin also for a zero wait, because the server ends its reads before the request timeout.
     */
    static boolean keepsReplyMargin(DeferredPollOptions options) {
        return options.requestTimeout().compareTo(options.maxWait().plus(DeferredPollOptions.REPLY_MARGIN)) >= 0;
    }

    /**
     * Sends one fetch over {@code partitions} to the primary of the first one. A NOT_PRIMARY failure marks the routes
     * of all of them moved before the returned future fails.
     *
     * @param topic the encoded stream and topic identifiers
     */
    static CompletableFuture<ByteBuf> send(
            PollRouter router,
            ByteBuf payload,
            byte[] topic,
            List<Long> partitions,
            long deadline,
            AsyncTcpConnection.TransientFailoverState state) {
        int first = PollRouter.waitParameters(payload, payload.readerIndex()) + LIMITS_BYTES;
        List<Long> unfenced = unfencedPartitions(payload, first);
        if (unfenced.isEmpty()) {
            return sendResolved(router, payload, topic, partitions, deadline, state);
        }
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        CompletableFuture<Map<Long, PartitionContext>> routed = router.routeContexts(topic, unfenced, deadline, state);
        AtomicReference<CompletableFuture<?>> step = new AtomicReference<>(routed);
        result.whenComplete((body, error) -> {
            if (result.isCancelled()) {
                step.get().cancel(false);
            }
        });
        routed.whenComplete((contexts, error) -> {
            if (error != null || result.isDone()) {
                payload.release();
                if (error != null) {
                    result.completeExceptionally(PollRouter.unwrap(error));
                }
                return;
            }
            applyContexts(payload, first, contexts);
            CompletableFuture<ByteBuf> reply = sendResolved(router, payload, topic, partitions, deadline, state);
            step.set(reply);
            if (result.isDone()) {
                reply.cancel(false);
            }
            reply.whenComplete((body, failed) -> {
                if (failed != null) {
                    result.completeExceptionally(PollRouter.unwrap(failed));
                } else if (!result.complete(body)) {
                    body.release();
                }
            });
        });
        return result;
    }

    private static List<Long> unfencedPartitions(ByteBuf payload, int first) {
        List<Long> unfenced = new ArrayList<>();
        for (int target = first; target < payload.writerIndex(); target += TARGET_BYTES) {
            if (payload.getLongLE(target + INCARNATION_OFFSET) == 0) {
                unfenced.add(payload.getUnsignedIntLE(target));
            }
        }
        return unfenced;
    }

    private static void applyContexts(ByteBuf payload, int first, Map<Long, PartitionContext> contexts) {
        for (int target = first; target < payload.writerIndex(); target += TARGET_BYTES) {
            if (payload.getLongLE(target + INCARNATION_OFFSET) == 0) {
                PartitionContext context = contexts.get(payload.getUnsignedIntLE(target));
                payload.setLongLE(
                        target + INCARNATION_OFFSET, context.incarnation().longValue());
                if (payload.getLongLE(target + INCARNATION_OFFSET + Long.BYTES) == 0) {
                    payload.setLongLE(
                            target + INCARNATION_OFFSET + Long.BYTES,
                            context.ownerGeneration().longValue());
                }
            }
        }
    }

    private static CompletableFuture<ByteBuf> sendResolved(
            PollRouter router,
            ByteBuf payload,
            byte[] topic,
            List<Long> partitions,
            long deadline,
            AsyncTcpConnection.TransientFailoverState state) {
        CompletableFuture<ByteBuf> reply = router.fetchRequest(payload, topic, partitions.get(0), deadline, state);
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        result.whenComplete((body, error) -> {
            if (result.isCancelled()) {
                reply.cancel(false);
            }
        });
        reply.whenComplete((body, error) -> {
            if (error == null) {
                if (!result.complete(body)) {
                    body.release();
                }
                return;
            }
            var refusal = AsyncTcpConnection.findServerError(error);
            if (refusal != null && refusal.getRawErrorCode() == AsyncTcpConnection.NOT_PRIMARY) {
                router.mayHaveMoved(topic, partitions);
            }
            if (refusal instanceof IggyOperationException blocked
                    && blocked.getErrorCode() == IggyErrorCode.LANE_BLOCKED
                    && blocked.getPartitionId().isPresent()) {
                result.completeExceptionally(new IggyOperationException(
                        IggyErrorCode.PARTITION_LANE_BLOCKED,
                        blocked.getOperationId(),
                        blocked.getPartitionId().getAsLong()));
                return;
            }
            result.completeExceptionally(error);
        });
        return result;
    }

    /** Marks the routes of the partitions with a NOT_PRIMARY section moved. */
    static void markMoved(PollRouter router, byte[] topic, List<FetchedMessages> sections) {
        List<Long> moved = sections.stream()
                .filter(section -> section.status() == AsyncTcpConnection.NOT_PRIMARY)
                .map(FetchedMessages::partitionId)
                .toList();
        if (!moved.isEmpty()) {
            router.mayHaveMoved(topic, moved);
        }
    }

    /**
     * The progress lanes of a fetch whose body is {@code payload}, or none. A fetch that commits or that starts a
     * partition at its next offset takes the progress lane of every target, so it keeps its place among the stores and
     * polls of those partitions. Any other fetch only reads, and takes no lane.
     */
    static List<ConsensusSession.LaneKey> progressLanes(ByteBuf payload) {
        int limits = PollRouter.waitParameters(payload, payload.readerIndex());
        long targetsCount = payload.getUnsignedIntLE(limits + TARGETS_COUNT_OFFSET);
        int first = limits + LIMITS_BYTES;
        boolean progress = commits(payload);
        List<ConsensusSession.LaneKey> lanes = new ArrayList<>((int) targetsCount);
        for (int target = first; target < first + targetsCount * TARGET_BYTES; target += TARGET_BYTES) {
            progress |= payload.getUnsignedByte(target + STRATEGY_KIND_OFFSET) == PollingKind.Next.asCode();
            lanes.add(new ConsensusSession.LaneKey(
                    payload.getLongLE(target + INCARNATION_OFFSET), payload.getUnsignedIntLE(target), true));
        }
        return progress ? List.copyOf(lanes) : List.of();
    }

    /** Whether the fetch whose body is {@code payload} commits the consumer offset of each partition that it reads. */
    static boolean commits(ByteBuf payload) {
        return payload.getUnsignedByte(PollRouter.waitParameters(payload, payload.readerIndex()) + AUTO_COMMIT_OFFSET)
                != 0;
    }

    /**
     * Lowers the byte limits of the fetch whose limits start at {@code limits} in {@code frame} until its whole reply
     * fits in {@code maxReplyBody}, the largest reply body that the link reads. The link drops a larger reply, after
     * the server may have committed what it holds.
     *
     * @throws IggyInvalidArgumentException if the section table leaves less than one record of the largest size
     */
    static void fitReply(ByteBuf frame, int limits, long maxReplyBody) {
        long targetsCount = frame.getUnsignedIntLE(limits + TARGETS_COUNT_OFFSET);
        long records = maxReplyBody
                - BytesDeserializer.FETCH_SECTIONS_COUNT_BYTES
                - targetsCount * BytesDeserializer.FETCH_SECTION_BYTES;
        if (records < DeferredPollOptions.MAX_RECORD_SIZE) {
            throw new IggyInvalidArgumentException("A fetch of " + targetsCount
                    + " partitions can get a first message that needs a reply larger than " + maxReplyBody
                    + " bytes, the most that the link reads");
        }
        long maxBytes = Math.min(frame.getUnsignedIntLE(limits + MAX_BYTES_OFFSET), records);
        frame.setIntLE(limits + MAX_BYTES_OFFSET, (int) maxBytes);
        int first = limits + LIMITS_BYTES;
        for (int target = first; target < first + targetsCount * TARGET_BYTES; target += TARGET_BYTES) {
            if (frame.getUnsignedIntLE(target + TARGET_MAX_BYTES_OFFSET) > maxBytes) {
                frame.setIntLE(target + TARGET_MAX_BYTES_OFFSET, (int) maxBytes);
            }
        }
    }

    /**
     * The largest reply body that the fetch in {@code frame} can get: the section count, one table entry per target and
     * the byte limit of the request, or one record of the largest size, which the server returns alone above it.
     */
    static long replyLimit(ByteBuf frame) {
        int limits = PollRouter.waitParameters(frame, frame.readerIndex() + VsrHeaders.HEADER_SIZE);
        return BytesDeserializer.FETCH_SECTIONS_COUNT_BYTES
                + frame.getUnsignedIntLE(limits + TARGETS_COUNT_OFFSET) * BytesDeserializer.FETCH_SECTION_BYTES
                + Math.max(frame.getUnsignedIntLE(limits + MAX_BYTES_OFFSET), DeferredPollOptions.MAX_RECORD_SIZE);
    }

    /** The largest reply body of the deferred poll in {@code frame}: its byte limit, or one record of the largest size. */
    static long pollReplyLimit(ByteBuf frame) {
        return Math.max(PollRouter.deferredMaxBytes(frame), DeferredPollOptions.MAX_RECORD_POLL_REPLY_SIZE);
    }

    static void configureResponse(VsrResponseHandler.PendingRequest pending, ByteBuf frame) {
        pending.limitResponseBody(replyLimit(frame));
    }

    static ByteBuf payload(
            byte[] consumer,
            byte[] topic,
            Map<Long, PollingStrategy> targets,
            long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        ByteBuf payload =
                Unpooled.buffer(consumer.length + topic.length + LIMITS_BYTES + targets.size() * TARGET_BYTES);
        payload.writeBytes(consumer)
                .writeBytes(topic)
                .writeLongLE(options.maxWaitMicros())
                .writeLongLE(options.requestTimeoutMicros())
                .writeIntLE((int) options.minCount())
                .writeIntLE((int) count)
                .writeIntLE((int) options.maxBytes())
                .writeByte(autoCommit ? 1 : 0)
                .writeIntLE(targets.size());
        targets.forEach((partition, strategy) -> {
            PartitionContext fence = strategy.context().orElse(PartitionContext.EMPTY);
            ByteBuf encodedStrategy = BytesSerializer.toBytes(strategy);
            try {
                payload.writeIntLE((int) (long) partition)
                        .writeBytes(encodedStrategy)
                        .writeIntLE((int) options.maxBytes())
                        .writeLongLE(fence.incarnation().longValue())
                        .writeLongLE(fence.ownerGeneration().longValue());
            } finally {
                encodedStrategy.release();
            }
        });
        return payload;
    }
}
