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
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.hash.XxHash32;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PartitioningKind;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static org.apache.iggy.serde.BytesSerializer.encodeMessagesBatchInto;
import static org.apache.iggy.serde.BytesSerializer.toBytes;

/**
 * Async TCP implementation of MessagesClient using Netty for non-blocking I/O.
 */
public class MessagesTcpClient implements MessagesClient {

    private static final Logger log = LoggerFactory.getLogger(MessagesTcpClient.class);

    /**
     * A generation-fenced group poll is answered with an empty poll body
     * carrying this sentinel partition id, telling the client to re-sync its
     * assignment and retry; mirrors RESYNC_REQUIRED_PARTITION_SENTINEL in
     * core/common/src/lib.rs.
     */
    private static final long RESYNC_REQUIRED_PARTITION_SENTINEL = 0xFFFF_FFFFL;

    private static final int GROUP_POLL_MAX_ATTEMPTS = 2;
    private static final int PARTITION_NOT_OWNED_ERROR_CODE = 5009;

    /**
     * Staleness budget for group assignments. They re-fetch on the next use once
     * this old, so a partition-count change or rebalance is picked up without
     * a background refresher thread.
     */
    private static final Duration ROUTING_CACHE_REFRESH = Duration.ofSeconds(5);

    private final ClientRoutingState routingState;
    private final ConsumerGroupsTcpClient consumerGroupsClient;
    private final PollRouter pollRouter;

    /**
     * Creates a low-level client on the supplied connection without primary routing.
     * Use {@code Iggy.tcpClientBuilder()} for clustered polling so the
     * coordinator retains group membership while data connections reach primaries.
     */
    public MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, new ClientRoutingState());
    }

    MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier, ClientRoutingState routingState) {
        this(connectionSupplier, routingState, null);
    }

    MessagesTcpClient(
            Supplier<AsyncTcpConnection> connectionSupplier, ClientRoutingState routingState, PollRouter pollRouter) {
        this.routingState = routingState;
        this.consumerGroupsClient = new ConsumerGroupsTcpClient(connectionSupplier);
        this.pollRouter = pollRouter == null ? new PollRouter(connectionSupplier, null) : pollRouter;
    }

    @Override
    public CompletableFuture<PolledMessages> pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {
        long deadline = pollRouter.operationDeadline();
        if (consumer.kind() == Consumer.Kind.ConsumerGroup && partitionId.isEmpty()) {
            // The VSR broker fences group polls against unowned partitions
            // instead of picking one, so the partition is selected here from
            // the member's synced assignment, matching the Rust SDK.
            CallCancellation cancellation = new CallCancellation();
            CompletableFuture<PolledMessages> result = pollGroupMessages(
                    streamId,
                    topicId,
                    consumer,
                    strategy,
                    count,
                    autoCommit,
                    GROUP_POLL_MAX_ATTEMPTS,
                    cancellation,
                    deadline);
            result.whenComplete((response, error) -> {
                if (result.isCancelled()) {
                    cancellation.cancel();
                }
            });
            return result;
        }
        return pollPartition(streamId, topicId, partitionId, consumer, strategy, count, autoCommit, deadline);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private CompletableFuture<PolledMessages> pollPartition(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            long deadline) {

        var payload = Unpooled.buffer();

        writeAndRelease(payload, toBytes(consumer));
        writeAndRelease(payload, toBytes(streamId));
        writeAndRelease(payload, toBytes(topicId));
        writeAndRelease(payload, toBytes(partitionId));
        writeAndRelease(payload, toBytes(strategy));

        payload.writeIntLE(count.intValue());

        payload.writeByte(autoCommit ? 1 : 0);

        CompletableFuture<ByteBuf> sent = pollRouter.partitionRequest(
                org.apache.iggy.serde.CommandCode.Messages.POLL.getValue(), payload, strategy.context(), deadline);
        CompletableFuture<PolledMessages> result = AsyncTcpConnection.mapResponse(sent, response -> {
            try {
                return BytesDeserializer.readPolledMessages(response);
            } finally {
                response.release();
            }
        });
        return result;
    }

    CompletableFuture<ByteBuf> sendPoll(ByteBuf payload, Optional<PartitionContext> context) {
        return pollRouter.poll(payload, context);
    }

    @Override
    public CompletableFuture<SendMessagesResponse> sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        long deadline = pollRouter.operationDeadline();
        byte[] batch = freezeMessages(messages);
        Partitioning frozenPartitioning =
                new Partitioning(partitioning.kind(), partitioning.value().clone());
        if (partitioning.kind() == PartitioningKind.PartitionId) {
            return sendToPartition(streamId, topicId, frozenPartitioning, batch, deadline);
        }
        CallCancellation cancellation = new CallCancellation();
        CompletableFuture<SendMessagesResponse> result =
                sendResolved(streamId, topicId, frozenPartitioning, batch, deadline, false, cancellation);
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                cancellation.cancel();
            }
        });
        return result;
    }

    private CompletableFuture<SendMessagesResponse> sendResolved(
            StreamId streamId,
            TopicId topicId,
            Partitioning partitioning,
            byte[] batch,
            long deadline,
            boolean refreshed,
            CallCancellation cancellation) {
        return resolvePartitioning(streamId, topicId, partitioning, deadline, cancellation)
                .thenCompose(resolved -> {
                    if (cancellation.isCancelled()) {
                        return CompletableFuture.failedFuture(new CancellationException());
                    }
                    var sent = sendToPartition(streamId, topicId, resolved, batch, deadline);
                    cancellation.track(sent);
                    return sent;
                })
                .exceptionallyCompose(error -> {
                    Throwable cause = unwrapCompletion(error);
                    if (!refreshed
                            && !cancellation.isCancelled()
                            && cause instanceof IggyServerException server
                            && server.getRawErrorCode() == IggyErrorCode.PARTITION_NOT_FOUND.getCode()) {
                        pollRouter.invalidateTopic(streamId, topicId);
                        return sendResolved(streamId, topicId, partitioning, batch, deadline, true, cancellation);
                    }
                    return CompletableFuture.failedFuture(error);
                });
    }

    private CompletableFuture<SendMessagesResponse> sendToPartition(
            StreamId streamId, TopicId topicId, Partitioning partitioning, byte[] batch, long deadline) {

        var metadataLength = streamId.getSize() + topicId.getSize() + partitioning.getSize() + 4;
        var payload = Unpooled.buffer(metadataLength + batch.length);

        CompletableFuture<ByteBuf> sent;
        try {
            payload.writeIntLE(metadataLength);
            writeAndRelease(payload, toBytes(streamId));
            writeAndRelease(payload, toBytes(topicId));
            writeAndRelease(payload, toBytes(partitioning));
            payload.writeBytes(batch);
            var contextPayload = Unpooled.buffer();
            writeAndRelease(contextPayload, toBytes(streamId));
            writeAndRelease(contextPayload, toBytes(topicId));
            contextPayload.writeBytes(partitioning.value());
            sent = pollRouter.sendMessages(payload, contextPayload, deadline);
        } catch (RuntimeException | Error error) {
            payload.release();
            throw error;
        }
        return AsyncTcpConnection.mapResponse(sent, response -> {
            try {
                return BytesDeserializer.readSendMessagesResponse(response);
            } catch (RuntimeException e) {
                // The batch is already committed server-side; failing here would
                // trigger a spurious resend, so a malformed confirmation degrades
                // to an empty one.
                log.warn("Discarding malformed send confirmation: {}", e.getMessage());
                return SendMessagesResponse.empty();
            } finally {
                response.release();
            }
        });
    }

    private static byte[] freezeMessages(List<Message> messages) {
        var batch = Unpooled.buffer();
        try {
            batch.writeIntLE(messages.size());
            encodeMessagesBatchInto(batch, messages);
            byte[] frozen = new byte[batch.readableBytes()];
            batch.readBytes(frozen);
            return frozen;
        } finally {
            batch.release();
        }
    }

    private static void writeAndRelease(ByteBuf destination, ByteBuf source) {
        try {
            destination.writeBytes(source);
        } finally {
            source.release();
        }
    }

    /**
     * One group-poll attempt: sync the assignment when missing or stale, pick
     * the next assigned partition round-robin, poll it explicitly, and on a
     * generation fence (the re-sync sentinel), drop the cached assignment and retry. The attempt budget allows one
     * re-sync after the coordinator rejects a stale assignment, then one
     * retry; an exhausted budget is an empty poll, not an error. A
     * partition-not-owned error is returned without retry.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private CompletableFuture<PolledMessages> pollGroupMessages(
            StreamId streamId,
            TopicId topicId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            int attemptsLeft,
            CallCancellation cancellation,
            long deadline) {
        if (cancellation.isCancelled()) {
            return CompletableFuture.failedFuture(new CancellationException());
        }
        if (attemptsLeft == 0) {
            return CompletableFuture.completedFuture(emptyPolledMessages());
        }
        var groupKey = ClientRoutingState.groupKey(streamId, topicId, consumer.id());
        return ensureFreshAssignment(streamId, topicId, consumer, groupKey, deadline, cancellation)
                .thenCompose(ignored -> {
                    if (cancellation.isCancelled()) {
                        return CompletableFuture.failedFuture(new CancellationException());
                    }
                    var partitionId = routingState.nextGroupPartition(groupKey);
                    if (partitionId.isEmpty()) {
                        if (routingState.assignment(groupKey).isPresent()) {
                            // a member owning no partitions polls nothing
                            return CompletableFuture.completedFuture(emptyPolledMessages());
                        }
                        return CompletableFuture.failedFuture(new IggyResourceNotFoundException(
                                IggyErrorCode.CONSUMER_GROUP_MEMBER_NOT_FOUND,
                                IggyErrorCode.CONSUMER_GROUP_MEMBER_NOT_FOUND.getCode(),
                                "Cannot poll consumer group " + consumer.id() + " for topic " + topicId + " in stream "
                                        + streamId + ": this client is not a member, join the group first",
                                Optional.empty(),
                                Optional.empty()));
                    }
                    CompletableFuture<PolledMessages> polledPartition = pollPartition(
                            streamId,
                            topicId,
                            Optional.of(partitionId.getAsLong()),
                            consumer,
                            strategy,
                            count,
                            autoCommit,
                            deadline);
                    cancellation.track(polledPartition);
                    return polledPartition
                            .thenCompose(polled -> {
                                if (polled.messages().isEmpty()
                                        && polled.partitionId() == RESYNC_REQUIRED_PARTITION_SENTINEL) {
                                    routingState.invalidateAssignment(groupKey);
                                    return pollGroupMessages(
                                            streamId,
                                            topicId,
                                            consumer,
                                            strategy,
                                            count,
                                            autoCommit,
                                            attemptsLeft - 1,
                                            cancellation,
                                            deadline);
                                }
                                return CompletableFuture.completedFuture(polled);
                            })
                            .exceptionallyCompose(error -> {
                                if (isPartitionNotOwned(error)) {
                                    routingState.invalidateAssignment(groupKey);
                                }
                                return CompletableFuture.failedFuture(error);
                            });
                });
    }

    private CompletableFuture<Void> ensureFreshAssignment(
            StreamId streamId,
            TopicId topicId,
            Consumer consumer,
            ClientRoutingState.GroupKey groupKey,
            long deadline,
            CallCancellation cancellation) {
        var cached = routingState.assignment(groupKey);
        // As in the Rust SDK, an empty assignment is synced again on every poll:
        // a member gets its partitions only as each one installs the new owner.
        if (cached.isPresent()
                && !cached.get().partitions().isEmpty()
                && System.nanoTime() - cached.get().syncedAtNanos() < ROUTING_CACHE_REFRESH.toNanos()) {
            return CompletableFuture.completedFuture(null);
        }
        var synced = consumerGroupsClient.syncConsumerGroup(streamId, topicId, consumer.id(), deadline);
        cancellation.track(synced);
        return synced.thenAccept(assignment -> {
            if (assignment.isEmpty()) {
                // an empty sync reply means "not a member"
                routingState.invalidateAssignment(groupKey);
                return;
            }
            routingState.setAssignment(
                    groupKey, assignment.get().generation(), assignment.get().partitions(), System.nanoTime());
        });
    }

    private static boolean isPartitionNotOwned(Throwable error) {
        return unwrapCompletion(error) instanceof IggyServerException serverError
                && serverError.getRawErrorCode() == PARTITION_NOT_OWNED_ERROR_CODE;
    }

    private static Throwable unwrapCompletion(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    private static PolledMessages emptyPolledMessages() {
        return new PolledMessages(0L, BigInteger.ZERO, 0L, List.of(), PartitionContext.EMPTY);
    }

    private CompletableFuture<Partitioning> resolvePartitioning(
            StreamId streamId,
            TopicId topicId,
            Partitioning partitioning,
            long deadline,
            CallCancellation cancellation) {
        var count = pollRouter.partitionCount(streamId, topicId, deadline);
        cancellation.track(count);
        return count.thenApply(partitionsCount -> switch (partitioning.kind()) {
            case Balanced ->
                Partitioning.partitionId(routingState.nextBalancedPartition(
                        ClientRoutingState.topicKey(streamId, topicId), partitionsCount));
            case MessagesKey -> Partitioning.partitionId(XxHash32.hashUnsigned(partitioning.value()) % partitionsCount);
            case PartitionId -> partitioning;
        });
    }

    private static final class CallCancellation {
        private boolean cancelled;
        private CompletableFuture<?> active;

        synchronized boolean isCancelled() {
            return cancelled;
        }

        synchronized void track(CompletableFuture<?> poll) {
            active = poll;
            if (cancelled) {
                poll.cancel(false);
            }
        }

        synchronized void cancel() {
            cancelled = true;
            if (active != null) {
                active.cancel(false);
            }
        }
    }
}
