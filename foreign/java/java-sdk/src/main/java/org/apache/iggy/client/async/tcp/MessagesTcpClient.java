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
import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.TopicsClient;
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
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.topic.TopicDetails;
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
     * Staleness budget for the client-side routing caches: group assignments
     * and topic partition counts. Both re-fetch lazily on the next use once
     * this old, so a partition-count change or rebalance is picked up without
     * a background refresher thread.
     */
    private static final Duration ROUTING_CACHE_REFRESH = Duration.ofSeconds(5);

    private final Supplier<AsyncTcpConnection> connectionSupplier;
    private final ClientRoutingState routingState;
    private final TopicsClient topicsClient;
    private final ConsumerGroupsClient consumerGroupsClient;
    private final PollRouter pollRouter;
    private final Supplier<CompletableFuture<Boolean>> clustered;

    public MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, new ClientRoutingState());
    }

    MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier, ClientRoutingState routingState) {
        this(connectionSupplier, routingState, null, () -> CompletableFuture.completedFuture(false));
    }

    MessagesTcpClient(
            Supplier<AsyncTcpConnection> connectionSupplier,
            ClientRoutingState routingState,
            PollRouter pollRouter,
            Supplier<CompletableFuture<Boolean>> clustered) {
        this.connectionSupplier = connectionSupplier;
        this.routingState = routingState;
        this.topicsClient = new TopicsTcpClient(connectionSupplier);
        this.consumerGroupsClient = new ConsumerGroupsTcpClient(connectionSupplier);
        this.pollRouter = pollRouter;
        this.clustered = clustered;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
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
        if (consumer.kind() == Consumer.Kind.ConsumerGroup && partitionId.isEmpty()) {
            // The VSR broker fences group polls against unowned partitions
            // instead of picking one, so the partition is selected here from
            // the member's synced assignment, matching the Rust SDK.
            PollCancellation cancellation = new PollCancellation();
            CompletableFuture<PolledMessages> result = pollGroupMessages(
                    streamId, topicId, consumer, strategy, count, autoCommit, GROUP_POLL_MAX_ATTEMPTS, cancellation);
            result.whenComplete((response, error) -> {
                if (result.isCancelled()) {
                    cancellation.cancel();
                }
            });
            return result;
        }
        return pollPartition(streamId, topicId, partitionId, consumer, strategy, count, autoCommit);
    }

    private CompletableFuture<PolledMessages> pollPartition(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {

        var payload = Unpooled.buffer();

        var consumerBytes = toBytes(consumer);
        payload.writeBytes(consumerBytes);

        var streamBytes = toBytes(streamId);
        payload.writeBytes(streamBytes);

        var topicBytes = toBytes(topicId);
        payload.writeBytes(topicBytes);

        payload.writeBytes(toBytes(partitionId));

        var strategyBytes = toBytes(strategy);
        payload.writeBytes(strategyBytes);

        payload.writeIntLE(count.intValue());

        payload.writeByte(autoCommit ? 1 : 0);

        // Send async request and transform response
        CompletableFuture<ByteBuf> sent = sendPoll(payload, autoCommit);
        CompletableFuture<PolledMessages> result = sent.thenApply(response -> {
            try {
                return BytesDeserializer.readPolledMessages(response);
            } finally {
                response.release();
            }
        });
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                sent.cancel(false);
            }
        });
        return result;
    }

    private CompletableFuture<ByteBuf> sendPoll(ByteBuf payload, boolean autoCommit) {
        if (!autoCommit || pollRouter == null) {
            return connection().send(CommandCode.Messages.POLL.getValue(), payload);
        }
        PollCancellation cancellation = new PollCancellation();
        CompletableFuture<ByteBuf> result = clustered
                .get()
                .handle((isClustered, error) -> {
                    if (error != null || cancellation.isCancelled()) {
                        payload.release();
                        return CompletableFuture.<ByteBuf>failedFuture(
                                error != null ? error : new CancellationException());
                    }
                    CompletableFuture<ByteBuf> sent = isClustered
                            ? pollRouter.poll(payload)
                            : connection().send(CommandCode.Messages.POLL.getValue(), payload);
                    cancellation.track(sent);
                    return sent;
                })
                .thenCompose(sent -> sent);
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                cancellation.cancel();
            }
        });
        return result;
    }

    @Override
    public CompletableFuture<SendMessagesResponse> sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        if (partitioning.kind() == PartitioningKind.PartitionId) {
            return sendToPartition(streamId, topicId, partitioning, messages);
        }
        // The VSR broker routes explicit partitions only, so balanced and
        // message-key partitioning resolve to a partition id client-side,
        // matching the Rust SDK (round-robin cursor, xxh32(key) % count).
        return resolvePartitioning(streamId, topicId, partitioning)
                .thenCompose(resolved -> sendToPartition(streamId, topicId, resolved, messages))
                .exceptionallyCompose(error -> {
                    // A resolved send refused with not-found means the cached
                    // partition count is stale (the topic shrank or was
                    // recreated); drop it so the next send re-fetches now
                    // instead of waiting out the staleness budget.
                    if (unwrapCompletion(error) instanceof IggyResourceNotFoundException) {
                        routingState.invalidatePartitionCount(ClientRoutingState.topicKey(streamId, topicId));
                    }
                    return CompletableFuture.failedFuture(error);
                });
    }

    private CompletableFuture<SendMessagesResponse> sendToPartition(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {

        var metadataLength = streamId.getSize() + topicId.getSize() + partitioning.getSize() + 4;
        // The batch is encoded straight after the metadata rather than into its
        // own buffer and copied over, which is the whole payload once per send.
        var payload = Unpooled.buffer(4 + metadataLength);

        CompletableFuture<ByteBuf> sent;
        try {
            payload.writeIntLE(metadataLength);
            writeAndRelease(payload, toBytes(streamId));
            writeAndRelease(payload, toBytes(topicId));
            writeAndRelease(payload, toBytes(partitioning));
            payload.writeIntLE(messages.size());
            encodeMessagesBatchInto(payload, messages);
            sent = connection().send(CommandCode.Messages.SEND.getValue(), payload);
        } catch (RuntimeException | Error error) {
            payload.release();
            throw error;
        }
        return sent.thenApply(response -> {
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
     * generation fence (the re-sync sentinel or a partition-not-owned error)
     * drop the cached assignment and retry. The attempt budget allows one
     * re-sync after the coordinator rejects a stale assignment, then one
     * retry; an exhausted budget is an empty poll, not an error.
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
            PollCancellation cancellation) {
        if (cancellation.isCancelled()) {
            return CompletableFuture.failedFuture(new CancellationException());
        }
        if (attemptsLeft == 0) {
            return CompletableFuture.completedFuture(emptyPolledMessages());
        }
        var groupKey = ClientRoutingState.groupKey(streamId, topicId, consumer.id());
        return ensureFreshAssignment(streamId, topicId, consumer, groupKey).thenCompose(ignored -> {
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
                    streamId, topicId, Optional.of(partitionId.getAsLong()), consumer, strategy, count, autoCommit);
            cancellation.track(polledPartition);
            return polledPartition
                    .thenCompose(polled -> {
                        if (polled.messages().isEmpty() && polled.partitionId() == RESYNC_REQUIRED_PARTITION_SENTINEL) {
                            routingState.invalidateAssignment(groupKey);
                            return pollGroupMessages(
                                    streamId,
                                    topicId,
                                    consumer,
                                    strategy,
                                    count,
                                    autoCommit,
                                    attemptsLeft - 1,
                                    cancellation);
                        }
                        return CompletableFuture.completedFuture(polled);
                    })
                    .exceptionallyCompose(error -> {
                        if (!isPartitionNotOwned(error)) {
                            return CompletableFuture.failedFuture(error);
                        }
                        routingState.invalidateAssignment(groupKey);
                        return pollGroupMessages(
                                streamId,
                                topicId,
                                consumer,
                                strategy,
                                count,
                                autoCommit,
                                attemptsLeft - 1,
                                cancellation);
                    });
        });
    }

    private CompletableFuture<Void> ensureFreshAssignment(
            StreamId streamId, TopicId topicId, Consumer consumer, ClientRoutingState.GroupKey groupKey) {
        var cached = routingState.assignment(groupKey);
        if (cached.isPresent() && System.nanoTime() - cached.get().syncedAtNanos() < ROUTING_CACHE_REFRESH.toNanos()) {
            return CompletableFuture.completedFuture(null);
        }
        return consumerGroupsClient
                .syncConsumerGroup(streamId, topicId, consumer.id())
                .thenAccept(assignment -> {
                    if (assignment.isEmpty()) {
                        // an empty sync reply means "not a member"
                        routingState.invalidateAssignment(groupKey);
                        return;
                    }
                    routingState.setAssignment(
                            groupKey,
                            assignment.get().generation(),
                            assignment.get().partitions(),
                            System.nanoTime());
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
        return new PolledMessages(0L, BigInteger.ZERO, 0L, List.of());
    }

    private CompletableFuture<Partitioning> resolvePartitioning(
            StreamId streamId, TopicId topicId, Partitioning partitioning) {
        return partitionCount(streamId, topicId).thenApply(partitionsCount -> switch (partitioning.kind()) {
            case Balanced ->
                Partitioning.partitionId(routingState.nextBalancedPartition(
                        ClientRoutingState.topicKey(streamId, topicId), partitionsCount));
            case MessagesKey -> Partitioning.partitionId(XxHash32.hashUnsigned(partitioning.value()) % partitionsCount);
            case PartitionId -> partitioning;
        });
    }

    private CompletableFuture<Long> partitionCount(StreamId streamId, TopicId topicId) {
        var topicKey = ClientRoutingState.topicKey(streamId, topicId);
        var cached = routingState.partitionCount(topicKey);
        if (cached.isPresent() && System.nanoTime() - cached.get().fetchedAtNanos() < ROUTING_CACHE_REFRESH.toNanos()) {
            return CompletableFuture.completedFuture(cached.get().count());
        }
        return topicsClient
                .getTopic(streamId, topicId)
                .thenApply(topicDetails -> {
                    var partitionsCount =
                            topicDetails.map(TopicDetails::partitionsCount).orElse(0L);
                    if (partitionsCount == 0) {
                        throw new IggyResourceNotFoundException(
                                IggyErrorCode.TOPIC_ID_NOT_FOUND,
                                IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode(),
                                "Cannot resolve partitioning: topic " + topicId + " in stream " + streamId
                                        + " was not found or has no partitions",
                                Optional.empty(),
                                Optional.empty());
                    }
                    routingState.setPartitionCount(topicKey, partitionsCount, System.nanoTime());
                    return partitionsCount;
                })
                .exceptionallyCompose(error -> {
                    // A failed refresh should not stop routing while a stale
                    // count is still on hand, except when the topic itself is
                    // gone; serving the stale value keeps sends flowing
                    // through transient metadata-fetch failures.
                    if (cached.isPresent() && !(unwrapCompletion(error) instanceof IggyResourceNotFoundException)) {
                        return CompletableFuture.completedFuture(cached.get().count());
                    }
                    return CompletableFuture.failedFuture(error);
                });
    }

    private static final class PollCancellation {
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
