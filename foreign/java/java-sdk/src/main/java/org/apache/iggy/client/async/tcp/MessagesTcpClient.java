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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

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

    public MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, new ClientRoutingState());
    }

    MessagesTcpClient(Supplier<AsyncTcpConnection> connectionSupplier, ClientRoutingState routingState) {
        this.connectionSupplier = connectionSupplier;
        this.routingState = routingState;
        this.topicsClient = new TopicsTcpClient(connectionSupplier);
        this.consumerGroupsClient = new ConsumerGroupsTcpClient(connectionSupplier);
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
            return pollGroupMessages(streamId, topicId, consumer, strategy, count, autoCommit, GROUP_POLL_MAX_ATTEMPTS);
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
        return connection().send(CommandCode.Messages.POLL.getValue(), payload).thenApply(response -> {
            try {
                return BytesDeserializer.readPolledMessages(response);
            } finally {
                response.release();
            }
        });
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

        // Build metadata section following the blocking client pattern
        var metadataLength = streamId.getSize() + topicId.getSize() + partitioning.getSize() + 4;
        var payload = Unpooled.buffer(4 + metadataLength);

        // Write metadata length and components
        payload.writeIntLE(metadataLength);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(partitioning));
        payload.writeIntLE(messages.size());

        // Write message index metadata (required by server)
        var position = 0;
        for (var message : messages) {
            // Calculate position for next message
            position += message.getSize();

            // offset (4 bytes)
            payload.writeIntLE(0);
            // position (4 bytes)
            payload.writeIntLE(position);
            // timestamp (8 bytes)
            payload.writeZero(8);
        }

        // Write actual message data
        for (var message : messages) {
            payload.writeBytes(toBytes(message));
        }

        return connection().send(CommandCode.Messages.SEND.getValue(), payload).thenApply(response -> {
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

    /**
     * One group-poll attempt: sync the assignment when missing or stale, pick
     * the next assigned partition round-robin, poll it explicitly, and on a
     * generation fence (the re-sync sentinel or a partition-not-owned error)
     * drop the cached assignment and retry. The attempt budget allows one
     * re-sync after the coordinator rejects a stale assignment, then one
     * retry; an exhausted budget is an empty poll, not an error.
     */
    private CompletableFuture<PolledMessages> pollGroupMessages(
            StreamId streamId,
            TopicId topicId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            int attemptsLeft) {
        if (attemptsLeft == 0) {
            return CompletableFuture.completedFuture(emptyPolledMessages());
        }
        var groupKey = ClientRoutingState.groupKey(streamId, topicId, consumer.id());
        return ensureFreshAssignment(streamId, topicId, consumer, groupKey).thenCompose(ignored -> {
            var partitionId = routingState.nextGroupPartition(groupKey);
            if (partitionId.isEmpty()) {
                if (routingState.assignment(groupKey).isPresent()) {
                    // a member owning no partitions polls nothing
                    return CompletableFuture.completedFuture(emptyPolledMessages());
                }
                return CompletableFuture.failedFuture(new IggyResourceNotFoundException(
                        IggyErrorCode.CONSUMER_GROUP_NOT_JOINED,
                        IggyErrorCode.CONSUMER_GROUP_NOT_JOINED.getCode(),
                        "Cannot poll consumer group " + consumer.id() + " for topic " + topicId + " in stream "
                                + streamId + ": this client is not a member, join the group first",
                        Optional.empty(),
                        Optional.empty()));
            }
            return pollPartition(
                            streamId,
                            topicId,
                            Optional.of(partitionId.getAsLong()),
                            consumer,
                            strategy,
                            count,
                            autoCommit)
                    .thenCompose(polled -> {
                        if (polled.messages().isEmpty() && polled.partitionId() == RESYNC_REQUIRED_PARTITION_SENTINEL) {
                            routingState.invalidateAssignment(groupKey);
                            return pollGroupMessages(
                                    streamId, topicId, consumer, strategy, count, autoCommit, attemptsLeft - 1);
                        }
                        return CompletableFuture.completedFuture(polled);
                    })
                    .exceptionallyCompose(error -> {
                        if (!isPartitionNotOwned(error)) {
                            return CompletableFuture.failedFuture(error);
                        }
                        routingState.invalidateAssignment(groupKey);
                        return pollGroupMessages(
                                streamId, topicId, consumer, strategy, count, autoCommit, attemptsLeft - 1);
                    });
        });
    }

    private CompletableFuture<Void> ensureFreshAssignment(
            StreamId streamId, TopicId topicId, Consumer consumer, String groupKey) {
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
}
