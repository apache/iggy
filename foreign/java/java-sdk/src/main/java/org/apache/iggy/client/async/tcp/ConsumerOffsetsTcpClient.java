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
import org.apache.iggy.client.async.ConsumerOffsetsClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Async TCP implementation of consumer offsets client.
 */
public class ConsumerOffsetsTcpClient implements ConsumerOffsetsClient {
    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetsTcpClient.class);

    private static final byte ACK_QUORUM = 1;
    private static final int STORE = CommandCode.ConsumerOffset.STORE.getValue();

    private final Supplier<AsyncTcpConnection> connectionSupplier;
    private final PartitionContexts storeContexts;
    private final PollRouter pollRouter;
    private final Supplier<CompletableFuture<Boolean>> clustered;

    /**
     * Creates a low-level client on the supplied connection without primary routing.
     * Use {@code Iggy.tcpClientBuilder()} for a cluster, so offset writes reach the
     * partition primary while the coordinator retains group membership.
     */
    public ConsumerOffsetsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, null, () -> CompletableFuture.completedFuture(false));
    }

    ConsumerOffsetsTcpClient(
            Supplier<AsyncTcpConnection> connectionSupplier,
            PollRouter pollRouter,
            Supplier<CompletableFuture<Boolean>> clustered) {
        this.connectionSupplier = connectionSupplier;
        this.storeContexts = new PartitionContexts(
                connectionSupplier,
                CommandCode.ConsumerOffset.GET_ROUTING.getValue(),
                PollRouter.ATTACHMENT_BYTES,
                false);
        this.pollRouter = pollRouter;
        this.clustered = clustered;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
    }

    @Override
    public CompletableFuture<Void> storeConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        log.debug(
                "Storing consumer offset - Stream: {}, Topic: {}, Partition: {}, Consumer: {}, Offset: {}",
                streamId,
                topicId,
                partitionId,
                consumer,
                offset);

        return store(streamId, topicId, partitionId, consumer, offset, Optional.empty());
    }

    @Override
    public CompletableFuture<Void> storeConsumerOffset(
            StreamId streamId,
            TopicId topicId,
            Long partitionId,
            Consumer consumer,
            BigInteger offset,
            PartitionContext context) {
        return store(streamId, topicId, Optional.of(partitionId), consumer, offset, Optional.of(context));
    }

    @Override
    public CompletableFuture<Optional<ConsumerOffsetInfo>> getConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var payload = offsetTarget(consumer, streamId, topicId, partitionId);

        log.debug(
                "Getting consumer offset - Stream: {}, Topic: {}, Partition: {}, Consumer: {}",
                streamId,
                topicId,
                partitionId,
                consumer);

        return connection()
                .send(CommandCode.ConsumerOffset.GET.getValue(), payload)
                .thenApply(response -> {
                    try {
                        if (response.isReadable()) {
                            return Optional.of(BytesDeserializer.readConsumerOffsetInfo(response));
                        } else {
                            return Optional.empty();
                        }
                    } finally {
                        response.release();
                    }
                });
    }

    private CompletableFuture<Void> store(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            BigInteger offset,
            Optional<PartitionContext> context) {
        var payload = offsetTarget(consumer, streamId, topicId, partitionId);
        int targetLength = payload.readableBytes();
        payload.writeBytes(BytesSerializer.toBytesAsU64(offset));
        payload.writeByte(ACK_QUORUM);
        return write(STORE, payload, targetLength, context).thenAccept(ByteBuf::release);
    }

    /**
     * Routes an offset write as Rust's {@code send_offset_write_with_response} does:
     * a cluster takes it to the partition primary, a single node serves it here.
     * Takes ownership of {@code payload}, whose first {@code targetLength} bytes
     * name the consumer and the partition.
     */
    CompletableFuture<ByteBuf> write(
            int command, ByteBuf payload, int targetLength, Optional<PartitionContext> context) {
        if (pollRouter == null) {
            return writeOnCoordinator(command, payload, targetLength, context);
        }
        return clustered
                .get()
                .handle((isClustered, error) -> {
                    if (error != null) {
                        payload.release();
                        return CompletableFuture.<ByteBuf>failedFuture(error);
                    }
                    return isClustered
                            ? pollRouter.writeOffset(command, payload, targetLength, context)
                            : writeOnCoordinator(command, payload, targetLength, context);
                })
                .thenCompose(written -> written);
    }

    private CompletableFuture<ByteBuf> writeOnCoordinator(
            int command, ByteBuf payload, int targetLength, Optional<PartitionContext> context) {
        return storeContexts.send(
                command, payload, payload.retainedSlice(payload.readerIndex(), targetLength), targetLength, context);
    }

    private static ByteBuf offsetTarget(
            Consumer consumer, StreamId streamId, TopicId topicId, Optional<Long> partitionId) {
        var target = BytesSerializer.toBytes(consumer);
        target.writeBytes(BytesSerializer.toBytes(streamId));
        target.writeBytes(BytesSerializer.toBytes(topicId));
        target.writeBytes(BytesSerializer.toBytes(partitionId));
        return target;
    }
}
