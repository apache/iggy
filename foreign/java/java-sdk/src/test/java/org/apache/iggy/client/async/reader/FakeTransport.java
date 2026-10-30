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

package org.apache.iggy.client.async.reader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** A transport whose reads the test answers one by one, and which records every store, abandon and group change. */
final class FakeTransport implements ReaderTransport {
    static final Duration TIMEOUT = Duration.ofSeconds(5);
    static final PartitionContext CONTEXT =
            new PartitionContext(BigInteger.valueOf(7), BigInteger.valueOf(1), BigInteger.valueOf(1));
    private static final int BATCH_HEADER_SIZE = 256;
    private static final int MESSAGE_HEADER_SIZE = 48;

    final Map<Long, Long> starts = new ConcurrentHashMap<>();
    final List<Long> deleted = new CopyOnWriteArrayList<>();
    final BlockingQueue<Read> reads = new LinkedBlockingQueue<>();
    final List<Store> stores = new CopyOnWriteArrayList<>();
    final List<Long> abandoned = new CopyOnWriteArrayList<>();
    final List<List<Long>> groupings = new CopyOnWriteArrayList<>();
    final BlockingQueue<CompletableFuture<Optional<ConsumerGroupAssignment>>> assignments = new LinkedBlockingQueue<>();
    final List<String> membership = new CopyOnWriteArrayList<>();
    private final boolean group;
    private volatile Function<List<Long>, List<List<Long>>> grouper = partitions -> List.of(partitions);

    FakeTransport(boolean group) {
        this.group = group;
    }

    FakeTransport groupedBy(Function<List<Long>, List<List<Long>>> grouper) {
        this.grouper = grouper;
        return this;
    }

    /** The next read that the reader sends. */
    Read nextRead() throws InterruptedException {
        Read read = reads.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (read == null) {
            throw new AssertionError("The reader sent no read");
        }
        return read;
    }

    /** Whether the reader sends no read within {@code wait}. */
    boolean readsNothingFor(Duration wait) throws InterruptedException {
        Read read = reads.poll(wait.toMillis(), TimeUnit.MILLISECONDS);
        if (read != null) {
            reads.add(read);
        }
        return read == null;
    }

    @Override
    public boolean group() {
        return group;
    }

    @Override
    public CompletableFuture<List<List<Long>>> groups(List<Long> partitions) {
        groupings.add(partitions);
        try {
            return CompletableFuture.completedFuture(grouper.apply(partitions));
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
    }

    @Override
    public CompletableFuture<List<FetchedMessages>> read(
            Map<Long, PollingStrategy> targets, long count, boolean commit, DeferredPollOptions options) {
        if (!commit
                && count == 1
                && targets.size() == 1
                && targets.values().iterator().next().kind() == org.apache.iggy.message.PollingKind.Next) {
            long partition = targets.keySet().iterator().next();
            return CompletableFuture.completedFuture(List.of(section(partition, starts.getOrDefault(partition, 0L))));
        }
        Read read = new Read(
                Collections.unmodifiableMap(new LinkedHashMap<>(targets)), count, commit, new CompletableFuture<>());
        reads.add(read);
        return read.reply;
    }

    @Override
    public CompletableFuture<Void> store(long partition, BigInteger offset, PartitionContext context) {
        stores.add(new Store(partition, offset.longValue()));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> delete(long partition, PartitionContext context) {
        deleted.add(partition);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<ConsumerOffsetInfo>> storedOffset(long partition) {
        return CompletableFuture.completedFuture(Optional.empty());
    }

    @Override
    public CompletableFuture<Void> join() {
        membership.add("join");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Optional<ConsumerGroupAssignment>> assignment() {
        CompletableFuture<Optional<ConsumerGroupAssignment>> assignment = new CompletableFuture<>();
        assignments.add(assignment);
        return assignment;
    }

    @Override
    public CompletableFuture<Void> leave() {
        membership.add("leave");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> abandon(long operationId) {
        abandoned.add(operationId);
        return CompletableFuture.completedFuture(null);
    }

    /** An OK section of {@code partition} with one message at each of {@code offsets}. */
    static FetchedMessages section(long partition, long... offsets) {
        ByteBuf records = Unpooled.buffer();
        for (long offset : offsets) {
            records.writeBytes(record(offset));
        }
        long next = offsets[offsets.length - 1] + 1;
        return new FetchedMessages(
                partition,
                FetchedMessages.STATUS_OK,
                BigInteger.valueOf(next),
                offsets.length,
                CONTEXT,
                BigInteger.valueOf(next),
                ByteBufUtil.getBytes(records));
    }

    /** A section of {@code partition} that refuses its read with {@code status}. */
    static FetchedMessages refused(long partition, long status) {
        return new FetchedMessages(partition, status, BigInteger.ZERO, 0, CONTEXT, BigInteger.ZERO, new byte[0]);
    }

    /** One batch record with one message at {@code offset}. */
    static ByteBuf record(long offset) {
        return batch(offset, frame(0, "message " + offset, 0));
    }

    /** One batch record based at {@code baseOffset} holding {@code frames}. */
    static ByteBuf batch(long baseOffset, byte[]... frames) {
        ByteBuf blob = Unpooled.buffer();
        for (byte[] frame : frames) {
            blob.writeBytes(frame);
        }
        return Unpooled.buffer()
                .writeLongLE(0)
                .writeLongLE(baseOffset)
                .writeLongLE(0)
                .writeLongLE(0)
                .writeLongLE(BATCH_HEADER_SIZE + blob.readableBytes())
                .writeZero(BATCH_HEADER_SIZE - 5 * Long.BYTES)
                .writeBytes(blob);
    }

    /** One message frame at {@code offsetDelta} of its batch, with {@code reserved} in the reserved field. */
    static byte[] frame(long offsetDelta, String payload, long reserved) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBufUtil.getBytes(Unpooled.buffer()
                .writeLongLE(0)
                .writeZero(16)
                .writeIntLE((int) offsetDelta)
                .writeIntLE(0)
                .writeIntLE(0)
                .writeIntLE(bytes.length)
                .writeLongLE(reserved)
                .writeBytes(bytes));
    }

    record Read(
            Map<Long, PollingStrategy> targets,
            long count,
            boolean commit,
            CompletableFuture<List<FetchedMessages>> reply) {
        void answer(FetchedMessages... sections) {
            reply.complete(List.of(sections));
        }
    }

    record Store(long partition, long offset) {}
}
