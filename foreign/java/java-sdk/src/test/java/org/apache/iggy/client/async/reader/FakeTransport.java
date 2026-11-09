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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A transport whose reads the test answers one by one. It keeps the stored offsets as the server does and records every
 * store, lookup, delete, abandon and group change.
 */
final class FakeTransport implements ReaderTransport {
    static final Duration TIMEOUT = Duration.ofSeconds(5);
    static final PartitionContext CONTEXT =
            new PartitionContext(BigInteger.valueOf(7), BigInteger.valueOf(1), BigInteger.valueOf(1));
    private static final int BATCH_HEADER_SIZE = 256;
    private static final int MESSAGE_HEADER_SIZE = 48;
    // The server's ConsumerOffsetNotFound, its refusal of a delete of a missing offset.
    private static final int CONSUMER_OFFSET_NOT_FOUND = 3021;
    // The server's InvalidOffset, its refusal of a store past the last message of a partition.
    private static final int INVALID_OFFSET = 4100;
    // The server's FETCH_STRATEGY_KINDS. It refuses a fetch with another target.
    private static final Set<PollingKind> FETCH_KINDS = EnumSet.of(PollingKind.Offset, PollingKind.Next);

    // The first offset of each partition, where a Next read starts without a stored offset.
    final Map<Long, Long> starts = new ConcurrentHashMap<>();
    // The last offset of each partition that a test models. The server refuses a store past it.
    final Map<Long, Long> ends = new ConcurrentHashMap<>();
    // The answer to the probe of a partition, instead of one message at its start.
    final Map<Long, FetchedMessages> probes = new ConcurrentHashMap<>();
    final List<Long> deleted = new CopyOnWriteArrayList<>();
    final BlockingQueue<RuntimeException> deleteFailures = new LinkedBlockingQueue<>();
    final List<Long> lookups = new CopyOnWriteArrayList<>();
    // The stored offset of each partition: a read moves it forward only, a store sets it, a delete removes it.
    final Map<Long, Long> offsets = new ConcurrentHashMap<>();
    // The reply to each lookup of a stored offset, which the test holds back.
    volatile CompletableFuture<Void> lookupReply = CompletableFuture.completedFuture(null);
    final BlockingQueue<Read> reads = new LinkedBlockingQueue<>();
    final List<Store> stores = new CopyOnWriteArrayList<>();
    // Every store that the reader sends, before its reply.
    final List<Store> sent = new CopyOnWriteArrayList<>();
    final BlockingQueue<RuntimeException> storeFailures = new LinkedBlockingQueue<>();
    // The reply to each store, which the test holds back. A store takes effect with its reply.
    volatile CompletableFuture<Void> storeReply = CompletableFuture.completedFuture(null);
    // The reply to each lookup of the groups, which the test holds back.
    volatile CompletableFuture<Void> groupsReply = CompletableFuture.completedFuture(null);
    // The refusal that the next lookup of the groups throws before it sends, as the client does for a bad partition id.
    volatile RuntimeException groupsRefusal;
    final List<Long> abandoned = new CopyOnWriteArrayList<>();
    // The reply to each abandon, which the test holds back.
    volatile CompletableFuture<Void> abandonReply = CompletableFuture.completedFuture(null);
    // The operation of each partition whose store has an unknown outcome. As in the client, it blocks the later stores
    // and deletes of the partition until its abandon.
    final Map<Long, Long> uncertain = new ConcurrentHashMap<>();
    // The partition of every read and poll, also of each probe that the fake answers itself.
    final List<Long> requested = new CopyOnWriteArrayList<>();
    final List<List<Long>> groupings = new CopyOnWriteArrayList<>();
    final BlockingQueue<CompletableFuture<Optional<ConsumerGroupAssignment>>> assignments = new LinkedBlockingQueue<>();
    final List<String> membership = new CopyOnWriteArrayList<>();
    volatile RuntimeException joinFailure;
    volatile RuntimeException leaveFailure;
    // The reply to each leave, which the test holds back.
    volatile CompletableFuture<Void> leaveReply = CompletableFuture.completedFuture(null);
    // The reply to the cancel of the committing reads, which the test holds back.
    volatile CompletableFuture<Void> settled = CompletableFuture.completedFuture(null);
    // The operations of the committing reads whose outcome stayed unknown after the cancel.
    final List<Long> lost = new CopyOnWriteArrayList<>();
    // Every abandon, store and delete in order.
    final List<String> events = new CopyOnWriteArrayList<>();
    // The context of every store and delete in order.
    final List<PartitionContext> written = new CopyOnWriteArrayList<>();
    private final boolean group;
    private volatile Function<List<Long>, List<List<Long>>> grouper = partitions -> List.of(partitions);
    // A group write with an unknown outcome blocks the later ones until its abandon, as the metadata lane does.
    private volatile Long uncertainGroupWrite;
    // The owner generation that the server installed for this member and the metadata op of that install. Null until a
    // session change, so the fake checks no owner before it.
    private volatile BigInteger installedOwner;
    private volatile BigInteger installedAt = BigInteger.ZERO;
    private volatile BigInteger watermark = CONTEXT.metadataOp();

    FakeTransport(boolean group) {
        this.group = group;
    }

    FakeTransport groupedBy(Function<List<Long>, List<List<Long>>> grouper) {
        this.grouper = grouper;
        return this;
    }

    /**
     * A new session of the client: the server installs the owner of each partition of the member again under a higher
     * generation, at the op of the register, whose reply raises the metadata watermark of the client to that op.
     */
    void changeSession() {
        watermark = watermark.add(BigInteger.ONE);
        installedOwner = (installedOwner == null ? CONTEXT.ownerGeneration() : installedOwner).add(BigInteger.ONE);
        installedAt = watermark;
    }

    /**
     * A rebalance by another member that gives the partitions of this member an owner of {@code generation} at the
     * metadata {@code op}. No reply of the client names that op.
     */
    void installOwner(BigInteger generation, BigInteger op) {
        installedOwner = generation;
        installedAt = op;
    }

    /** A metadata commit that the client saw. It raises the watermark of the client and installs no owner. */
    void commitMetadata() {
        watermark = watermark.add(BigInteger.ONE);
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
        RuntimeException refusal = groupsRefusal;
        if (refusal != null) {
            groupsRefusal = null;
            throw refusal;
        }
        return groupsReply.thenApply(ignored -> grouper.apply(partitions));
    }

    @Override
    public CompletableFuture<List<FetchedMessages>> read(
            Map<Long, PollingStrategy> targets, long count, boolean commit, DeferredPollOptions options) {
        requested.addAll(targets.keySet());
        if (targets.values().stream().anyMatch(target -> !FETCH_KINDS.contains(target.kind()))) {
            return CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(IggyErrorCode.INVALID_COMMAND.getCode(), new byte[0]));
        }
        if (!commit
                && count == 1
                && targets.size() == 1
                && targets.values().iterator().next().kind() == PollingKind.Next) {
            long partition = targets.keySet().iterator().next();
            FetchedMessages probe = probes.get(partition);
            Long stored = offsets.get(partition);
            long first = stored != null ? stored + 1 : starts.getOrDefault(partition, 0L);
            return CompletableFuture.completedFuture(List.of(probe != null ? probe : section(partition, first)));
        }
        Read read = new Read(
                Collections.unmodifiableMap(new LinkedHashMap<>(targets)),
                count,
                commit,
                options,
                new CompletableFuture<>(),
                false);
        if (commit) {
            read.reply.thenAccept(sections -> sections.forEach(this::autoCommit));
        }
        reads.add(read);
        return read.reply;
    }

    @Override
    public CompletableFuture<PolledMessages> poll(
            long partition, PollingStrategy strategy, long count, DeferredPollOptions options) {
        return polled(partition, strategy, count, options).thenApply(FetchedMessages::decode);
    }

    /** As the client, it fails on the status of the reply and decodes none of its messages. */
    @Override
    public CompletableFuture<PartitionContext> pollContext(
            long partition, PollingStrategy strategy, long count, DeferredPollOptions options) {
        return polled(partition, strategy, count, options).thenApply(section -> {
            if (section.status() != FetchedMessages.STATUS_OK) {
                throw IggyServerException.fromTcpResponse(section.status(), new byte[0]);
            }
            return section.context();
        });
    }

    @Override
    public CompletableFuture<Void> store(long partition, BigInteger offset, PartitionContext context) {
        sent.add(new Store(partition, offset.longValue()));
        written.add(context);
        return storeReply.thenCompose(ignored -> {
            if (uncertain.containsKey(partition)) {
                return CompletableFuture.failedFuture(laneBlocked(partition));
            }
            RuntimeException failure = storeFailures.poll();
            if (failure instanceof IggyOperationException unknown
                    && unknown.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN) {
                // The worst case of an unknown outcome: the server stored the offset.
                offsets.put(partition, offset.longValue());
                uncertain.put(partition, unknown.getOperationId());
            }
            if (failure != null) {
                return CompletableFuture.failedFuture(failure);
            }
            if (!owns(context)) {
                return CompletableFuture.failedFuture(notOwned());
            }
            if (offset.longValue() > ends.getOrDefault(partition, Long.MAX_VALUE)) {
                return CompletableFuture.failedFuture(IggyServerException.fromTcpResponse(INVALID_OFFSET, new byte[0]));
            }
            offsets.put(partition, offset.longValue());
            stores.add(new Store(partition, offset.longValue()));
            events.add("store " + partition + " " + offset);
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public CompletableFuture<Void> delete(long partition, PartitionContext context) {
        written.add(context);
        deleted.add(partition);
        if (uncertain.containsKey(partition)) {
            return CompletableFuture.failedFuture(laneBlocked(partition));
        }
        RuntimeException failure = deleteFailures.poll();
        if (failure instanceof IggyOperationException unknown
                && unknown.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN) {
            // The worst case of an unknown outcome: the server deleted the offset.
            offsets.remove(partition);
            uncertain.put(partition, unknown.getOperationId());
        }
        if (failure != null) {
            return CompletableFuture.failedFuture(failure);
        }
        if (!owns(context)) {
            return CompletableFuture.failedFuture(notOwned());
        }
        if (offsets.remove(partition) == null) {
            return CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(CONSUMER_OFFSET_NOT_FOUND, new byte[0]));
        }
        events.add("delete " + partition);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public BigInteger metadataWatermark() {
        return watermark;
    }

    @Override
    public CompletableFuture<Optional<ConsumerOffsetInfo>> storedOffset(long partition) {
        lookups.add(partition);
        return lookupReply.thenApply(ignored -> Optional.ofNullable(offsets.get(partition))
                .map(BigInteger::valueOf)
                .map(offset -> new ConsumerOffsetInfo(partition, offset, offset)));
    }

    @Override
    public CompletableFuture<Void> join() {
        membership.add("join");
        return groupWrite(joinFailure);
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
        return leaveReply.thenCompose(ignored -> groupWrite(leaveFailure));
    }

    /** A poll that the test answers with one section. */
    private CompletableFuture<FetchedMessages> polled(
            long partition, PollingStrategy strategy, long count, DeferredPollOptions options) {
        requested.add(partition);
        Read read = new Read(Map.of(partition, strategy), count, false, options, new CompletableFuture<>(), true);
        reads.add(read);
        return read.reply.thenApply(sections -> sections.get(0));
    }

    private CompletableFuture<Void> groupWrite(RuntimeException failure) {
        Long blocking = uncertainGroupWrite;
        if (blocking != null) {
            return CompletableFuture.failedFuture(new IggyOperationException(IggyErrorCode.LANE_BLOCKED, blocking));
        }
        if (failure instanceof IggyOperationException unknown
                && unknown.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN) {
            uncertainGroupWrite = unknown.getOperationId();
        }
        return failure == null ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(failure);
    }

    @Override
    public CompletableFuture<Void> abandon(long operationId) {
        abandoned.add(operationId);
        return abandonReply.thenRun(() -> {
            uncertain.values().remove(operationId);
            if (Long.valueOf(operationId).equals(uncertainGroupWrite)) {
                uncertainGroupWrite = null;
            }
        });
    }

    @Override
    public CompletableFuture<Void> cancelledReadsSettled() {
        return settled;
    }

    @Override
    public CompletableFuture<Void> abandonLost() {
        lost.forEach(operation -> {
            abandoned.add(operation);
            events.add("abandon " + operation);
        });
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Whether the server takes an offset write with {@code context}: it names the installed owner, or owner generation
     * 0 with a metadata op at or after the install. A write without a context names at least the watermark of the client.
     */
    private boolean owns(PartitionContext context) {
        BigInteger owner = installedOwner;
        BigInteger op = context.equals(PartitionContext.EMPTY) ? watermark : context.metadataOp();
        return owner == null
                || context.ownerGeneration().equals(owner)
                || (context.ownerGeneration().signum() == 0 && op.compareTo(installedAt) >= 0);
    }

    private static IggyServerException notOwned() {
        return IggyServerException.fromTcpResponse(
                IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode(), new byte[0]);
    }

    private IggyOperationException laneBlocked(long partition) {
        return new IggyOperationException(IggyErrorCode.PARTITION_LANE_BLOCKED, uncertain.get(partition), partition);
    }

    /** The server stores the last offset of a committing read, and never moves it back. */
    private void autoCommit(FetchedMessages section) {
        if (section.status() == FetchedMessages.STATUS_OK && section.count() > 0) {
            offsets.merge(section.partitionId(), section.nextOffset().longValue() - 1, Math::max);
        }
    }

    /** An OK section of {@code partition} with one message at each of {@code offsets}. */
    static FetchedMessages section(long partition, long... offsets) {
        return section(partition, CONTEXT, offsets);
    }

    /** An OK section of {@code partition} in the history of {@code context}. */
    static FetchedMessages section(long partition, PartitionContext context, long... offsets) {
        ByteBuf records = Unpooled.buffer();
        for (long offset : offsets) {
            records.writeBytes(record(offset));
        }
        long next = offsets[offsets.length - 1] + 1;
        // The read ends at the head of the partition, its last offset.
        return new FetchedMessages(
                partition,
                FetchedMessages.STATUS_OK,
                BigInteger.valueOf(next - 1),
                offsets.length,
                context,
                BigInteger.valueOf(next),
                ByteBufUtil.getBytes(records));
    }

    /** An OK section of {@code partition} that ends before the head {@code head} of the partition. */
    static FetchedMessages backlogged(long partition, long head, long... offsets) {
        FetchedMessages section = section(partition, offsets);
        return new FetchedMessages(
                partition,
                section.status(),
                BigInteger.valueOf(head),
                section.count(),
                section.context(),
                section.nextOffset(),
                section.records());
    }

    /** An OK section of {@code partition} without messages, whose next offset is {@code next}. */
    static FetchedMessages empty(long partition, long next) {
        return new FetchedMessages(
                partition,
                FetchedMessages.STATUS_OK,
                BigInteger.valueOf(Math.max(0, next - 1)),
                0,
                CONTEXT,
                BigInteger.valueOf(next),
                new byte[0]);
    }

    /** A section of a committing read of {@code partition} whose outcome {@code operation} left unknown. */
    static FetchedMessages lost(long partition, long operation) {
        return new FetchedMessages(
                partition,
                11001,
                BigInteger.ZERO,
                0,
                CONTEXT,
                BigInteger.ZERO,
                new byte[0],
                OptionalLong.of(operation));
    }

    /** A section of {@code partition} that refuses its read with {@code status}. */
    static FetchedMessages refused(long partition, long status) {
        return new FetchedMessages(partition, status, BigInteger.ZERO, 0, CONTEXT, BigInteger.ZERO, new byte[0]);
    }

    /**
     * An OK section of {@code partition} in the history of {@code context} with one message at {@code offset} that does
     * not decode: its reserved field is not zero.
     */
    static FetchedMessages undecodable(long partition, PartitionContext context, long offset) {
        return new FetchedMessages(
                partition,
                FetchedMessages.STATUS_OK,
                BigInteger.valueOf(offset),
                1,
                context,
                BigInteger.valueOf(offset + 1),
                ByteBufUtil.getBytes(batch(offset, frame(0, "message " + offset, 1))));
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
            DeferredPollOptions options,
            CompletableFuture<List<FetchedMessages>> reply,
            boolean poll) {
        void answer(FetchedMessages... sections) {
            reply.complete(List.of(sections));
        }
    }

    record Store(long partition, long offset) {}
}
