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

import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.DecodeFailure;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Reads the partitions of a topic in batches, with routing, prefetch and the position of each partition inside.
 *
 * <ul>
 *   <li>A standalone consumer reads the partitions of its builder. A consumer group reads the assignment of its
 *       member: the reader joins the group, follows its assignment, and leaves it in {@link #closeAsync()}.
 *   <li>{@link #next()} returns the next batch of one partition, in offset order within one generation of the
 *       partition: from its first read, from each {@link #seek}, and from each new assignment to this member.
 *   <li>The commit policy of the options decides who stores offsets. With {@code Manual}, {@link #commit} stores the
 *       offset of a checkpoint. With {@code OnRead}, the server stores the offset of each read, the reader reads only
 *       when the application waits for a batch, and {@link #closeAsync()} moves the offset back to the last delivered
 *       message. A partition that leaves the assignment still hands over the reads that the server stored for it.
 *       After a read whose reply was lost, the reader reads again what the lost read stored and delivers it first.
 *       With {@code Auto}, the reader counts each batch as its {@link CommitAfter} says, or each {@link #mark},
 *       and stores the latest counted position at the interval or the event of the policy, before a rebalance takes a
 *       partition away, and in {@link #closeAsync()}.
 *   <li>The reader stays usable after a {@link ReadException} and retries by itself. {@code HistoryUnavailable} stops
 *       its partition until a {@link #seek} when the position came from an earlier read of a history that is gone. A
 *       refusal that a quick retry does not change, such as {@code Unauthorized}, is retried after a wait that doubles
 *       each time, and after six in a row it stops its partition until a {@link #seek} with one error.
 * </ul>
 *
 * <p>The futures of a reader complete on its own thread, so a callback must not block.
 */
public final class PartitionReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartitionReader.class);
    private static final AtomicLong READERS = new AtomicLong();
    private static final int LASTING_REFUSALS_BEFORE_STOP = 6;
    private static final int MAX_BACKOFF_SHIFT = 5;
    private static final long WHOLE_READER = -1;

    private final ReaderTransport transport;
    private final List<Long> standalonePartitions;
    private final PollingStrategy start;
    private final PollOptions options;
    private final int slots;
    private final long retryIntervalNanos;
    private final boolean onRead;
    private final Optional<AutoCommit> autoCommit;
    private final ScheduledThreadPoolExecutor loop;

    // Everything below is read and written on the loop only, except the volatile list.
    private final Map<Long, Partition> partitions = new TreeMap<>();
    private final Map<Long, Long> seekGenerations = new HashMap<>();
    private volatile List<Long> partitionIds = List.of();
    private final Deque<Entry> queue = new ArrayDeque<>();
    private final Deque<Waiter> waiters = new ArrayDeque<>();
    private final Set<CompletableFuture<List<FetchedMessages>>> reads = new HashSet<>();
    private final List<Retired> retiring = new ArrayList<>();
    private List<List<Long>> groups = List.of();
    private boolean regroup = true;
    private boolean grouping;
    private long regroupAt;
    private int groupCursor;
    private long serves;
    private boolean refreshing;
    private long refreshAt;
    private long lastGeneration;
    private Uncounted uncounted;
    private CompletableFuture<Void> flushing;
    private boolean flushAgain;
    private ScheduledFuture<?> wake;
    private ScheduledFuture<?> interval;
    private CompletableFuture<Void> closed;

    PartitionReader(
            ReaderTransport transport,
            List<Long> standalonePartitions,
            PollingStrategy start,
            PollOptions options,
            int slots,
            Duration retryInterval) {
        this.transport = transport;
        this.standalonePartitions = standalonePartitions;
        this.start = start;
        this.options = options;
        this.slots = slots;
        this.retryIntervalNanos = retryInterval.toNanos();
        this.onRead = options.commitsOnRead();
        this.autoCommit = options.autoCommit();
        this.loop = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "iggy-partition-reader-" + READERS.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        this.loop.setRemoveOnCancelPolicy(true);
    }

    /** Starts the builder of a reader that sends its requests through {@code transport}. */
    public static PartitionReaderBuilder builder(ReaderTransport transport) {
        return new PartitionReaderBuilder(transport);
    }

    /**
     * The next batch of one partition. Never empty, and it has no time limit. Cancel safe: a cancelled call leaves its
     * batch in the reader. Fails with a {@link ReadException} for an error of a read.
     */
    public CompletableFuture<PolledMessages> next() {
        CompletableFuture<PolledMessages> result = new CompletableFuture<>();
        run(() -> {
            countUncounted();
            if (closed != null) {
                result.completeExceptionally(closedError());
                return;
            }
            waiters.addLast(new Waiter(result, true));
            handOver();
            pump();
        });
        return result;
    }

    /**
     * The next batch, without recording it as delivered: an {@link IggyConsumer} hands its messages over one by one
     * and records each with {@link #deliverMessage}.
     */
    CompletableFuture<PolledMessages> nextBatch() {
        CompletableFuture<PolledMessages> result = new CompletableFuture<>();
        run(() -> {
            if (closed != null) {
                result.completeExceptionally(closedError());
                return;
            }
            waiters.addLast(new Waiter(result, false));
            handOver();
            pump();
        });
        return result;
    }

    /** Records one message that an {@link IggyConsumer} handed over. */
    CompletableFuture<Void> deliverMessage(Checkpoint checkpoint) {
        return call(() -> {
            Partition partition = partitions.get(checkpoint.partitionId());
            if (partition != null) {
                if (partition.firstDelivered == null) {
                    partition.firstDelivered = checkpoint.offset();
                }
                partition.delivered = checkpoint;
            }
            return null;
        });
    }

    /**
     * Counts a checkpoint that this reader delivered, under {@code CommitPolicy.Auto}. It stores nothing now: the
     * policy stores the latest counted position of the partition. With {@link CommitAfter#MARKED} only marks count,
     * so batches processed concurrently count once they are done.
     *
     * @return a future that fails with {@link IggyErrorCode#INVALID_COMMAND} without {@code Auto}, and for a
     *         checkpoint that this reader did not deliver in the current generation of its partition
     */
    public CompletableFuture<Void> mark(Checkpoint checkpoint) {
        return call(() -> {
            if (autoCommit.isEmpty()) {
                throw invalid("Only an automatic commit counts marks");
            }
            Partition partition = delivering(checkpoint);
            count(partition, checkpoint, 1, false);
            return null;
        });
    }

    /**
     * Stores the offset of a checkpoint that this reader delivered. A checkpoint of an older generation of its
     * partition, or at or below the last commit of its generation, stores nothing.
     *
     * @return a future that fails with {@link IggyErrorCode#INVALID_COMMAND} under {@code OnRead}, and for a
     *         checkpoint that this reader did not deliver, or with the error of the store
     */
    public CompletableFuture<Void> commit(Checkpoint checkpoint) {
        return this.<CompletableFuture<Void>>call(() -> {
                    if (onRead) {
                        throw invalid("The server stores the offsets of a reader with OnRead");
                    }
                    Partition partition = partitions.get(checkpoint.partitionId());
                    if (partition == null) {
                        throw invalid("This reader does not read partition " + checkpoint.partitionId());
                    }
                    if (!partition.delivered(checkpoint)
                            || (partition.committed != null
                                    && checkpoint.offset().compareTo(partition.committed) <= 0)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    partition.committed = checkpoint.offset();
                    return transport.store(checkpoint.partitionId(), checkpoint.offset(), checkpoint.context());
                })
                .thenCompose(store -> store);
    }

    /**
     * Sets the next read position of {@code partitionId} and starts a new generation of it. It drops the queued
     * batches of the partition and ignores its late replies, and it resumes a stopped partition. Under {@code OnRead},
     * a seek to an offset stores the offset before it, so the next read of the server starts there, after the read in
     * flight ends.
     *
     * @return a future that fails with {@link IggyErrorCode#INVALID_COMMAND} for a partition that the reader does
     *         not read and for a continuation of another partition
     */
    public CompletableFuture<Void> seek(long partitionId, PollingStrategy start) {
        start.requirePartition(partitionId);
        return this.<CompletableFuture<Void>>call(() -> {
                    Partition partition = partitions.get(partitionId);
                    if (partition == null) {
                        throw invalid("This reader does not read partition " + partitionId);
                    }
                    seekGenerations.put(partitionId, ++lastGeneration);
                    queue.removeIf(entry -> entry.partition == partitionId);
                    PollingStrategy position = start.kind() == PollingKind.First
                            ? new PollingStrategy(PollingKind.Offset, BigInteger.ZERO, start.context())
                            : start;
                    if (onRead && partition.inFlight) {
                        partition.pendingSeek = position;
                        partition.sought = new CompletableFuture<>();
                        return partition.sought;
                    }
                    return applySeek(partition, position);
                })
                .thenCompose(sought -> sought);
    }

    /**
     * The partitions that the reader reads: those of a standalone consumer, or the current assignment of a group
     * member.
     */
    public List<Long> partitions() {
        return partitionIds;
    }

    /**
     * Stops the reader. It cancels the reads in flight. With {@code Manual} it stores nothing. With {@code Auto} it
     * counts the last batch under {@link CommitAfter#PROCESSING} and stores the latest counted position of each
     * partition. With {@code OnRead} it moves the offset of each partition with undelivered batches back to its last
     * delivered message. A group reader then leaves its group.
     *
     * @return a future that fails with the first failed store
     */
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            loop.execute(() -> shutDown(result));
        } catch (RejectedExecutionException alreadyClosed) {
            result.complete(null);
        }
        return result;
    }

    /** {@link #closeAsync()} and waits for it. */
    @Override
    public void close() {
        try {
            closeAsync().join();
        } catch (CompletionException error) {
            throw error.getCause() instanceof RuntimeException runtime ? runtime : error;
        }
    }

    CompletableFuture<PartitionReader> start() {
        CompletableFuture<PartitionReader> started = new CompletableFuture<>();
        autoCommit
                .flatMap(AutoCommit::interval)
                .ifPresent(period -> interval = loop.scheduleWithFixedDelay(
                        this::flush, period.toNanos(), period.toNanos(), TimeUnit.NANOSECONDS));
        run(() -> {
            if (!transport.group()) {
                standalonePartitions.forEach(this::addPartition);
                publishPartitions();
                started.complete(this);
                pump();
                return;
            }
            transport
                    .join()
                    .whenCompleteAsync(
                            (ignored, error) -> {
                                if (error != null) {
                                    loop.shutdownNow();
                                    started.completeExceptionally(unwrap(error));
                                    return;
                                }
                                started.complete(this);
                                pump();
                            },
                            loop);
        });
        return started;
    }

    /** Counts {@code messages} up to {@code checkpoint} of a delivered batch, for an {@code IggyConsumer}. */
    CompletableFuture<Void> countDelivered(Checkpoint checkpoint, long messages, boolean endsBatch) {
        return call(() -> {
            Partition partition = partitions.get(checkpoint.partitionId());
            if (partition != null && partition.delivered(checkpoint)) {
                count(partition, checkpoint, messages, endsBatch);
            }
            return null;
        });
    }

    boolean commitsOnRead() {
        return onRead;
    }

    Optional<AutoCommit> autoCommit() {
        return autoCommit;
    }

    private void shutDown(CompletableFuture<Void> result) {
        if (closed != null) {
            closed.whenComplete((ignored, error) -> complete(result, error));
            return;
        }
        closed = result;
        cancel(wake);
        cancel(interval);
        IggyClientException closing = closedError();
        waiters.forEach(waiter -> waiter.future.completeExceptionally(closing));
        waiters.clear();
        Set<Long> reading = new HashSet<>();
        partitions.values().stream().filter(partition -> partition.inFlight).forEach(partition -> {
            reading.add(partition.id);
        });
        // A cancelled read that stores keeps its partition held until the server answers, so the rewind goes after it.
        List.copyOf(reads).forEach(read -> read.cancel(false));
        countUncounted();
        CompletableFuture<Void> stores = onRead ? rewind(reading) : flushThen();
        stores.handle((ignored, error) -> error)
                .thenCompose(storeError -> (transport.group()
                                ? transport.leave()
                                : CompletableFuture.<Void>completedFuture(null))
                        .handle((ignored, leaveError) -> storeError == null ? leaveError : storeError))
                .whenComplete((error, ignored) -> {
                    loop.shutdown();
                    complete(result, error);
                });
    }

    /** With {@code Auto}, the final store of each partition. */
    private CompletableFuture<Void> flushThen() {
        if (autoCommit.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> pending = flushing == null ? CompletableFuture.completedFuture(null) : flushing;
        return pending.handle((ignored, error) -> null).thenComposeAsync(ignored -> storeCounted(), loop);
    }

    /** Moves the offset of each partition with undelivered messages back to its last delivered message. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<Void> rewind(Set<Long> reading) {
        Map<Long, BigInteger> firstQueued = new HashMap<>();
        for (Entry entry : queue) {
            if (entry.batch != null && !entry.batch.messages().isEmpty()) {
                firstQueued.putIfAbsent(
                        entry.partition, entry.batch.messages().get(0).header().offset());
            }
        }
        queue.clear();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        List<Throwable> failures = new ArrayList<>();
        for (Partition partition : partitions.values()) {
            boolean undelivered = firstQueued.containsKey(partition.id)
                    || reading.contains(partition.id)
                    || (partition.received != null
                            && (partition.delivered == null
                                    || partition.received.compareTo(partition.delivered.offset()) > 0));
            if (!undelivered) {
                continue;
            }
            BigInteger offset;
            PartitionContext context;
            if (partition.delivered != null) {
                offset = partition.delivered.offset();
                context = partition.delivered.context();
            } else if (partition.baseline != null) {
                context = partition.context;
                if (partition.baseline.signum() == 0) {
                    chain = chain.thenCompose(ignored -> transport
                            .delete(partition.id, partition.context)
                            .exceptionally(error -> {
                                failures.add(unwrap(error));
                                return null;
                            }));
                    continue;
                }
                offset = partition.baseline.subtract(BigInteger.ONE);
            } else if (start.kind() == PollingKind.Next
                    && firstQueued.containsKey(partition.id)
                    && firstQueued.get(partition.id).signum() > 0) {
                offset = firstQueued.get(partition.id).subtract(BigInteger.ONE);
                context = partition.context;
            } else {
                log.warn("Cannot move the offset of partition {} back, no message of it was delivered", partition.id);
                continue;
            }
            chain = chain.thenCompose(
                    ignored -> transport.store(partition.id, offset, context).exceptionally(error -> {
                        failures.add(unwrap(error));
                        return null;
                    }));
        }
        return chain.thenCompose(ignored -> failures.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(failures.get(0)));
    }

    private CompletableFuture<Void> applySeek(Partition partition, PollingStrategy position) {
        partition.generation = ++lastGeneration;
        partition.stopped = null;
        partition.retryAt = 0;
        partition.lastingRefusals = 0;
        partition.received = null;
        partition.baseline = null;
        partition.delivered = null;
        partition.firstDelivered = null;
        partition.committed = null;
        partition.counted = null;
        partition.sinceStore = 0;
        partition.gapFrom = null;
        partition.held = null;
        partition.fromRead = position.context().isPresent();
        CompletableFuture<Void> stored = CompletableFuture.completedFuture(null);
        if (onRead && position.kind() == PollingKind.Offset && position.value().signum() > 0) {
            // The server stores forward only on a read, so a store moves the offset that the next read starts after.
            BigInteger before = position.value().subtract(BigInteger.ONE);
            partition.received = before;
            partition.position = PollingStrategy.next();
            stored = transport.store(partition.id, before, position.context().orElse(partition.context));
        } else {
            partition.position = position;
        }
        pump();
        return stored;
    }

    private void addPartition(long id) {
        Partition partition = new Partition(id);
        partition.generation = ++lastGeneration;
        partition.position = start;
        partition.fromRead = start.context().isPresent();
        partitions.put(id, partition);
    }

    private void publishPartitions() {
        partitionIds = List.copyOf(partitions.keySet());
    }

    /** Sends what the budget and the demand allow. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void pump() {
        if (closed != null) {
            return;
        }
        long now = System.nanoTime();
        if (transport.group() && !refreshing && due(now, refreshAt)) {
            refreshAssignment();
        }
        if (regroup && !grouping && !partitions.isEmpty() && due(now, regroupAt)) {
            regroup();
        }
        if (!grouping && !groups.isEmpty() && demanded()) {
            int budget = onRead ? groups.size() : slots - queue.size() - reads.size();
            int first = groupCursor;
            for (int turn = 0; turn < groups.size() && budget > 0; turn++) {
                int index = (first + turn) % groups.size();
                Map<Long, PollingStrategy> targets = readyTargets(groups.get(index), now);
                if (!targets.isEmpty()) {
                    read(targets, options.count(), onRead);
                    budget--;
                    groupCursor = index + 1;
                }
            }
        }
        scheduleWake(now);
    }

    /** Under {@code OnRead} the reader reads only for a waiting {@link #next()}, so it stores only what it hands over. */
    private boolean demanded() {
        waiters.removeIf(waiter -> waiter.future.isDone());
        return !onRead || (!waiters.isEmpty() && queue.isEmpty());
    }

    /** The partitions served last go last, since a read that reaches its count serves its first targets. */
    private Map<Long, PollingStrategy> readyTargets(List<Long> group, long now) {
        List<Partition> ready = new ArrayList<>();
        for (Long id : group) {
            Partition partition = partitions.get(id);
            if (partition != null
                    && !partition.inFlight
                    && partition.stopped == null
                    && partition.gapFrom == null
                    && due(now, partition.retryAt)) {
                ready.add(partition);
            }
        }
        ready.sort(Comparator.comparingLong(partition -> partition.servedAt));
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        ready.forEach(partition -> targets.put(partition.id, partition.position));
        return targets;
    }

    private void read(Map<Long, PollingStrategy> targets, long count, boolean commit) {
        Map<Long, Long> generations = new HashMap<>();
        targets.keySet().forEach(id -> {
            Partition partition = partitions.get(id);
            partition.inFlight = true;
            generations.put(id, partition.generation);
        });
        CompletableFuture<List<FetchedMessages>> read;
        try {
            read = commit
                    ? prepareRead(targets, generations, count)
                    : transport.read(
                            targets,
                            count,
                            false,
                            options.withMinCount(Math.min(count, options.minCount()))
                                    .deferredOptions());
        } catch (RuntimeException refused) {
            read = CompletableFuture.failedFuture(refused);
        }
        reads.add(read);
        CompletableFuture<List<FetchedMessages>> sent = read;
        read.whenCompleteAsync(
                (sections, error) -> {
                    reads.remove(sent);
                    if (closed != null) {
                        return;
                    }
                    generations.keySet().forEach(id -> {
                        Partition partition = partitions.get(id);
                        if (partition != null) {
                            partition.inFlight = false;
                        }
                    });
                    if (error == null) {
                        accept(targets, generations, commit, sections);
                    } else {
                        refuse(targets.keySet(), error);
                    }
                    applyPendingSeeks(generations.keySet());
                    handOver();
                    pump();
                },
                loop);
    }

    private CompletableFuture<List<FetchedMessages>> prepareRead(
            Map<Long, PollingStrategy> targets, Map<Long, Long> generations, long count) {
        List<CompletableFuture<Void>> probes = new ArrayList<>();
        targets.forEach((id, strategy) -> {
            Partition partition = partitions.get(id);
            if (strategy.kind() != PollingKind.Next || partition.received != null || partition.baseline != null) {
                return;
            }
            probes.add(transport
                    .read(
                            Map.of(id, strategy),
                            1,
                            false,
                            options.withMinCount(1).deferredOptions())
                    .thenAcceptAsync(sections -> acceptProbe(id, generations.get(id), sections), loop));
        });
        return CompletableFuture.allOf(probes.toArray(CompletableFuture[]::new))
                .thenComposeAsync(ignored -> commitPrepared(targets, generations, count), loop);
    }

    private void acceptProbe(long id, long generation, List<FetchedMessages> sections) {
        Partition partition = partitions.get(id);
        if (partition == null || partition.generation != generation) {
            return;
        }
        partition.retryAt = System.nanoTime() + retryIntervalNanos;
        for (FetchedMessages section : sections) {
            if (section.partitionId() != id) {
                continue;
            }
            if (section.status() != FetchedMessages.STATUS_OK) {
                refuseSection(partition, section.status(), System.nanoTime());
            } else {
                captureBaseline(partition, section, System.nanoTime());
            }
        }
    }

    private CompletableFuture<List<FetchedMessages>> commitPrepared(
            Map<Long, PollingStrategy> targets, Map<Long, Long> generations, long count) {
        if (closed != null || !demanded()) {
            return CompletableFuture.completedFuture(List.of());
        }
        Map<Long, PollingStrategy> prepared = new LinkedHashMap<>();
        targets.forEach((id, strategy) -> {
            Partition partition = partitions.get(id);
            if (readyToCommit(partition, generations.get(id), strategy)) {
                prepared.put(id, partition.position);
            }
        });
        if (prepared.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        CompletableFuture<List<FetchedMessages>> sent =
                transport.read(prepared, count, true, options.deferredOptions());
        reads.add(sent);
        sent.whenCompleteAsync((sections, error) -> reads.remove(sent), loop);
        return sent;
    }

    private boolean readyToCommit(Partition partition, long generation, PollingStrategy strategy) {
        return partition != null
                && partition.generation == generation
                && partition.pendingSeek == null
                && partition.stopped == null
                && (strategy.kind() != PollingKind.Next || partition.received != null || partition.baseline != null);
    }

    private void applyPendingSeeks(Set<Long> ids) {
        for (Long id : ids) {
            Partition partition = partitions.get(id);
            if (partition != null && partition.pendingSeek != null) {
                PollingStrategy position = partition.pendingSeek;
                CompletableFuture<Void> sought = partition.sought;
                partition.pendingSeek = null;
                partition.sought = null;
                queue.removeIf(entry -> entry.partition == id);
                applySeek(partition, position).whenComplete((ignored, error) -> complete(sought, error));
            }
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void accept(
            Map<Long, PollingStrategy> targets,
            Map<Long, Long> generations,
            boolean commit,
            List<FetchedMessages> sections) {
        Set<Long> answered = new HashSet<>();
        long now = System.nanoTime();
        for (FetchedMessages section : sections) {
            Partition partition = partitions.get(section.partitionId());
            answered.add(section.partitionId());
            Long generation = generations.get(section.partitionId());
            if (generation == null || !keeps(section.partitionId(), generation)) {
                continue;
            }
            if (commit && (partition == null || partition.generation != generation)) {
                // The server stored this read, so a partition that left the assignment still hands it over.
                receiveStored(section, generations.get(section.partitionId()));
                continue;
            }
            if (partition == null
                    || partition.generation != generations.getOrDefault(section.partitionId(), -1L)
                    || partition.pendingSeek != null) {
                continue;
            }
            Optional<IggyOperationException> unknown = section.recoveryError();
            if (unknown.isPresent()) {
                // A later read of the server shows what the lost read stored, and the gap check reads it again.
                transport.abandon(unknown.get().getOperationId());
                continue;
            }
            if (section.status() != FetchedMessages.STATUS_OK) {
                refuseSection(partition, section.status(), now);
                continue;
            }
            partition.lastingRefusals = 0;
            partition.context = section.context();
            receive(partition, section, commit, targets.get(section.partitionId()), now);
        }
        for (Long id : targets.keySet()) {
            Partition partition = partitions.get(id);
            if (!answered.contains(id) && partition != null && options.maxWait().isZero()) {
                partition.retryAt = now + retryIntervalNanos;
            }
        }
    }

    private void captureBaseline(Partition partition, FetchedMessages section, long now) {
        List<Message> messages = new ArrayList<>();
        Optional<DecodeFailure> malformed =
                BytesDeserializer.readMessagesUntilMalformed(Unpooled.wrappedBuffer(section.records()), messages);
        if (malformed.isPresent()) {
            partition.stopped = new ReadException(OptionalLong.of(partition.id), malformed.get(), Optional.empty());
            queue.addLast(new Entry(partition.id, partition.generation, null, partition.stopped));
        } else if (messages.isEmpty()) {
            partition.retryAt = now + retryIntervalNanos;
        } else {
            partition.baseline = messages.get(0).header().offset();
            partition.context = section.context();
            partition.retryAt = 0;
            partition.position = new PollingStrategy(PollingKind.Next, BigInteger.ZERO, Optional.of(section.context()));
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void receive(Partition partition, FetchedMessages section, boolean commit, PollingStrategy from, long now) {
        List<Message> messages = new ArrayList<>();
        Optional<DecodeFailure> malformed =
                BytesDeserializer.readMessagesUntilMalformed(Unpooled.wrappedBuffer(section.records()), messages);
        BigInteger expected = partition.received == null ? partition.baseline : partition.received.add(BigInteger.ONE);
        if (messages.isEmpty() && malformed.isEmpty()) {
            if (commit
                    && partition.gapFrom == null
                    && expected != null
                    && section.nextOffset().compareTo(expected) > 0) {
                partition.gapFrom = expected;
                partition.gapEnd = section.nextOffset();
                partition.held =
                        new PolledMessages(partition.id, section.currentOffset(), 0L, List.of(), section.context());
                partition.received = section.nextOffset().subtract(BigInteger.ONE);
                readGap(partition);
                return;
            }
            if (options.maxWait().isZero()) {
                partition.retryAt = now + retryIntervalNanos;
            }
            return;
        }
        if (!messages.isEmpty()) {
            partition.servedAt = ++serves;
            PolledMessages batch = new PolledMessages(
                    partition.id, section.currentOffset(), (long) messages.size(), messages, section.context());
            BigInteger first = messages.get(0).header().offset();
            BigInteger last = messages.get(messages.size() - 1).header().offset();
            if (commit && partition.gapFrom == null && expected != null && first.compareTo(expected) > 0) {
                // A read whose reply was lost stored up to here, so the messages in between are read again first.
                partition.gapFrom = expected;
                partition.gapEnd = first;
                partition.held = batch;
                partition.received = last;
                readGap(partition);
            } else if (partition.gapFrom != null && !commit) {
                acceptGap(partition, batch);
            } else {
                partition.received = last;
                queue.addLast(new Entry(partition.id, partition.generation, batch, null));
            }
            partition.position =
                    onRead ? PollingStrategy.next() : batch.continuation().orElse(from);
            partition.fromRead = !onRead;
        }
        malformed.ifPresent(failure -> {
            // Malformed framing isolates no message, so the bad one is the first after the healthy prefix.
            BigInteger bad = failure.offset()
                    .orElseGet(() -> messages.isEmpty()
                            ? section.nextOffset().subtract(BigInteger.valueOf(section.count()))
                            : messages.get(messages.size() - 1)
                                    .header()
                                    .offset()
                                    .add(BigInteger.ONE));
            PollingStrategy skip = new PollingStrategy(
                    PollingKind.Offset,
                    bad.add(BigInteger.ONE),
                    Optional.of(section.context()),
                    Optional.of(partition.id));
            partition.stopped = new ReadException(OptionalLong.of(partition.id), failure, Optional.of(skip));
            queue.addLast(new Entry(partition.id, partition.generation, null, partition.stopped));
        });
    }

    /** Queues the messages of a stored read of a partition that left the assignment. */
    private void receiveStored(FetchedMessages section, long generation) {
        if (section.status() != FetchedMessages.STATUS_OK
                || section.recoveryError().isPresent()) {
            return;
        }
        List<Message> messages = new ArrayList<>();
        BytesDeserializer.readMessagesUntilMalformed(Unpooled.wrappedBuffer(section.records()), messages);
        if (!messages.isEmpty()) {
            PolledMessages batch = new PolledMessages(
                    section.partitionId(),
                    section.currentOffset(),
                    (long) messages.size(),
                    messages,
                    section.context());
            queue.addLast(new Entry(section.partitionId(), generation, batch, null));
        }
    }

    /** Reads the messages between the last received one and the batch that a lost read moved past, without a store. */
    private void readGap(Partition partition) {
        BigInteger missing = partition.gapEnd.subtract(partition.gapFrom);
        long count = Math.min(
                options.count(),
                missing.min(BigInteger.valueOf(Integer.MAX_VALUE)).longValue());
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        targets.put(
                partition.id,
                new PollingStrategy(PollingKind.Offset, partition.gapFrom, Optional.of(partition.context)));
        read(targets, count, false);
    }

    private void acceptGap(Partition partition, PolledMessages read) {
        BigInteger heldFirst = partition.gapEnd;
        List<Message> missing = read.messages().stream()
                .filter(message -> message.header().offset().compareTo(heldFirst) < 0)
                .toList();
        if (!missing.isEmpty()) {
            queue.addLast(new Entry(
                    partition.id,
                    partition.generation,
                    new PolledMessages(
                            partition.id, read.currentOffset(), (long) missing.size(), missing, read.context()),
                    null));
            partition.gapFrom =
                    missing.get(missing.size() - 1).header().offset().add(BigInteger.ONE);
        }
        if (missing.isEmpty() || partition.gapFrom.compareTo(heldFirst) >= 0) {
            if (!partition.held.messages().isEmpty()) {
                queue.addLast(new Entry(partition.id, partition.generation, partition.held, null));
            }
            partition.gapFrom = null;
            partition.held = null;
        } else {
            readGap(partition);
        }
    }

    private void refuseSection(Partition partition, long status, long now) {
        int code = (int) status;
        if (partition.gapFrom != null && code == IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()) {
            giveUpGap(partition);
        }
        handleRefusal(partition, code, IggyServerException.fromTcpResponse(code, new byte[0]), now);
    }

    /** Another member owns the partition now, so the messages that the lost read stored are gone for this one. */
    private void giveUpGap(Partition partition) {
        ReadException lost = new ReadException(
                OptionalLong.of(partition.id),
                new IggyServerException(
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN,
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode(),
                        "Offsets " + partition.gapFrom + " to "
                                + partition.gapEnd.subtract(BigInteger.ONE)
                                + " were stored by a read whose reply was lost",
                        Optional.empty(),
                        Optional.empty()),
                Optional.empty());
        queue.addLast(new Entry(partition.id, partition.generation, null, lost));
        if (!partition.held.messages().isEmpty()) {
            queue.addLast(new Entry(partition.id, partition.generation, partition.held, null));
        }
        partition.gapFrom = null;
        partition.held = null;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void refuse(Set<Long> targets, Throwable error) {
        Throwable cause = unwrap(error);
        long now = System.nanoTime();
        if (cause instanceof CancellationException) {
            return;
        }
        if (cause instanceof IggyOperationException operation
                && operation.getErrorCode() == IggyErrorCode.FETCH_PROGRESS_UNCERTAIN) {
            // The next read of the server starts after what the lost read stored, and the gap check reads it again.
            transport.abandon(operation.getOperationId());
            return;
        }
        if (cause instanceof IggyServerException server) {
            int code = server.getRawErrorCode();
            if (code == IggyErrorCode.NOT_PRIMARY.getCode()) {
                regroup = true;
                return;
            }
            for (Long id : targets) {
                Partition partition = partitions.get(id);
                if (partition != null) {
                    handleRefusal(partition, code, server, now);
                }
            }
            return;
        }
        targets.forEach(id -> {
            Partition partition = partitions.get(id);
            if (partition != null) {
                partition.retryAt = now + retryIntervalNanos;
            }
        });
        log.warn("A read of partitions {} failed, the reader retries it: {}", targets, cause.getMessage());
        if (queue.isEmpty() || queue.peekLast().partition != WHOLE_READER) {
            queue.addLast(
                    new Entry(WHOLE_READER, 0, null, new ReadException(OptionalLong.empty(), cause, Optional.empty())));
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void handleRefusal(Partition partition, int code, IggyServerException error, long now) {
        if (code == IggyErrorCode.NOT_PRIMARY.getCode()) {
            regroup = true;
            return;
        }
        if (code == IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()) {
            refreshAt = now;
            partition.retryAt = now + retryIntervalNanos;
            return;
        }
        if (code == IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()
                || code == IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode()
                || code == IggyErrorCode.DEFERRED_POLL_CAPACITY_EXCEEDED.getCode()) {
            partition.retryAt = now + retryIntervalNanos;
            return;
        }
        if (code == IggyErrorCode.HISTORY_UNAVAILABLE.getCode() && !partition.fromRead) {
            log.warn("Partition {} has a new history, the reader reads it again", partition.id);
            regroup = true;
            partition.retryAt = now + retryIntervalNanos;
            return;
        }
        boolean stops = code == IggyErrorCode.HISTORY_UNAVAILABLE.getCode()
                || code == IggyErrorCode.INVALID_SIZE_BYTES.getCode()
                || ++partition.lastingRefusals >= LASTING_REFUSALS_BEFORE_STOP;
        ReadException failure = new ReadException(OptionalLong.of(partition.id), error, Optional.empty());
        if (stops) {
            partition.stopped = failure;
            queue.addLast(new Entry(partition.id, partition.generation, null, failure));
            return;
        }
        int shift = Math.min(partition.lastingRefusals - 1, MAX_BACKOFF_SHIFT);
        partition.retryAt = now + (retryIntervalNanos << shift);
        log.warn("Partition {} refused a read, the reader retries it: {}", partition.id, error.getMessage());
    }

    private void regroup() {
        grouping = true;
        regroup = false;
        transport
                .groups(List.copyOf(partitions.keySet()))
                .whenCompleteAsync(
                        (found, error) -> {
                            grouping = false;
                            if (closed != null) {
                                return;
                            }
                            if (error != null) {
                                regroup = true;
                                regroupAt = System.nanoTime() + retryIntervalNanos;
                                log.warn(
                                        "Failed to group the partitions of a reader, the reader tries again: {}",
                                        error.getMessage());
                                partitions.values().forEach(partition -> partition.retryAt = regroupAt);
                            } else {
                                groups = found;
                            }
                            pump();
                        },
                        loop);
    }

    private void refreshAssignment() {
        refreshing = true;
        transport
                .assignment()
                .thenCompose(assignment -> assignment.isPresent()
                        ? CompletableFuture.completedFuture(assignment)
                        : transport.join().thenCompose(ignored -> transport.assignment()))
                .whenCompleteAsync(
                        (assignment, error) -> {
                            refreshing = false;
                            refreshAt = System.nanoTime() + retryIntervalNanos;
                            if (closed != null) {
                                return;
                            }
                            if (error != null) {
                                log.warn(
                                        "Failed to read the group assignment of a reader, it tries again: {}",
                                        error.getMessage());
                            } else {
                                assignment.ifPresent(found -> assign(found.partitions()));
                            }
                            handOver();
                            pump();
                        },
                        loop);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void assign(List<Long> assigned) {
        Set<Long> next = new HashSet<>(assigned);
        boolean changed = false;
        for (Long id : List.copyOf(partitions.keySet())) {
            if (!next.contains(id)) {
                Partition revoked = partitions.remove(id);
                if (!onRead) {
                    queue.removeIf(entry -> entry.partition == id);
                } else if (revoked.gapFrom != null) {
                    giveUpGap(revoked);
                }
                if (revoked.counted != null && revoked.counted != revoked.stored) {
                    // The partition goes to another member, so its counted position is stored first.
                    retiring.add(new Retired(revoked.id, revoked.counted));
                }
                changed = true;
            }
        }
        for (Long id : assigned) {
            if (!partitions.containsKey(id)) {
                addPartition(id);
                changed = true;
            }
        }
        if (changed) {
            regroup = true;
            publishPartitions();
            if (!retiring.isEmpty()) {
                flush();
            }
        }
    }

    /** Hands the queued batches over to the waiting calls, in order. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void handOver() {
        while (!waiters.isEmpty() && !queue.isEmpty()) {
            Waiter waiting = waiters.pollFirst();
            CompletableFuture<PolledMessages> waiter = waiting.future;
            if (waiter.isDone()) {
                continue;
            }
            Entry entry = queue.pollFirst();
            if (!current(entry)) {
                waiters.addFirst(waiting);
                continue;
            }
            if (entry.error != null) {
                if (!waiter.completeExceptionally(entry.error)) {
                    queue.addFirst(entry);
                }
                continue;
            }
            if (!waiter.complete(entry.batch)) {
                queue.addFirst(entry);
                continue;
            }
            Partition partition = partitions.get(entry.partition);
            if (waiting.delivers && partition != null) {
                delivered(partition, entry.batch);
            }
        }
        if (onRead && !waiters.isEmpty() && queue.isEmpty()) {
            pump();
        }
    }

    /**
     * Whether an entry still counts. A seek or a new assignment of its partition drops it. A partition that left the
     * assignment keeps the reads that the server stored under {@code OnRead}.
     */
    private boolean current(Entry entry) {
        if (entry.partition == WHOLE_READER) {
            return true;
        }
        return keeps(entry.partition, entry.generation);
    }

    private boolean keeps(long partitionId, long generation) {
        Partition partition = partitions.get(partitionId);
        return (partition != null && partition.generation == generation)
                || (onRead && generation >= seekGenerations.getOrDefault(partitionId, 0L));
    }

    private void delivered(Partition partition, PolledMessages batch) {
        Checkpoint last = batch.checkpoint().orElseThrow();
        if (partition.firstDelivered == null) {
            partition.firstDelivered = batch.messages().get(0).header().offset();
        }
        partition.delivered = last;
        CommitAfter after = autoCommit.map(AutoCommit::after).orElse(CommitAfter.MARKED);
        if (after == CommitAfter.DELIVERY) {
            count(partition, last, batch.messages().size(), true);
        } else if (after == CommitAfter.PROCESSING) {
            uncounted = new Uncounted(
                    partition.id, partition.generation, last, batch.messages().size());
        }
    }

    private void countUncounted() {
        if (uncounted == null) {
            return;
        }
        Uncounted batch = uncounted;
        uncounted = null;
        Partition partition = partitions.get(batch.partition);
        if (partition != null && partition.generation == batch.generation) {
            count(partition, batch.checkpoint, batch.messages, true);
        }
    }

    private void count(Partition partition, Checkpoint checkpoint, long messages, boolean endsBatch) {
        if (autoCommit.isEmpty()) {
            return;
        }
        if (partition.counted == null || checkpoint.offset().compareTo(partition.counted.offset()) > 0) {
            partition.counted = checkpoint;
        }
        partition.sinceStore += messages;
        boolean due = autoCommit
                .get()
                .every()
                .map(every -> every instanceof CommitEvery.Message
                        || (every instanceof CommitEvery.Messages many && partition.sinceStore >= many.count())
                        || (every instanceof CommitEvery.Batch && endsBatch))
                .orElse(false);
        if (due) {
            flush();
        }
    }

    /** Stores the counted positions once the store in flight ends. */
    private void flush() {
        if (flushing != null) {
            flushAgain = true;
            return;
        }
        flushing = storeCounted();
        flushing.whenCompleteAsync(
                (ignored, error) -> {
                    flushing = null;
                    if (error != null) {
                        log.error(
                                "Failed to store the counted offsets of a reader: {}",
                                unwrap(error).getMessage());
                    }
                    if (flushAgain && closed == null) {
                        flushAgain = false;
                        flush();
                    }
                },
                loop);
    }

    /** Stores the retired positions, then each counted position ahead of its last store, one at a time. */
    private CompletableFuture<Void> storeCounted() {
        List<Retired> stores = new ArrayList<>(retiring);
        retiring.clear();
        for (Partition partition : partitions.values()) {
            if (partition.counted != null && partition.counted != partition.stored) {
                stores.add(new Retired(partition.id, partition.counted));
            }
        }
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        List<Throwable> failures = new ArrayList<>();
        for (Retired store : stores) {
            Checkpoint checkpoint = store.checkpoint;
            long sinceStore = Optional.ofNullable(partitions.get(store.partition))
                    .map(partition -> partition.sinceStore)
                    .orElse(0L);
            chain = chain.thenCompose(ignored -> transport
                    .store(store.partition, checkpoint.offset(), checkpoint.context())
                    .handleAsync(
                            (stored, error) -> {
                                if (error != null) {
                                    failures.add(unwrap(error));
                                    return null;
                                }
                                Partition partition = partitions.get(store.partition);
                                if (partition != null && partition.delivered(checkpoint)) {
                                    partition.stored = checkpoint;
                                    partition.sinceStore = Math.max(0, partition.sinceStore - sinceStore);
                                }
                                return null;
                            },
                            loop));
        }
        return chain.thenCompose(ignored -> failures.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(failures.get(0)));
    }

    private Partition delivering(Checkpoint checkpoint) {
        Partition partition = partitions.get(checkpoint.partitionId());
        if (partition == null || !partition.delivered(checkpoint)) {
            throw invalid("This reader did not deliver " + checkpoint + " in the current generation of its partition");
        }
        return partition;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void scheduleWake(long now) {
        long earliest = Long.MAX_VALUE;
        for (Partition partition : partitions.values()) {
            if (!partition.inFlight && partition.stopped == null && !due(now, partition.retryAt)) {
                earliest = Math.min(earliest, partition.retryAt - now);
            }
        }
        if (transport.group() && !refreshing) {
            earliest = Math.min(earliest, due(now, refreshAt) ? 0 : refreshAt - now);
        }
        if (regroup && !grouping) {
            earliest = Math.min(earliest, due(now, regroupAt) ? retryIntervalNanos : regroupAt - now);
        }
        if (earliest == Long.MAX_VALUE) {
            return;
        }
        // A wake that is due runs this pump now, so only a later one still covers the next turn.
        long pending = wake == null || wake.isDone() ? 0 : wake.getDelay(TimeUnit.NANOSECONDS);
        if (pending > 0 && pending <= earliest) {
            return;
        }
        cancel(wake);
        wake = loop.schedule(this::pump, earliest, TimeUnit.NANOSECONDS);
    }

    private void run(Runnable task) {
        try {
            loop.execute(task);
        } catch (RejectedExecutionException closedReader) {
            log.debug("The reader is closed: {}", closedReader.getMessage());
        }
    }

    private <T> CompletableFuture<T> call(Supplier<T> task) {
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            loop.execute(() -> {
                try {
                    if (closed != null) {
                        throw closedError();
                    }
                    result.complete(task.get());
                } catch (RuntimeException error) {
                    result.completeExceptionally(error);
                }
            });
        } catch (RejectedExecutionException closedReader) {
            result.completeExceptionally(closedError());
        }
        return result;
    }

    /** Whether the time {@code at} passed, where zero is no time. */
    private static boolean due(long now, long at) {
        return at == 0 || now - at >= 0;
    }

    private static void cancel(ScheduledFuture<?> timer) {
        if (timer != null) {
            timer.cancel(false);
        }
    }

    private static <T> void complete(CompletableFuture<T> future, Throwable error) {
        if (error == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(unwrap(error));
        }
    }

    private static Throwable unwrap(Throwable error) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static IggyClientException closedError() {
        return new IggyClientException("The partition reader is closed");
    }

    private static IggyValidationException invalid(String reason) {
        return new IggyValidationException(
                IggyErrorCode.INVALID_COMMAND,
                IggyErrorCode.INVALID_COMMAND.getCode(),
                reason,
                Optional.empty(),
                Optional.empty());
    }

    private static final class Partition {
        private final long id;
        private long generation;
        private PollingStrategy position;
        // The position came from a read, so it carries the history of the partition.
        private boolean fromRead;
        private boolean inFlight;
        private long retryAt;
        // The read turn that last returned messages of the partition.
        private long servedAt;
        private int lastingRefusals;
        private ReadException stopped;
        private PartitionContext context = PartitionContext.EMPTY;
        // The last offset that a read of the current generation returned.
        private BigInteger received;
        private BigInteger baseline;
        private BigInteger firstDelivered;
        private Checkpoint delivered;
        private BigInteger committed;
        // Under OnRead, the start of the messages that a lost read stored, and the batch read after them.
        private BigInteger gapFrom;
        private BigInteger gapEnd;
        private PolledMessages held;
        private PollingStrategy pendingSeek;
        private CompletableFuture<Void> sought;
        private Checkpoint counted;
        private Checkpoint stored;
        private long sinceStore;

        private Partition(long id) {
            this.id = id;
        }

        /** Whether this reader delivered {@code checkpoint} in the current generation of the partition. */
        private boolean delivered(Checkpoint checkpoint) {
            return delivered != null
                    && firstDelivered != null
                    && checkpoint.context().equals(delivered.context())
                    && checkpoint.offset().compareTo(firstDelivered) >= 0
                    && checkpoint.offset().compareTo(delivered.offset()) <= 0;
        }
    }

    private record Entry(long partition, long generation, PolledMessages batch, ReadException error) {}

    /** A waiting call, which records the batch that it takes as delivered unless an {@link IggyConsumer} does. */
    private record Waiter(CompletableFuture<PolledMessages> future, boolean delivers) {}

    private record Uncounted(long partition, long generation, Checkpoint checkpoint, long messages) {}

    private record Retired(long partition, Checkpoint checkpoint) {}
}
