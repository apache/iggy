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
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
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
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.apache.iggy.client.async.reader.ReaderSupport.cancel;
import static org.apache.iggy.client.async.reader.ReaderSupport.checkPoll;
import static org.apache.iggy.client.async.reader.ReaderSupport.closedError;
import static org.apache.iggy.client.async.reader.ReaderSupport.complete;
import static org.apache.iggy.client.async.reader.ReaderSupport.completeSeek;
import static org.apache.iggy.client.async.reader.ReaderSupport.due;
import static org.apache.iggy.client.async.reader.ReaderSupport.installedContext;
import static org.apache.iggy.client.async.reader.ReaderSupport.invalid;
import static org.apache.iggy.client.async.reader.ReaderSupport.leaveGroup;
import static org.apache.iggy.client.async.reader.ReaderSupport.needsCheck;
import static org.apache.iggy.client.async.reader.ReaderSupport.nextAfter;
import static org.apache.iggy.client.async.reader.ReaderSupport.outcome;
import static org.apache.iggy.client.async.reader.ReaderSupport.probe;
import static org.apache.iggy.client.async.reader.ReaderSupport.probed;
import static org.apache.iggy.client.async.reader.ReaderSupport.releaseUnknown;
import static org.apache.iggy.client.async.reader.ReaderSupport.skipPast;
import static org.apache.iggy.client.async.reader.ReaderSupport.transientRefusal;
import static org.apache.iggy.client.async.reader.ReaderSupport.undecoded;
import static org.apache.iggy.client.async.reader.ReaderSupport.unknownOutcome;
import static org.apache.iggy.client.async.reader.ReaderSupport.unwrap;

/**
 * Reads the partitions of a topic in batches, with routing, prefetch and the position of each partition inside.
 *
 * <ul>
 *   <li>A standalone consumer reads the partitions of its builder. A consumer group reads the assignment of its
 *       member: the reader joins the group, follows its assignment, and leaves it in {@link #closeAsync()}.
 *   <li>{@link #next()} returns the next batch of one partition, in offset order within one generation of the
 *       partition: from its first read, from each {@link #seek}, and from each new assignment to this member.
 *   <li>The commit policy of the options decides who stores offsets. With {@code Manual}, {@link #commit} stores the
 *       offset of a checkpoint. With {@code OnRead}, the server stores the offset of each read before the application
 *       takes its batch, the reader reads only when the application waits for a batch, and {@link #closeAsync()}
 *       moves the offset back to the last delivered message. A partition that leaves the assignment still hands over
 *       the reads that the server stored for it. The close never moves back the offset of a partition that left: the
 *       server refuses that store after the handoff, and a lower offset blocks the handoff. So the batches of a left
 *       partition that the application did not take before the close are lost for the group. After a read whose reply
 *       was lost, the reader reads again what the lost read stored and delivers it first. A crash, or a reader that
 *       stops without {@link #closeAsync()}, loses the batches that the reader held: up to the effective prefetch
 *       budget, which holds at least
 *       {@value PartitionReaderBuilder#MIN_PREFETCH_SLOTS} batches. {@code Manual} and {@code Auto} store offsets
 *       after delivery instead. With {@code Auto}, the reader counts each batch as its {@link CommitAfter} says, or
 *       each {@link #mark}, and stores the latest counted position at the interval or the event of the policy, before
 *       a rebalance takes a partition away, and in {@link #closeAsync()}.
 *   <li>The reader stays usable after a {@link ReadException} and retries by itself. {@code HistoryUnavailable} stops
 *       its partition until a {@link #seek} when the position came from an earlier read of a history that is gone. A
 *       refusal that a quick retry does not change, such as {@code Unauthorized}, is retried after a wait that doubles
 *       each time, and after six in a row it stops its partition until a {@link #seek} with one error. A refusal
 *       because another member owns a partition is not a leave: the partition keeps its state, names owner generation
 *       0, and asks for its assignment. It reads again after a wait that doubles each time, and six such refusals in
 *       a row are reported once. After such a refusal of an offset write of a partition that stays assigned, the
 *       reader reads the partition once for the owner that the server installed. If that read serves the incarnation
 *       of the write, the write goes once more for that owner. {@link #closeAsync()} ends that read for a seek write
 *       and for an automatic store, and their refusal stands. A cancel of a {@link #commit} ends that read for its
 *       write. The writes of {@link #closeAsync()} read their owner until a cancel of the first call, and at most
 *       until 10 s after the close started. A read in flight at that end can still get an answer, but that answer
 *       leads to no write.
 * </ul>
 *
 * <p>The futures of a reader complete on its own thread, so a callback must not block.
 */
public final class PartitionReader implements AutoCloseable {
    // A client admits 64 deferred requests, so one reader leaves the other half to the other readers.
    static final int MAX_READS_IN_FLIGHT = 32;
    // The owner reads of the writes of a close end this long after the close starts, so a refusal that stays transient
    // cannot hold the close.
    static final Duration CLOSE_BUDGET = Duration.ofSeconds(10);

    private static final Logger log = LoggerFactory.getLogger(PartitionReader.class);
    private static final AtomicLong READERS = new AtomicLong();
    private static final int LASTING_REFUSALS_BEFORE_STOP = 6;
    private static final int MAX_BACKOFF_SHIFT = 5;
    private static final long WHOLE_READER = -1;
    // A join moves ownership only after the partition logs persist the new owner, a few milliseconds later.
    private static final Duration ASSIGNMENT_SETTLE_PAUSE = Duration.ofMillis(10);

    private final ReaderTransport transport;
    private final List<Long> standalonePartitions;
    private final PollingStrategy start;
    private final PollOptions options;
    private final int slots;
    private final long retryIntervalNanos;
    private final boolean onRead;
    private final Optional<AutoCommit> autoCommit;
    private final ScheduledThreadPoolExecutor loop;
    // The reader is the caller of a seek write and of an automatic store, so the close ends the read of their owner.
    private final BooleanSupplier untilClose;
    // The close belongs to its first caller, so a write of the close reads its owner until that caller cancels or the
    // close budget ends. The close sets closeDeadline and closed before its first write.
    private final BooleanSupplier untilCloseBudget;
    // Ends with the close. Every close call waits for it, so the cancel of one call ends no other.
    private final CompletableFuture<Void> closeEnded = new CompletableFuture<>();

    // Everything below is read and written on the loop only, except the volatile fields.
    private final NavigableMap<Long, Partition> partitions = new TreeMap<>();
    private final Map<Long, Long> seekGenerations = new HashMap<>();
    private volatile List<Long> partitionIds = List.of();
    private final Deque<Entry> queue = new ArrayDeque<>();
    private final Deque<Waiter> waiters = new ArrayDeque<>();
    private final Set<CompletableFuture<?>> reads = new HashSet<>();
    // The probes in flight besides the first of each read: a read sends its probes before its own request.
    private int extraProbes;
    // The gap reads and checks in flight, each of one partition. They leave a request for the read of each group.
    private final Set<CompletableFuture<?>> aloneReads = new HashSet<>();
    // The partition of the last gap read or check. The next turn starts after it, so the limit does not favor low ids.
    private long aloneCursor = Long.MIN_VALUE;
    // The committing requests of the reads in flight, which the close cancels with them.
    private final Set<CompletableFuture<List<FetchedMessages>>> sends = new HashSet<>();
    // The committing reads of each partition, of any generation, until the client released them. A seek waits for them.
    private final Map<Long, Integer> unreleased = new HashMap<>();
    private final List<Retired> retiring = new ArrayList<>();
    private List<List<Long>> groups = List.of();
    private boolean regroup = true;
    private boolean grouping;
    private long regroupAt;
    private int groupCursor;
    private long serves;
    private boolean refreshing;
    private long refreshAt;
    private long refreshPause;
    // The start joins before the first sync.
    private boolean joinedSinceSync = true;
    private long lastGeneration;
    private Uncounted uncounted;
    private CompletableFuture<Void> flushing;
    private boolean flushAgain;
    private ScheduledFuture<?> wake;
    private ScheduledFuture<?> interval;
    // The future of the first close call.
    private volatile CompletableFuture<Void> closed;
    // When the close budget ends, set once at the close start.
    private volatile long closeDeadline;

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
        this.untilClose = () -> closed == null;
        this.untilCloseBudget = () -> !closed.isCancelled() && System.nanoTime() - closeDeadline < 0;
    }

    /** Starts the builder of a reader that sends its requests through {@code transport}. */
    public static PartitionReaderBuilder builder(ReaderTransport transport) {
        return new PartitionReaderBuilder(transport);
    }

    /**
     * The next batch of one partition. Never empty, and it has no time limit. Cancel safe: a cancelled call leaves its
     * batch in the reader. Fails with a {@link ReadException} for an error of a read. Once the reader is closed, it
     * fails at once.
     */
    public CompletableFuture<PolledMessages> next() {
        return waitForBatch(true);
    }

    /**
     * The next batch, without recording it as delivered: an {@link IggyConsumer} hands its messages over one by one
     * and records each with {@link #deliverMessage}.
     */
    CompletableFuture<PolledMessages> nextBatch() {
        return waitForBatch(false);
    }

    /** Records one message that an {@link IggyConsumer} handed over, unless an earlier assignment read it. */
    CompletableFuture<Void> deliverMessage(Checkpoint checkpoint) {
        return call(() -> {
            Partition partition = partitions.get(checkpoint.partitionId());
            if (partition != null && partition.owners.named(checkpoint.context())) {
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
     * Stores the offset of a checkpoint that this reader delivered. A checkpoint that it did not deliver in the current
     * generation of its partition, as one of an older generation, stores nothing, and so does a checkpoint at or below
     * the last commit of the generation. A store whose outcome stays unknown
     * fails the commit, and the reader releases it in the client, so a retry can store. After a refusal as not the
     * owner, the commit reads the owner until its caller cancels the returned future or the reader stops, and then the
     * refusal stands.
     *
     * @return a future that fails with {@link IggyErrorCode#INVALID_COMMAND} under {@code OnRead}, and for a
     *         partition that this reader does not read, or with the error of the store
     */
    public CompletableFuture<Void> commit(Checkpoint checkpoint) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        BooleanSupplier retries = () -> !result.isCancelled() && !loop.isShutdown();
        this.<CompletableFuture<Void>>call(() -> {
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
                    long generation = partition.generation;
                    PartitionContext context = writeContext(partition, checkpoint.context());
                    return owned(partition, checkpoint.offset(), context, ReaderTransport::store, retries)
                            .exceptionallyCompose(error -> releaseUnknown(transport, error))
                            .thenRun(() -> run(() -> recordCommit(checkpoint, generation)));
                })
                .thenCompose(store -> store)
                .whenComplete((ignored, error) -> complete(result, error));
        return result;
    }

    /**
     * Sets the next read position of {@code partitionId} and starts a new generation of it. It drops the queued
     * batches of the partition and ignores its late replies, and it resumes a stopped partition. Under {@code OnRead},
     * a seek writes the stored offset first and moves the partition only once its writes succeed. It waits for each
     * committing read in flight of the partition, also one of an earlier assignment, and for an earlier seek of it, and
     * the partition starts no read until the seek ends.
     * A seek to an offset above 0 stores the offset before it, so the next read of the server starts there. A read
     * never moves the stored offset back. So if the server holds an offset, a seek to offset 0, also by {@code First},
     * deletes it. A seek to a timestamp first reads its first message without a store, then seeks to the offset of
     * that message. Without such a message, it stores nothing. A failed read, store, lookup or delete fails the seek
     * and leaves the position and the queued batches of the partition as they were. A write whose outcome stays unknown
     * fails it too, and the reader then reads again what that write could skip. So a seek of a partition that has not
     * read yet first looks up the stored offset. A cancel before the first write stops the seek. After that write, the
     * seek ends in the background, and a later seek and the close wait for it. A failed write there is logged.
     *
     * @return a future that fails with {@link IggyErrorCode#INVALID_COMMAND} for a partition that the reader does
     *         not read, for a continuation of another partition, and for a partition that leaves the assignment
     *         before the seek ends
     */
    public CompletableFuture<Void> seek(long partitionId, PollingStrategy start) {
        return seek(partitionId, start, moved -> {});
    }

    /**
     * {@link #seek}, which runs {@code applied} on the reader thread with whether the seek moved the partition, once it
     * dropped the queued batches or failed.
     */
    CompletableFuture<Void> seek(long partitionId, PollingStrategy start, Consumer<Boolean> applied) {
        start.requirePartition(partitionId);
        // The caller gets this future itself, so its cancel reaches the seek.
        CompletableFuture<Void> sought = new CompletableFuture<>();
        this.<Void>call(() -> {
                    Partition partition = partitions.get(partitionId);
                    if (partition == null) {
                        applied.accept(false);
                        throw invalid("This reader does not read partition " + partitionId);
                    }
                    PollingStrategy position = start.kind() == PollingKind.First
                            ? new PollingStrategy(PollingKind.Offset, BigInteger.ZERO, start.context())
                            : start;
                    if (!onRead) {
                        move(partition, position);
                        applied.accept(true);
                        pump();
                        sought.complete(null);
                        return null;
                    }
                    partition.seeks.addLast(new Seek(position, applied, sought));
                    applyPendingSeeks(List.of(partition));
                    return null;
                })
                .exceptionally(error -> {
                    sought.completeExceptionally(error);
                    return null;
                });
        return sought;
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
     * delivered message. A partition that delivered none moves before its first undelivered batch. If that batch starts
     * at offset 0 and the server holds an offset, the close deletes it. After a seek write with an unknown outcome, the
     * close restores the offset from before that write until a batch arrives. It stores that offset again, or deletes
     * the offset when there was none. A delivered message or a lost read from before that write decides instead, also a
     * read that the close cancels. But if the partition took its start without a stored offset, only a delivered message
     * or such a cancelled read decides, and otherwise the close deletes the offset, so a restart takes the start again.
     * A batch of an earlier assignment decides nothing. A group reader then leaves its group. A store or delete whose
     * outcome stays unknown fails the close, which releases it in the client for a later reader. The close belongs to
     * its first caller: a cancel of the future of the first call ends the owner reads of the writes of the close, and
     * so does the end of the close budget, 10 s after the close started. Their refusal then stands, and each write of
     * the close that was not sent yet still goes once. The leave and the stop of the reader still run. A later call
     * completes when the close ends, and its cancel does not end the close.
     *
     * @return a future that fails with the first failed store
     */
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            loop.execute(() -> shutDown(result));
        } catch (RejectedExecutionException alreadyClosed) {
            if (closed == null) {
                result.complete(null);
            } else {
                closeEnded.whenComplete((ignored, error) -> complete(result, error));
            }
        }
        return result;
    }

    /**
     * {@link #closeAsync()} and waits for it. As in the blocking reader, an interrupt of the waiting thread cancels the
     * future of this call, and this call throws {@link IggyClientException} with the interrupt flag still set. If this
     * call started the close, the close then ends as after a cancel of its first caller.
     */
    @Override
    public void close() {
        ReaderSupport.awaitClose(closeAsync());
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
            ReaderSupport.joinGroup(transport)
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

    /** A close fails the waiting calls, and a later call fails through {@link #call}, also once the loop stopped. */
    private CompletableFuture<PolledMessages> waitForBatch(boolean delivers) {
        CompletableFuture<PolledMessages> result = new CompletableFuture<>();
        this.<Void>call(() -> {
                    if (delivers) {
                        countUncounted();
                    }
                    waiters.addLast(new Waiter(result, delivers));
                    handOver();
                    pump();
                    return null;
                })
                .exceptionally(error -> {
                    result.completeExceptionally(error);
                    return null;
                });
        return result;
    }

    private void shutDown(CompletableFuture<Void> result) {
        closeEnded.whenComplete((ignored, error) -> complete(result, error));
        if (closed != null) {
            return;
        }
        closeDeadline = System.nanoTime() + CLOSE_BUDGET.toNanos();
        closed = result;
        cancel(wake);
        cancel(interval);
        IggyClientException closing = closedError();
        waiters.forEach(waiter -> waiter.future.completeExceptionally(closing));
        waiters.clear();
        // Only a committing read can have stored, and a probe or a lookup before it cannot.
        Set<Long> reading = new HashSet<>();
        partitions.values().stream()
                .filter(partition -> partition.inFlight && unreleased.containsKey(partition.id))
                .forEach(partition -> reading.add(partition.id));
        List.copyOf(reads).forEach(read -> read.cancel(false));
        List.copyOf(sends).forEach(send -> send.cancel(false));
        countUncounted();
        // The server can store until it answers the cancel of a committing read, so the stores wait for that reply. A
        // read whose outcome stays unknown would block its partition with no reader left to recover it.
        CompletableFuture<Void> stores = onRead
                ? transport
                        .cancelledReadsSettled()
                        .thenComposeAsync(ignored -> transport.abandonLost(), loop)
                        .thenComposeAsync(ignored -> seeksSettled(), loop)
                        .thenComposeAsync(ignored -> seekThenRewind(reading), loop)
                : flushThen();
        stores.handle((ignored, error) -> error)
                .thenCompose(storeError ->
                        leaveGroup(transport).thenApply(leaveError -> storeError == null ? leaveError : storeError))
                .whenComplete((error, ignored) -> {
                    loop.shutdown();
                    complete(closeEnded, error);
                });
    }

    /** With {@code Auto}, the final store of each partition. */
    private CompletableFuture<Void> flushThen() {
        if (autoCommit.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> pending = flushing == null ? CompletableFuture.completedFuture(null) : flushing;
        return pending.handle((ignored, error) -> null)
                .thenComposeAsync(ignored -> storeCounted(untilCloseBudget), loop);
    }

    /**
     * Ends once the seek in progress of each partition ends, failed or not. A seek that waits for one writes only
     * after it, or a delete would remove its store.
     */
    private CompletableFuture<Void> seeksSettled() {
        return CompletableFuture.allOf(partitions.values().stream()
                .map(partition -> partition.seeking)
                .filter(seeking -> seeking != null)
                .map(seeking -> seeking.handle((ignored, error) -> null))
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Runs the seeks that wait for a read in flight, one after another with their writes, then moves the partitions
     * back. A partition that a seek moved has nothing to move back.
     */
    private CompletableFuture<Void> seekThenRewind(Set<Long> reading) {
        List<CompletableFuture<Void>> seeks = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        for (Partition partition : partitions.values()) {
            CompletableFuture<Void> previous = CompletableFuture.completedFuture(null);
            for (Seek seek : List.copyOf(partition.seeks)) {
                // A cancel ends only the future of the caller, so the next seek waits for the end of the seek itself.
                previous = previous.thenCompose(ignored -> applySeek(partition, seek))
                        .handle((ignored, error) -> {
                            if (error == null) {
                                // The seek wrote after the cancelled reads settled, so it replaced what they stored.
                                reading.remove(partition.id);
                            } else if (!(unwrap(error) instanceof CancellationException)) {
                                failures.add(unwrap(error));
                            }
                            return null;
                        });
            }
            seeks.add(previous);
        }
        return CompletableFuture.allOf(seeks.toArray(CompletableFuture[]::new))
                .thenComposeAsync(ignored -> rewind(reading), loop)
                .handle((ignored, rewindError) -> failures.isEmpty() ? rewindError : failures.get(0))
                .thenCompose(error -> outcome(error));
    }

    /** Moves the offset of each partition with undelivered messages back to its last delivered message. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<Void> rewind(Set<Long> reading) {
        Map<Long, BigInteger> firstQueued = new HashMap<>();
        for (Entry entry : queue) {
            Partition owner = partitions.get(entry.partition);
            // A batch of an earlier assignment decides nothing: another member can have moved the offset since.
            if (owner != null && entry.batch != null && entry.generation == owner.generation) {
                firstQueued.putIfAbsent(
                        entry.partition, entry.batch.messages().get(0).header().offset());
            }
        }
        queue.clear();
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        List<Throwable> failures = new ArrayList<>();
        for (Partition partition : partitions.values()) {
            Checkpoint delivered = partition.delivered;
            boolean undelivered = firstQueued.containsKey(partition.id)
                    || reading.contains(partition.id)
                    || (partition.received != null
                            && (delivered == null || partition.received.compareTo(delivered.offset()) > 0))
                    || (partition.lost != null
                            && (delivered == null || partition.lost.from().compareTo(delivered.offset()) > 0));
            if (!undelivered) {
                continue;
            }
            BigInteger before = partition.baseline != null
                    ? partition.baseline
                    : partition.lost != null ? partition.lost.from() : firstQueued.get(partition.id);
            Supplier<CompletableFuture<Void>> move;
            if (delivered != null) {
                PartitionContext context = writeContext(partition, delivered.context());
                move = () -> owned(partition, delivered.offset(), context, ReaderTransport::store, untilCloseBudget);
            } else if (partition.restorePoint != null && partition.received == null) {
                // No batch decides the rewind, so the close undoes the seek write that can have moved the offset. A
                // read that the close cancelled can have stored before that write, so it decides as a lost read does.
                BigInteger point = reading.contains(partition.id) && before != null ? before : partition.restorePoint;
                PartitionContext context = writeContext(partition, partition.context);
                move = () -> owned(partition, point, context, ReaderSupport::restoreBefore, untilCloseBudget);
            } else if (before != null) {
                PartitionContext context = writeContext(
                        partition,
                        partition.baseline == null && partition.lost != null
                                ? partition.lost.context()
                                : partition.context);
                // At offset 0, or at a start offset without a batch, the server can hold no offset that a read stored.
                ReaderSupport.OffsetWrite write =
                        before.signum() == 0 || (partition.received == null && partition.startFoundNoOffset)
                                ? ReaderSupport::storeBeforeIfStored
                                : ReaderSupport::storeBefore;
                move = () -> owned(partition, before, context, write, untilCloseBudget);
            } else {
                log.warn("Cannot move the offset of partition {} back, no message of it was delivered", partition.id);
                continue;
            }
            chain = chain.thenCompose(ignored -> move.get()
                    .exceptionallyCompose(error -> releaseUnknown(transport, error))
                    .exceptionally(error -> {
                        failures.add(unwrap(error));
                        return null;
                    }));
        }
        return chain.thenCompose(ignored -> failures.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(failures.get(0)));
    }

    /**
     * Writes the stored offset of a seek under {@code OnRead}, then moves its partition. The partition starts no read
     * until the seek ends, so no read reaches the server before the writes, and a failed write leaves it as it was. The
     * future ends with the seek and fails with its error.
     */
    private CompletableFuture<Void> applySeek(Partition partition, Seek seek) {
        // Each step runs as a stage, so a request that throws fails the seek instead of holding the partition.
        CompletableFuture<Void> ended = ReaderSupport.startSeek(seek.sought(), seek.position())
                .thenComposeAsync(
                        position -> position.kind() == PollingKind.Timestamp
                                ? checkPoll(transport, options, partition.id, position, Duration.ZERO)
                                        .thenApply(polled -> probed(position, polled))
                                : CompletableFuture.completedFuture(position),
                        loop)
                .thenComposeAsync(found -> write(partition, found, seek).thenApply(ignored -> found), loop)
                .handleAsync(
                        (found, error) -> {
                            partition.seeking = null;
                            // A partition that left the assignment failed its seeks already.
                            if (partition.seeks.peekFirst() == seek) {
                                partition.seeks.pollFirst();
                                if (error == null) {
                                    move(partition, found);
                                }
                                endSeek(partition, seek, error);
                            }
                            return error;
                        },
                        loop)
                .thenCompose(error -> outcome(error));
        partition.seeking = ended;
        return ended;
    }

    /**
     * Moves the stored offset before an offset target, so the next read of the server starts there. A read never
     * moves the stored offset back, and no offset comes before 0, so a target of 0 deletes the stored offset if the
     * server holds one. A seek that the caller cancels before this write stops here. Once sent, the write goes on.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private CompletableFuture<Void> write(Partition partition, PollingStrategy target, Seek seek) {
        boolean offset = target.kind() == PollingKind.Offset;
        boolean deletes = offset && target.value().signum() == 0;
        // After a write with an unknown outcome, only the offset from before the write tells where a position that
        // follows the stored offset reads next, and what the close restores when nothing else decides it.
        boolean looksUp = offset
                && (deletes || partition.startUnresolved || expected(partition) == null || noClosePoint(partition));
        PartitionContext context = writeContext(partition, target.context().orElse(partition.context));
        return (looksUp
                        ? transport.storedOffset(partition.id)
                        : CompletableFuture.completedFuture(Optional.<ConsumerOffsetInfo>empty()))
                .thenCompose(before -> seek.sought().isCancelled()
                        ? ReaderSupport.<Void>cancelledSeek()
                        : !offset || (deletes && before.isEmpty())
                                ? CompletableFuture.completedFuture(null)
                                : owned(partition, target.value(), context, ReaderSupport::storeBefore, untilClose)
                                        .exceptionallyComposeAsync(
                                                error -> abandonUnknown(partition, before, error), loop));
    }

    /**
     * A write whose outcome stays unknown can have moved the stored offset, and it blocks its partition on the client
     * until abandoned. So the seek abandons it before it ends, and the next read catches up what a move ahead skipped.
     * The partition belongs to the generation of the write, so a partition that left and came back stays as it is.
     */
    private CompletableFuture<Void> abandonUnknown(
            Partition partition, Optional<ConsumerOffsetInfo> before, Throwable error) {
        return ReaderSupport.<Void>releaseUnknown(transport, error)
                .whenCompleteAsync(
                        (ignored, failure) -> {
                            if (unknownOutcome(error)) {
                                if (partition.startFoundNoOffset || noClosePoint(partition)) {
                                    partition.restorePoint =
                                            partition.startFoundNoOffset ? BigInteger.ZERO : nextAfter(before);
                                }
                                pin(partition, before);
                                lose(partition, System.nanoTime());
                            }
                        },
                        loop);
    }

    /**
     * Gives a position without a known next offset the offset that the server held before a seek write with an unknown
     * outcome. A start without a stored offset stays the start, and the close then restores the missing offset.
     */
    private void pin(Partition partition, Optional<ConsumerOffsetInfo> before) {
        if (partition.startUnresolved) {
            takeLookup(partition, before);
        }
        BigInteger next = nextAfter(before);
        if (partition.position.kind() == PollingKind.Next && expected(partition) == null) {
            partition.baseline = next;
            // The write can have moved the stored offset back as well as ahead, so the next read names its offset.
            partition.position = PollingStrategy.offset(next);
        }
        remember(partition, Optional.ofNullable(expected(partition)).orElse(next), partition.context);
    }

    /**
     * Starts a new generation of the partition at {@code target}, without its queued batches and late replies. Under
     * {@code OnRead} the writes of the seek moved the stored offset before an offset target.
     */
    private void move(Partition partition, PollingStrategy target) {
        seekGenerations.put(partition.id, ++lastGeneration);
        queue.removeIf(entry -> entry.partition == partition.id);
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
        partition.lost = null;
        partition.backlogged = false;
        partition.startUnresolved = false;
        partition.startFoundNoOffset = false;
        partition.restorePoint = null;
        partition.fromRead = target.context().isPresent();
        partition.position = partition.owners.fence(target);
        if (onRead && target.kind() == PollingKind.Offset) {
            target.context().ifPresent(context -> partition.context = partition.owners.take(context));
            partition.baseline = target.value();
            if (target.value().signum() > 0) {
                partition.position = PollingStrategy.next();
            }
        }
    }

    private void endSeek(Partition partition, Seek seek, Throwable error) {
        seek.applied().accept(error == null);
        if (closed == null) {
            applyPendingSeeks(List.of(partition));
            pump();
        }
        completeSeek(seek.sought(), partition.id, error);
    }

    private void addPartition(long id) {
        Partition partition = new Partition(id);
        partition.generation = ++lastGeneration;
        takeStart(partition);
        partitions.put(id, partition);
    }

    private void takeStart(Partition partition) {
        partition.position = partition.owners.fence(start);
        partition.fromRead = start.context().isPresent();
        partition.startUnresolved = onRead && start.kind() != PollingKind.Next;
        partition.startFoundNoOffset = false;
        partition.restorePoint = null;
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
        // Every gap read and every poll of a position that a fetch does not take starts here, so each partition waits
        // for its turn.
        Map<Long, Partition> after = partitions.tailMap(aloneCursor, false);
        Map<Long, Partition> before = partitions.headMap(aloneCursor, true);
        for (Map<Long, Partition> turn : List.of(after, before)) {
            for (Partition partition : turn.values()) {
                if (partition.inFlight
                        || partition.misrouted
                        || !partition.seeks.isEmpty()
                        || partition.stopped != null
                        || !due(now, partition.retryAt)) {
                    continue;
                }
                if (partition.gapFrom != null) {
                    readGap(partition);
                } else if (needsCheck(partition.position) && !partition.startUnresolved && roomAlone()) {
                    check(partition);
                }
            }
        }
        // A lookup only refreshes the groups, so the reads go on with the last good ones while it runs.
        if (!groups.isEmpty() && demanded()) {
            int dataRoom = slots - queue.size() - reads.size();
            readGroups(Math.min(dataRoom, MAX_READS_IN_FLIGHT - requestsInFlight()), now);
        }
        scheduleWake(now);
    }

    /** The requests of the reads in flight, each probe included. */
    private int requestsInFlight() {
        return reads.size() + extraProbes;
    }

    /**
     * Every group with ready partitions gets a read, those without a read that may wait in flight first. The slots left
     * spread the backlogged partitions of each group over reads of their own, each with the whole limits. Such a read
     * returns at once, so each slot keeps a full read in flight, and the quiet partitions share the one read that may
     * wait. Under {@code OnRead} that read stores, so a group holds one at a time, and a waiting call fills the slots.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void readGroups(int budget, long now) {
        int first = groupCursor;
        List<GroupPlan<Partition>> plans = new ArrayList<>();
        for (boolean waiting : new boolean[] {false, true}) {
            for (int turn = 0; turn < groups.size(); turn++) {
                int index = (first + turn) % groups.size();
                List<Long> group = groups.get(index);
                boolean parked = group.stream()
                        .map(partitions::get)
                        .anyMatch(partition -> partition != null && partition.inFlight && partition.parked);
                List<Partition> ready = parked == waiting ? ready(group, now, parked) : List.of();
                if (!ready.isEmpty() && plans.size() < budget) {
                    plans.add(new GroupPlan<>(index, ready, partition -> partition.backlogged));
                }
            }
        }
        int extra = GroupPlan.deal(plans, budget - plans.size());
        // A lookup of the start that finds a stored offset leads to a probe.
        Predicate<Partition> probes =
                partition -> onRead && (partition.startUnresolved || needsProbe(partition, partition.position));
        for (GroupPlan<Partition> plan : plans) {
            List<Partition> waiting = plan.waiting();
            if (!waiting.isEmpty()) {
                extra -= GroupPlan.fitProbes(waiting, extra, probes);
                read(targetsOf(waiting), options.count(), onRead, true);
            }
            for (List<Partition> targets : plan.spread()) {
                extra -= GroupPlan.fitProbes(targets, extra, probes);
                read(targetsOf(targets), options.count(), onRead, false);
            }
            groupCursor = plan.index() + 1;
        }
    }

    private static boolean needsProbe(Partition partition, PollingStrategy strategy) {
        return strategy.kind() == PollingKind.Next && partition.received == null && partition.baseline == null;
    }

    /** Under {@code OnRead} the reader reads only for a waiting {@link #next()}, so it stores only what it hands over. */
    private boolean demanded() {
        waiters.removeIf(waiter -> waiter.future.isDone());
        return !onRead || (!waiters.isEmpty() && queue.isEmpty());
    }

    /**
     * The partitions served last go last, since a read that reaches its count serves its first targets. Under
     * {@code OnRead} a quiet partition waits for the read of the group that may wait, since a second one would store.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private List<Partition> ready(List<Long> group, long now, boolean parked) {
        List<Partition> ready = new ArrayList<>();
        for (Long id : group) {
            Partition partition = partitions.get(id);
            if (partition != null
                    && !partition.inFlight
                    && !partition.misrouted
                    && partition.seeks.isEmpty()
                    && partition.stopped == null
                    && partition.gapFrom == null
                    && (partition.startUnresolved || !needsCheck(partition.position))
                    && due(now, partition.retryAt)
                    && (partition.backlogged || !onRead || !parked)) {
                ready.add(partition);
            }
        }
        ready.sort(Comparator.comparingLong(partition -> partition.servedAt));
        return ready;
    }

    private static Map<Long, PollingStrategy> targetsOf(List<Partition> ready) {
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        ready.forEach(partition -> targets.put(partition.id, partition.position));
        return targets;
    }

    private CompletableFuture<List<FetchedMessages>> read(
            Map<Long, PollingStrategy> targets, long count, boolean commit, boolean parked) {
        Map<Long, Long> generations = new HashMap<>();
        // A partition that leaves and comes back gets a new state, which another read may hold.
        List<Partition> held = new ArrayList<>();
        targets.keySet().forEach(id -> {
            Partition partition = partitions.get(id);
            partition.inFlight = true;
            partition.parked = parked;
            generations.put(id, partition.generation);
            held.add(partition);
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
                    aloneReads.remove(sent);
                    if (closed != null) {
                        return;
                    }
                    held.forEach(partition -> partition.inFlight = false);
                    if (error == null) {
                        accept(targets, generations, commit, sections);
                    } else {
                        refuse(generations, error);
                    }
                    applyPendingSeeks(held);
                    handOver();
                    pump();
                },
                loop);
        return sent;
    }

    private CompletableFuture<List<FetchedMessages>> prepareRead(
            Map<Long, PollingStrategy> targets, Map<Long, Long> generations, long count) {
        List<CompletableFuture<Void>> probes = new ArrayList<>();
        targets.forEach((id, strategy) -> {
            Partition partition = partitions.get(id);
            if (partition.startUnresolved) {
                probes.add(resolveStart(partition, generations.get(id)));
            } else if (needsProbe(partition, strategy)) {
                probes.add(probeStart(id, generations.get(id), strategy));
            }
        });
        int extra = Math.max(0, probes.size() - 1);
        extraProbes += extra;
        return CompletableFuture.allOf(probes.toArray(CompletableFuture[]::new))
                .whenCompleteAsync(
                        (ignored, error) -> {
                            extraProbes -= extra;
                            if (extra > 0) {
                                pump();
                            }
                        },
                        loop)
                .thenComposeAsync(ignored -> commitPrepared(targets, generations, count), loop);
    }

    /**
     * Under {@code OnRead} a start other than Next applies only to a partition without a stored offset, so the first
     * read looks the offset up. A partition with one reads as with a Next start, probe included. A failed lookup fails
     * as a read of the partition does.
     */
    private CompletableFuture<Void> resolveStart(Partition partition, long generation) {
        return transport
                .storedOffset(partition.id)
                .handleAsync(
                        (stored, error) -> {
                            // A partition that left the assignment and came back reads with a state of its own.
                            if (partitions.get(partition.id) != partition) {
                                return false;
                            }
                            if (error != null) {
                                refuse(Map.of(partition.id, generation), error);
                                return false;
                            }
                            takeLookup(partition, stored);
                            return stored.isPresent();
                        },
                        loop)
                .thenCompose(resumes -> resumes
                        ? probeStart(partition.id, generation, partition.position)
                        : CompletableFuture.completedFuture(null));
    }

    private static void takeLookup(Partition partition, Optional<ConsumerOffsetInfo> stored) {
        partition.startUnresolved = false;
        partition.startFoundNoOffset = stored.isEmpty();
        if (stored.isPresent()) {
            partition.position = PollingStrategy.next();
            partition.fromRead = false;
        } else if (partition.position.kind() == PollingKind.Offset) {
            // The first read stores from the start offset on, so the close moves back before it.
            partition.baseline = partition.position.value();
        }
    }

    /**
     * Polls the first message at a position that a fetch does not take, apart from the reads of the groups. The
     * partition then reads from that message, so under {@code OnRead} the close moves back before it. Without a
     * message, the partition polls again after the retry interval.
     */
    private void check(Partition partition) {
        partition.inFlight = true;
        long generation = partition.generation;
        CompletableFuture<PolledMessages> polled =
                checkPoll(transport, options, partition.id, partition.position, options.maxWait());
        reads.add(polled);
        aloneReads.add(polled);
        aloneCursor = partition.id;
        polled.whenCompleteAsync(
                (found, error) -> {
                    reads.remove(polled);
                    aloneReads.remove(polled);
                    if (closed != null) {
                        return;
                    }
                    partition.inFlight = false;
                    // A partition that a seek moved meanwhile reads from its new position.
                    boolean current = partitions.get(partition.id) == partition && partition.generation == generation;
                    if (current && error != null) {
                        refuse(Map.of(partition.id, generation), error);
                    } else if (current && found.messages().isEmpty()) {
                        partition.lastingRefusals = 0;
                        partition.retryAt = System.nanoTime() + retryIntervalNanos;
                    } else if (current) {
                        partition.lastingRefusals = 0;
                        partition.position = partition.owners.fence(probed(partition.position, found));
                        partition.fromRead = true;
                        partition.context = partition.owners.take(found.context());
                        if (onRead && partition.baseline == null) {
                            partition.baseline = partition.position.value();
                        }
                    }
                    applyPendingSeeks(List.of(partition));
                    handOver();
                    pump();
                },
                loop);
    }

    private CompletableFuture<Void> probeStart(long id, long generation, PollingStrategy strategy) {
        return probe(transport, options, id, strategy)
                .thenAcceptAsync(sections -> acceptProbe(id, generation, sections), loop);
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
        targets.keySet().forEach(id -> {
            Partition partition = partitions.get(id);
            if (readyToCommit(partition, generations.get(id))) {
                prepared.put(id, partition.position);
            }
        });
        if (prepared.isEmpty()) {
            return CompletableFuture.completedFuture(List.of());
        }
        CompletableFuture<List<FetchedMessages>> sent =
                transport.read(prepared, count, true, options.deferredOptions());
        sends.add(sent);
        prepared.keySet().forEach(id -> unreleased.merge(id, 1, Integer::sum));
        sent.whenCompleteAsync(
                (sections, error) -> {
                    sends.remove(sent);
                    release(prepared.keySet(), sections, error);
                },
                loop);
        return sent;
    }

    /**
     * Ends a committing read of {@code ids} once the client released it. A lost read blocks its partitions on the
     * client until abandoned, also after a seek or a revoke, so it ends with its abandon.
     */
    private void release(Set<Long> ids, List<FetchedMessages> sections, Throwable error) {
        Map<Long, Long> lost = new HashMap<>();
        if (unwrap(error) instanceof IggyOperationException operation
                && operation.getErrorCode() == IggyErrorCode.FETCH_PROGRESS_UNCERTAIN) {
            ids.forEach(id -> lost.put(id, operation.getOperationId()));
        } else if (sections != null) {
            sections.forEach(section -> section.recoveryError()
                    .ifPresent(unknown -> lost.put(section.partitionId(), unknown.getOperationId())));
        }
        Map<Long, CompletableFuture<Void>> abandons = new HashMap<>();
        ids.forEach(id -> Optional.ofNullable(lost.get(id))
                .map(operation -> abandons.computeIfAbsent(operation, transport::abandon))
                .orElse(CompletableFuture.completedFuture(null))
                .whenCompleteAsync((ignored, abandonError) -> released(id), loop));
    }

    private void released(long id) {
        unreleased.computeIfPresent(id, (key, left) -> left > 1 ? left - 1 : null);
        Partition partition = partitions.get(id);
        if (partition != null && closed == null) {
            applyPendingSeeks(List.of(partition));
        }
    }

    private boolean readyToCommit(Partition partition, long generation) {
        return partition != null
                && partition.generation == generation
                && partition.seeks.isEmpty()
                && partition.stopped == null
                && !partition.startUnresolved
                && !needsCheck(partition.position)
                && !needsProbe(partition, partition.position);
    }

    /** Starts the first waiting seek of each partition once no read and no other seek of it is in flight. */
    private void applyPendingSeeks(List<Partition> held) {
        for (Partition partition : held) {
            if (partitions.get(partition.id) == partition
                    && !partition.inFlight
                    && !unreleased.containsKey(partition.id)
                    && partition.seeking == null
                    && !partition.seeks.isEmpty()) {
                applySeek(partition, partition.seeks.peekFirst());
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
            Optional<IggyOperationException> unknown = section.recoveryError();
            if (generation == null || !keeps(section.partitionId(), generation)) {
                continue;
            }
            if (commit && (partition == null || partition.generation != generation)) {
                // The server stored this read, so a partition that left the assignment still hands it over.
                receiveStored(section, generations.get(section.partitionId()));
                continue;
            }
            if (partition == null || partition.generation != generations.getOrDefault(section.partitionId(), -1L)) {
                continue;
            }
            if (unknown.isPresent()) {
                // A later read of the server shows what the lost read stored, and the gap check reads it again.
                lose(partition, now);
                continue;
            }
            if (section.status() != FetchedMessages.STATUS_OK) {
                refuseSection(partition, section.status(), now);
                continue;
            }
            partition.lastingRefusals = 0;
            if (!commit && partition.gapFrom != null && ReaderSupport.unserved(section)) {
                partition.retryAt = now + retryIntervalNanos;
                continue;
            }
            partition.context = partition.owners.take(section.context());
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
            BigInteger bad = undecoded(section, malformed.get(), messages);
            partition.stopped = new ReadException(
                    OptionalLong.of(partition.id),
                    malformed.get(),
                    Optional.of(skipPast(partition.id, bad, section.context())));
            queue.addLast(new Entry(partition.id, partition.generation, null, partition.stopped));
        } else if (messages.isEmpty()) {
            partition.retryAt = now + retryIntervalNanos;
        } else {
            partition.baseline = messages.get(0).header().offset();
            partition.context = partition.owners.take(section.context());
            partition.retryAt = 0;
            partition.position = new PollingStrategy(PollingKind.Next, BigInteger.ZERO, Optional.of(partition.context));
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void receive(Partition partition, FetchedMessages section, boolean commit, PollingStrategy from, long now) {
        List<Message> messages = new ArrayList<>();
        Optional<DecodeFailure> malformed =
                BytesDeserializer.readMessagesUntilMalformed(Unpooled.wrappedBuffer(section.records()), messages);
        // A read that ends before the head returns at once. An empty partition reports the head 0.
        partition.backlogged = section.nextOffset().compareTo(section.currentOffset()) <= 0
                && (section.count() > 0 || section.currentOffset().signum() > 0);
        BigInteger expected = expected(partition);
        // Only a lost read explains a later start. Without one, the application or another session stored it.
        boolean catchesUp = commit && partition.gapFrom == null && partition.lost != null && expected != null;
        if (messages.isEmpty() && malformed.isEmpty()) {
            if (catchesUp && section.nextOffset().compareTo(expected) > 0) {
                partition.gapFrom = expected;
                partition.gapEnd = section.nextOffset();
                partition.held =
                        new PolledMessages(partition.id, section.currentOffset(), 0L, List.of(), section.context());
                partition.received = section.nextOffset().subtract(BigInteger.ONE);
                return;
            }
            if (!commit && partition.gapFrom != null) {
                skipGap(partition, section.nextOffset(), now);
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
            if (catchesUp && first.compareTo(expected) > 0) {
                // A read whose reply was lost stored up to here, so the messages in between are read again first.
                partition.gapFrom = expected;
                partition.gapEnd = first;
                partition.held = batch;
                partition.received = last;
            } else if (partition.gapFrom != null && !commit) {
                acceptGap(partition, batch);
            } else {
                if (commit && from.kind() == PollingKind.Next) {
                    // A read from Next starts at the stored offset, so its store fences out the lost read.
                    partition.lost = null;
                }
                partition.received = last;
                queue.addLast(new Entry(partition.id, partition.generation, batch, null));
            }
            partition.position =
                    onRead ? PollingStrategy.next() : batch.continuation().orElse(from);
            partition.fromRead = !onRead;
        }
        malformed.ifPresent(failure -> {
            BigInteger bad = undecoded(section, failure, messages);
            if (commit) {
                // The server stored the whole read, so the close moves the offset back before the bad message.
                remember(partition, bad, section.context());
            }
            partition.stopped = new ReadException(
                    OptionalLong.of(partition.id),
                    failure,
                    Optional.of(skipPast(partition.id, bad, section.context())));
            queue.addLast(new Entry(partition.id, partition.generation, null, partition.stopped));
        });
    }

    /**
     * A committing read whose outcome is unknown can have stored from the expected offset on. The close moves the
     * offset back over it, a later read catches up a forward gap, and the partition waits the retry interval.
     */
    private void lose(Partition partition, long now) {
        remember(partition, expected(partition), partition.context);
        partition.retryAt = now + retryIntervalNanos;
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
        if (!roomAlone()) {
            // The gap stays open, and its turn comes again when a read ends.
            return;
        }
        BigInteger missing = partition.gapEnd.subtract(partition.gapFrom);
        long count = Math.min(
                options.count(),
                missing.min(BigInteger.valueOf(Integer.MAX_VALUE)).longValue());
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        targets.put(
                partition.id,
                new PollingStrategy(PollingKind.Offset, partition.gapFrom, Optional.of(partition.context)));
        aloneReads.add(read(targets, count, false, false));
        aloneCursor = partition.id;
    }

    /** Whether a gap read or a check can start. Each group keeps a request for its read. */
    private boolean roomAlone() {
        return aloneReads.size() < Math.max(MAX_READS_IN_FLIGHT - groups.size(), 1)
                && requestsInFlight() < MAX_READS_IN_FLIGHT;
    }

    private void acceptGap(Partition partition, PolledMessages read) {
        loseGapBefore(partition, read.messages().get(0).header().offset());
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
            closeGap(partition);
        }
    }

    /** An empty gap read: the messages before its next offset are gone. */
    private void skipGap(Partition partition, BigInteger next, long now) {
        loseGapBefore(partition, next);
        if (partition.gapFrom.compareTo(partition.gapEnd) >= 0) {
            closeGap(partition);
        } else if (options.maxWait().isZero()) {
            partition.retryAt = now + retryIntervalNanos;
        }
    }

    /** Reports once that the messages of the gap before {@code start} are gone, as after retention, and skips them. */
    private void loseGapBefore(Partition partition, BigInteger start) {
        BigInteger end = start.min(partition.gapEnd);
        if (end.compareTo(partition.gapFrom) > 0) {
            ReadException lost = ReaderSupport.lostRange(partition.id, partition.gapFrom, end);
            queue.addLast(new Entry(partition.id, partition.generation, null, lost));
            partition.gapFrom = end;
        }
    }

    private void closeGap(Partition partition) {
        if (!partition.held.messages().isEmpty()) {
            queue.addLast(new Entry(partition.id, partition.generation, partition.held, null));
        }
        partition.gapFrom = null;
        partition.held = null;
    }

    private void refuseSection(Partition partition, long status, long now) {
        int code = (int) status;
        handleRefusal(partition, code, IggyServerException.fromTcpResponse(code, new byte[0]), now);
    }

    /** Applies the error of a read to each partition that it read, unless a seek or an assignment replaced it. */
    private void refuse(Map<Long, Long> generations, Throwable error) {
        Throwable cause = unwrap(error);
        long now = System.nanoTime();
        List<Partition> targets = generations.keySet().stream()
                .map(partitions::get)
                .filter(partition -> partition != null && partition.generation == generations.get(partition.id))
                .toList();
        if (cause instanceof CancellationException || targets.isEmpty()) {
            return;
        }
        if (cause instanceof IggyOperationException operation
                && operation.getErrorCode() == IggyErrorCode.FETCH_PROGRESS_UNCERTAIN) {
            // The next read of the server starts after what the lost read stored, and the gap check reads it again.
            targets.forEach(partition -> lose(partition, now));
            return;
        }
        if (cause instanceof IggyServerException server) {
            targets.forEach(partition -> handleRefusal(partition, server.getRawErrorCode(), server, now));
            return;
        }
        targets.forEach(partition -> partition.retryAt = now + retryIntervalNanos);
        log.warn("A read of partitions {} failed, the reader retries it: {}", generations.keySet(), cause.getMessage());
        if (queue.isEmpty() || queue.peekLast().partition != WHOLE_READER) {
            queue.addLast(
                    new Entry(WHOLE_READER, 0, null, new ReadException(OptionalLong.empty(), cause, Optional.empty())));
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void handleRefusal(Partition partition, int code, IggyServerException error, long now) {
        if (code == IggyErrorCode.NOT_PRIMARY.getCode()) {
            // The groups name the old primary until the next lookup, so a read before it would go there again.
            partition.misrouted = true;
            regroup = true;
            return;
        }
        if (code == IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()) {
            // A new session and a move away and back look the same here. Only a leave that a refresh shows drops what
            // the close needs, so the partition keeps it and its position.
            loseOwner(partition);
            refreshAt = now;
            int refusals = ++partition.lastingRefusals;
            partition.retryAt = now + backoff(refusals);
            if (refusals >= LASTING_REFUSALS_BEFORE_STOP) {
                partition.lastingRefusals = 0;
                queue.addLast(new Entry(
                        partition.id,
                        partition.generation,
                        null,
                        new ReadException(OptionalLong.of(partition.id), error, Optional.empty())));
            }
            return;
        }
        if (transientRefusal(code)) {
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
        partition.retryAt = now + backoff(partition.lastingRefusals);
        log.warn("Partition {} refused a read, the reader retries it: {}", partition.id, error.getMessage());
    }

    /**
     * The wait after the {@code refusals}-th refusal of a read in a row: the retry interval, doubled for each refusal
     * before it, and at most 32 times the retry interval.
     */
    private long backoff(int refusals) {
        return retryIntervalNanos << Math.min(refusals - 1, MAX_BACKOFF_SHIFT);
    }

    private void regroup() {
        grouping = true;
        regroup = false;
        CompletableFuture<List<List<Long>>> lookup;
        try {
            lookup = transport.groups(List.copyOf(partitions.keySet()));
        } catch (RuntimeException refused) {
            // A throw before the send fails as a failed lookup does, so grouping cannot stay set.
            lookup = CompletableFuture.failedFuture(refused);
        }
        lookup.whenCompleteAsync(
                (found, error) -> {
                    grouping = false;
                    if (closed != null) {
                        return;
                    }
                    // Also after a failed lookup: while lookups fail, a refused partition tries its last group once
                    // per lookup.
                    partitions.values().forEach(partition -> partition.misrouted = false);
                    if (error != null) {
                        regroup = true;
                        regroupAt = System.nanoTime() + retryIntervalNanos;
                        log.warn(
                                "Failed to group the partitions of a reader, the reader tries again: {}",
                                error.getMessage());
                    } else {
                        groups = found;
                    }
                    pump();
                },
                loop);
    }

    private void refreshAssignment() {
        refreshing = true;
        ReaderSupport.assignment(transport, () -> closed != null, this::loseMembership, loop)
                .whenCompleteAsync(
                        (assignment, error) -> {
                            refreshing = false;
                            long now = System.nanoTime();
                            refreshAt = now + retryIntervalNanos;
                            if (closed != null) {
                                return;
                            }
                            if (error != null) {
                                log.warn(
                                        "Failed to read the group assignment of a reader, it tries again: {}",
                                        error.getMessage());
                            } else if (assignment.isPresent()) {
                                boolean changed = assign(assignment.get().partitions());
                                // Only the partitions of this member count: a hand-over between other members also
                                // moves the generation.
                                refreshPause = joinedSinceSync || changed
                                        ? Math.min(ASSIGNMENT_SETTLE_PAUSE.toNanos(), retryIntervalNanos)
                                        : Math.min(2 * refreshPause, retryIntervalNanos);
                                joinedSinceSync = false;
                                refreshAt = now + refreshPause;
                            }
                            handOver();
                            pump();
                        },
                        loop);
    }

    /**
     * After a lost membership, as after a lost session, its owners are gone, and no partition that left can store. The
     * refusals came from the lost membership, so each partition counts them again and reads at once.
     */
    private void loseMembership() {
        joinedSinceSync = true;
        retiring.clear();
        partitions.values().forEach(partition -> {
            loseOwner(partition);
            partition.lastingRefusals = 0;
            partition.retryAt = 0;
        });
    }

    /** The server installs the owner again under a higher generation, so the partition names owner generation 0. */
    private void loseOwner(Partition partition) {
        partition.context = partition.owners.lose(partition.position, partition.context);
        partition.position = partition.owners.fence(partition.position);
    }

    /** Applies the partitions of this member and returns whether they changed. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private boolean assign(List<Long> assigned) {
        Set<Long> next = new HashSet<>(assigned);
        boolean changed = false;
        for (Long id : List.copyOf(partitions.keySet())) {
            if (!next.contains(id)) {
                Partition revoked = partitions.remove(id);
                failSeeks(revoked, invalid("Partition " + id + " left the assignment of this reader"));
                if (!onRead) {
                    queue.removeIf(entry -> entry.partition == id);
                } else if (revoked.gapFrom != null) {
                    // Another member owns the partition now, so what the lost read stored is gone for this one.
                    loseGapBefore(revoked, revoked.gapEnd);
                    closeGap(revoked);
                }
                boolean counted = revoked.counted != null && revoked.counted != revoked.stored;
                if (counted && !revoked.owners.lost(revoked.context)) {
                    // The partition goes to another member, so its owner stores its counted position first.
                    retiring.add(new Retired(revoked, revoked.counted));
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
        return changed;
    }

    /** Hands the queued batches to the waiting calls, in order. A batch of an earlier assignment decides nothing. */
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
            if (waiting.delivers && partition != null && entry.generation == partition.generation) {
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
        flushing = storeCounted(untilClose);
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
    private CompletableFuture<Void> storeCounted(BooleanSupplier retries) {
        List<Retired> stores = new ArrayList<>(retiring);
        retiring.clear();
        for (Partition partition : partitions.values()) {
            if (partition.counted != null && partition.counted != partition.stored) {
                stores.add(new Retired(partition, partition.counted));
            }
        }
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        List<Throwable> failures = new ArrayList<>();
        for (Retired store : stores) {
            Checkpoint checkpoint = store.checkpoint;
            PartitionContext context = writeContext(store.partition, checkpoint.context());
            long sinceStore = Optional.ofNullable(partitions.get(store.partition.id))
                    .map(partition -> partition.sinceStore)
                    .orElse(0L);
            chain = chain.thenCompose(ignored -> owned(
                            store.partition, checkpoint.offset(), context, ReaderTransport::store, retries)
                    .exceptionallyCompose(error -> releaseUnknown(transport, error))
                    .handleAsync(
                            (stored, error) -> {
                                if (error != null) {
                                    failures.add(unwrap(error));
                                    return null;
                                }
                                Partition partition = partitions.get(store.partition.id);
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

    /** The context of an offset write of {@code partition} from {@code source}, as {@link PartitionOwners#write}. */
    private PartitionContext writeContext(Partition partition, PartitionContext source) {
        return partition.owners.write(source, partition.context, transport.metadataWatermark());
    }

    /** {@link ReaderSupport#ownedWrite} of {@code partition}, which reads its owner while {@code retries} holds. */
    private CompletableFuture<Void> owned(
            Partition partition,
            BigInteger offset,
            PartitionContext context,
            ReaderSupport.OffsetWrite write,
            BooleanSupplier retries) {
        return ReaderSupport.ownedWrite(
                transport, partition.id, offset, context, write, () -> installedOwner(partition, retries));
    }

    /**
     * The context of the owner that the server installed for {@code partition}, after a refusal of a write as not the
     * owner. The reader takes the owner as lost, as after such a refusal of a read, and asks for its assignment. A
     * partition that left the assignment keeps the refusal, and one that came back has a state of its own. So the
     * context is empty for it, and once the reader stopped.
     */
    private CompletableFuture<Optional<PartitionContext>> installedOwner(Partition partition, BooleanSupplier retries) {
        try {
            return CompletableFuture.supplyAsync(
                            () -> {
                                boolean stayed = partitions.get(partition.id) == partition;
                                if (stayed) {
                                    loseOwner(partition);
                                    refreshAt = System.nanoTime();
                                    pump();
                                }
                                return stayed;
                            },
                            loop)
                    .thenCompose(stayed -> stayed
                            ? installedContext(transport, options, partition.id, loop, retryIntervalNanos, retries)
                            : CompletableFuture.completedFuture(Optional.empty()));
        } catch (RejectedExecutionException stopped) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
    }

    private void recordCommit(Checkpoint checkpoint, long generation) {
        Partition partition = partitions.get(checkpoint.partitionId());
        if (partition != null
                && partition.generation == generation
                && (partition.committed == null || checkpoint.offset().compareTo(partition.committed) > 0)) {
            partition.committed = checkpoint.offset();
        }
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

    /** The offset that the next read of the partition starts at, or null before its first read. */
    private static BigInteger expected(Partition partition) {
        return partition.received == null ? partition.baseline : partition.received.add(BigInteger.ONE);
    }

    private static boolean noClosePoint(Partition partition) {
        return partition.delivered == null && partition.received == null && partition.lost == null;
    }

    /** Keeps the lowest start of what the server stored without handing it over. */
    private static void remember(Partition partition, BigInteger from, PartitionContext context) {
        if (from != null && (partition.lost == null || from.compareTo(partition.lost.from()) < 0)) {
            partition.lost = new Lost(from, context);
        }
    }

    private static void failSeeks(Partition partition, RuntimeException error) {
        for (Seek seek : partition.seeks) {
            seek.applied().accept(false);
            seek.sought().completeExceptionally(error);
        }
        partition.seeks.clear();
    }

    private record Entry(long partition, long generation, PolledMessages batch, ReadException error) {}

    /** A seek under {@code OnRead}, which runs {@code applied} and completes {@code sought} once it moved or failed. */
    record Seek(PollingStrategy position, Consumer<Boolean> applied, CompletableFuture<Void> sought) {}

    /** A waiting call, which records the batch that it takes as delivered unless an {@link IggyConsumer} does. */
    private record Waiter(CompletableFuture<PolledMessages> future, boolean delivers) {}

    private record Uncounted(long partition, long generation, Checkpoint checkpoint, long messages) {}

    private record Retired(Partition partition, Checkpoint checkpoint) {}

    record Lost(BigInteger from, PartitionContext context) {}
}
