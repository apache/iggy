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

import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Reads one message at a time from a {@link PartitionReader}: a standalone consumer over its partitions, or a
 * consumer group member over its assignment. The commit policy of the reader options applies per message:
 *
 * <ul>
 *   <li>{@link #next()}: under {@link CommitAfter#DELIVERY} a message counts when it is returned, under
 *       {@link CommitAfter#PROCESSING} when the next one is asked for or at {@link #shutdown()}.
 *   <li>{@link #consume}: under {@link CommitAfter#DELIVERY} a message counts before its handler runs, under
 *       {@link CommitAfter#PROCESSING} once its handler succeeds. A handler that fails leaves its message uncounted and
 *       ends the loop with its error.
 *   <li>Under {@link CommitAfter#MARKED} only {@link #mark} counts. Under {@code OnRead} the server stores each read,
 *       and {@link #shutdown()} moves the offset back to the last message handed over.
 * </ul>
 *
 * <p>One caller at a time calls {@link #next()} or runs {@link #consume}.
 */
public final class IggyConsumer {
    private final PartitionReader reader;
    private final CommitAfter after;
    private final Deque<Held> held = new ArrayDeque<>();
    // The partitions of the seeks that the reader has not applied yet, each with its latest seek and its batches. A
    // seek that moves drops them, and a failed one hands them over.
    private final Map<Long, Parked> parked = new HashMap<>();
    private long seeks;
    private ReceivedMessage uncounted;
    private CompletableFuture<ReceivedMessage> waiting;
    private CompletableFuture<PolledMessages> reading;
    private boolean waitingProcesses;
    private volatile boolean stopping;
    private CompletableFuture<Void> loop = CompletableFuture.completedFuture(null);

    private IggyConsumer(PartitionReader reader) {
        this.reader = reader;
        this.after = reader.autoCommit().map(AutoCommit::after).orElse(CommitAfter.MARKED);
    }

    /** Builds the reader of {@code builder} and a consumer on it, a group member once it joined its group. */
    public static CompletableFuture<IggyConsumer> build(PartitionReaderBuilder builder) {
        return builder.build().thenApply(IggyConsumer::new);
    }

    /**
     * The next message. Fails with a {@link ReadException} for an error of a read, after which the consumer goes on.
     */
    public synchronized CompletableFuture<ReceivedMessage> next() {
        countUncounted();
        return take(true);
    }

    /**
     * Runs {@code handler} on each message, one after another, until {@link #shutdown()} or the first failure of a
     * handler or a read.
     *
     * @return a future that ends with the shutdown, or fails with the first error
     */
    public synchronized CompletableFuture<Void> consume(Function<ReceivedMessage, CompletableFuture<Void>> handler) {
        countUncounted();
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop = done;
        step(handler, done);
        return done;
    }

    /** See {@link PartitionReader#commit}. */
    public CompletableFuture<Void> commit(Checkpoint checkpoint) {
        return reader.commit(checkpoint);
    }

    /** See {@link PartitionReader#mark}. */
    public CompletableFuture<Void> mark(Checkpoint checkpoint) {
        return reader.mark(checkpoint);
    }

    /**
     * See {@link PartitionReader#seek}. The consumer holds the messages of the partition back until the seek ends. A
     * seek that moves drops them, and a failed one hands them over.
     */
    public synchronized CompletableFuture<Void> seek(long partitionId, PollingStrategy start) {
        // A refused seek changes nothing, so the batches stay with an earlier seek of the partition.
        start.requirePartition(partitionId);
        if (uncounted != null && uncounted.partitionId() == partitionId) {
            uncounted = null;
        }
        long seek = ++seeks;
        Parked earlier = parked.get(partitionId);
        List<Held> kept = earlier == null ? new ArrayList<>() : earlier.batches();
        parked.put(partitionId, new Parked(seek, kept));
        for (Iterator<Held> batches = held.iterator(); batches.hasNext(); ) {
            Held next = batches.next();
            if (next.batch.partitionId() == partitionId) {
                kept.add(next);
                batches.remove();
            }
        }
        return reader.seek(partitionId, start, moved -> applied(partitionId, seek, moved));
    }

    /** The partitions of the reader. */
    public List<Long> partitions() {
        return reader.partitions();
    }

    /**
     * Stops a running {@link #consume} after its current handler, counts the last message under
     * {@link CommitAfter#PROCESSING}, and closes the reader, which stores, moves offsets back and leaves its group as
     * its commit policy says. A cancel of the returned future cancels that close, as {@link PartitionReader#closeAsync()}
     * describes.
     */
    public CompletableFuture<Void> shutdown() {
        stopping = true;
        CompletableFuture<Void> running;
        synchronized (this) {
            if (waiting != null && !waiting.isDone()) {
                waiting.completeExceptionally(new IggyClientException("The consumer is shut down"));
            }
            running = loop;
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        running.handle((ignored, error) -> null)
                .thenCompose(ignored -> {
                    synchronized (this) {
                        countUncounted();
                    }
                    CompletableFuture<Void> closing = reader.closeAsync();
                    result.whenComplete((done, error) -> {
                        if (result.isCancelled()) {
                            closing.cancel(false);
                        }
                    });
                    return closing;
                })
                .whenComplete((ignored, error) -> ReaderSupport.complete(result, error));
        return result;
    }

    private void step(Function<ReceivedMessage, CompletableFuture<Void>> handler, CompletableFuture<Void> done) {
        // Held messages and quick handlers complete at once, so the loop goes on here instead of nesting callbacks.
        while (!stopping) {
            CompletableFuture<Void> handled = handleNext(handler);
            if (!handled.isDone()) {
                handled.whenComplete((ignored, error) -> {
                    if (stopping) {
                        done.complete(null);
                    } else if (error != null) {
                        done.completeExceptionally(unwrap(error));
                    } else {
                        step(handler, done);
                    }
                });
                return;
            }
            if (handled.isCompletedExceptionally()) {
                handled.whenComplete((ignored, error) -> done.completeExceptionally(unwrap(error)));
                return;
            }
        }
        done.complete(null);
    }

    private CompletableFuture<Void> handleNext(Function<ReceivedMessage, CompletableFuture<Void>> handler) {
        CompletableFuture<ReceivedMessage> taken;
        synchronized (this) {
            taken = take(false);
        }
        return taken.thenCompose(message -> {
            CompletableFuture<Void> handled;
            try {
                handled = handler.apply(message);
            } catch (RuntimeException failure) {
                handled = CompletableFuture.failedFuture(failure);
            }
            return handled.thenCompose(ignored -> after == CommitAfter.PROCESSING
                    ? reader.countDelivered(message.checkpoint(), 1, message.endsBatch())
                    : CompletableFuture.completedFuture(null));
        });
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /** Hands the next held message over, or takes the next batch from the reader first. */
    private CompletableFuture<ReceivedMessage> take(boolean processing) {
        CompletableFuture<ReceivedMessage> result = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                synchronized (IggyConsumer.this) {
                    boolean cancelled = super.cancel(mayInterruptIfRunning);
                    if (cancelled && waiting == this && reading != null) {
                        reading.cancel(false);
                    }
                    return cancelled;
                }
            }
        };
        waiting = result;
        waitingProcesses = processing;
        resumeTake();
        return result;
    }

    private void resumeTake() {
        if (waiting == null || waiting.isDone()) {
            return;
        }
        if (stopping) {
            waiting.completeExceptionally(new IggyClientException("The consumer is shut down"));
            return;
        }
        while (!held.isEmpty()
                && held.peekFirst().index == held.peekFirst().batch.messages().size()) {
            held.pollFirst();
        }
        if (!held.isEmpty()) {
            CompletableFuture<ReceivedMessage> result = waiting;
            waiting = null;
            ReceivedMessage message = handOver(held.peekFirst());
            if (waitingProcesses && after == CommitAfter.PROCESSING) {
                uncounted = message;
            }
            result.complete(message);
            return;
        }
        if (reading != null) {
            return;
        }
        reading = reader.nextBatch();
        reading.whenComplete(this::receiveBatch);
    }

    /** The reader hands a batch of a sought partition over before it applies the seek, so the batch waits for it. */
    private synchronized void receiveBatch(PolledMessages next, Throwable error) {
        reading = null;
        if (error != null) {
            if (waiting != null) {
                waiting.completeExceptionally(unwrap(error));
            }
            return;
        }
        Parked kept = parked.get(next.partitionId());
        if (kept != null) {
            kept.batches().add(new Held(next));
        } else {
            held.addLast(new Held(next));
        }
        resumeTake();
    }

    private synchronized void applied(long partitionId, long seek, boolean moved) {
        Parked kept = parked.get(partitionId);
        if (kept == null) {
            return;
        }
        if (moved) {
            kept.batches().clear();
        }
        // A later seek of the partition decides what the batches before it become.
        if (kept.seek() == seek) {
            parked.remove(partitionId);
            held.addAll(kept.batches());
            resumeTake();
        }
    }

    private ReceivedMessage handOver(Held next) {
        int at = next.index++;
        PolledMessages batch = next.batch;
        Checkpoint checkpoint = batch.checkpointAt(at).orElseThrow();
        ReceivedMessage message = new ReceivedMessage(
                batch.partitionId(),
                batch.messages().get(at),
                checkpoint,
                at == batch.messages().size() - 1);
        reader.deliverMessage(checkpoint);
        if (after == CommitAfter.DELIVERY) {
            reader.countDelivered(checkpoint, 1, message.endsBatch());
        }
        return message;
    }

    private void countUncounted() {
        if (uncounted != null) {
            ReceivedMessage message = uncounted;
            uncounted = null;
            reader.countDelivered(message.checkpoint(), 1, message.endsBatch());
        }
    }

    /** A batch and the index of its next message to hand over. */
    private static final class Held {
        private final PolledMessages batch;
        private int index;

        private Held(PolledMessages batch) {
            this.batch = batch;
        }
    }

    /** The number of the latest seek of a partition, and the batches of the partition that wait for it. */
    private record Parked(long seek, List<Held> batches) {}
}
