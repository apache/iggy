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

import java.util.HashMap;
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
    private PolledMessages batch;
    private int index;
    // The partitions of the seeks that the reader has not applied yet, each with the number of its latest seek.
    private final Map<Long, Long> seeking = new HashMap<>();
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

    /** See {@link PartitionReader#seek}. The messages of the partition that the consumer still holds are dropped. */
    public synchronized CompletableFuture<Void> seek(long partitionId, PollingStrategy start) {
        if (batch != null && batch.partitionId() == partitionId) {
            batch = null;
        }
        if (uncounted != null && uncounted.partitionId() == partitionId) {
            uncounted = null;
        }
        long seek = ++seeks;
        seeking.put(partitionId, seek);
        try {
            return reader.seek(partitionId, start, () -> applied(partitionId, seek));
        } catch (RuntimeException refused) {
            applied(partitionId, seek);
            throw refused;
        }
    }

    /** The partitions of the reader. */
    public List<Long> partitions() {
        return reader.partitions();
    }

    /**
     * Stops a running {@link #consume} after its current handler, counts the last message under
     * {@link CommitAfter#PROCESSING}, and closes the reader, which stores, moves offsets back and leaves its group as
     * its commit policy says.
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
        return running.handle((ignored, error) -> null).thenCompose(ignored -> {
            synchronized (this) {
                countUncounted();
            }
            return reader.closeAsync();
        });
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
        if (batch != null && index < batch.messages().size()) {
            CompletableFuture<ReceivedMessage> result = waiting;
            waiting = null;
            ReceivedMessage message = handOver();
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

    /** The reader handed a batch of a sought partition over before it applied the seek, so the batch is stale. */
    private synchronized void receiveBatch(PolledMessages next, Throwable error) {
        reading = null;
        if (error != null) {
            if (waiting != null) {
                waiting.completeExceptionally(unwrap(error));
            }
            return;
        }
        if (!seeking.containsKey(next.partitionId())) {
            batch = next;
            index = 0;
        }
        resumeTake();
    }

    private synchronized void applied(long partitionId, long seek) {
        seeking.remove(partitionId, seek);
    }

    private ReceivedMessage handOver() {
        int at = index++;
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
}
