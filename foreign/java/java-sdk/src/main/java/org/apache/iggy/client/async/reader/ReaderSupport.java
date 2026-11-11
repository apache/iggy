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

import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.DecodeFailure;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** The helpers of {@link PartitionReader} that hold no state. */
final class ReaderSupport {
    private static final Logger log = LoggerFactory.getLogger(PartitionReader.class);

    private ReaderSupport() {}

    /** The first message that does not decode. Malformed framing isolates none, so it follows the healthy prefix. */
    static BigInteger undecoded(FetchedMessages section, DecodeFailure failure, List<Message> messages) {
        return failure.offset()
                .orElseGet(() -> messages.isEmpty()
                        ? section.nextOffset().subtract(BigInteger.valueOf(section.count()))
                        : messages.get(messages.size() - 1).header().offset().add(BigInteger.ONE));
    }

    static PollingStrategy skipPast(long partitionId, BigInteger bad, PartitionContext context) {
        return new PollingStrategy(
                PollingKind.Offset, bad.add(BigInteger.ONE), Optional.of(context), Optional.of(partitionId));
    }

    /** The error of the offsets from {@code from} to before {@code end}, which a read whose reply was lost stored. */
    static ReadException lostRange(long partitionId, BigInteger from, BigInteger end) {
        BigInteger last = end.subtract(BigInteger.ONE);
        return new ReadException(
                partitionId,
                new IggyServerException(
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN,
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode(),
                        "Offsets " + from + " to " + last + " were stored by a read whose reply was lost",
                        Optional.empty(),
                        Optional.empty()),
                new ReadException.OffsetRange(from, last));
    }

    /**
     * Whether the server could not serve the read of {@code section}. Its fail-fast reply has no messages and a zero
     * context, so it says nothing about the history of the partition, and the reader reads the same range again.
     */
    static boolean unserved(FetchedMessages section) {
        return section.context().equals(PartitionContext.EMPTY);
    }

    /**
     * A read never moves the stored offset back, so a seek to a timestamp probes its first message and seeks to the
     * offset of it. Without such a message, every later one lies after the stored offset, so the partition reads from
     * the timestamp. The partition reads nothing until the seek ends, so the probe does not wait for a new message.
     */
    static PollingStrategy probed(PollingStrategy position, PolledMessages polled) {
        if (polled.messages().isEmpty()) {
            return position;
        }
        return new PollingStrategy(
                PollingKind.Offset, polled.messages().get(0).header().offset(), Optional.of(polled.context()));
    }

    /** Stores the offset before {@code from}. No offset comes before 0, so there it deletes the stored offset. */
    static CompletableFuture<Void> storeBefore(
            ReaderTransport transport, long partitionId, BigInteger from, PartitionContext context) {
        return from.signum() > 0
                ? transport.store(partitionId, from.subtract(BigInteger.ONE), context)
                : transport.delete(partitionId, context);
    }

    /**
     * If the server holds an offset at or past {@code from}, this runs {@link #storeBefore}. Otherwise no read stored
     * from there, and the server refuses a delete of a missing offset and a store past the last message.
     */
    static CompletableFuture<Void> storeBeforeIfStored(
            ReaderTransport transport, long partitionId, BigInteger from, PartitionContext context) {
        return transport
                .storedOffset(partitionId)
                .thenCompose(stored ->
                        stored.isPresent() && stored.get().storedOffset().compareTo(from) >= 0
                                ? storeBefore(transport, partitionId, from, context)
                                : CompletableFuture.completedFuture(null));
    }

    /**
     * Stores the offset before {@code point} again. At 0 no offset was stored, so it deletes the stored offset if the
     * server holds one.
     */
    static CompletableFuture<Void> restoreBefore(
            ReaderTransport transport, long partitionId, BigInteger point, PartitionContext context) {
        return point.signum() == 0
                ? storeBeforeIfStored(transport, partitionId, point, context)
                : storeBefore(transport, partitionId, point, context);
    }

    /** The offset after a stored offset, where a read from the next offset starts, or 0 without one. */
    static BigInteger nextAfter(Optional<ConsumerOffsetInfo> stored) {
        return stored.map(info -> info.storedOffset().add(BigInteger.ONE)).orElse(BigInteger.ZERO);
    }

    /** Polls {@code strategy} without a store. A {@code Last} poll asks for the count of the reader. */
    static CompletableFuture<PolledMessages> checkPoll(
            ReaderTransport transport, PollOptions options, long id, PollingStrategy strategy, Duration maxWait) {
        long count = strategy.kind() == PollingKind.Last ? options.count() : 1;
        try {
            return transport.poll(
                    id,
                    strategy,
                    count,
                    options.withMinCount(1).withMaxWait(maxWait).deferredOptions());
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
    }

    /** Reads one message from {@code strategy} without a store, and waits up to the wait of the options for it. */
    static CompletableFuture<List<FetchedMessages>> probe(
            ReaderTransport transport, PollOptions options, long id, PollingStrategy strategy) {
        return transport.read(
                Map.of(id, strategy), 1, false, options.withMinCount(1).deferredOptions());
    }

    /** A fetch starts only at an offset or at the next offset, so a poll finds the offset of any other position. */
    static boolean needsCheck(PollingStrategy position) {
        return position.kind() != PollingKind.Offset && position.kind() != PollingKind.Next;
    }

    /**
     * Joins the group. A join whose outcome stays unknown blocks the metadata writes of the client until released. The
     * reader abandons it at once instead of resolving it: a later refresh reads the assignment first and joins again if
     * needed, and a second join of a session is a no-op, so a proved outcome would not change the next step.
     */
    static CompletableFuture<Void> joinGroup(ReaderTransport transport) {
        return transport.join().exceptionallyCompose(error -> releaseUnknown(transport, error));
    }

    /**
     * The assignment of the member. If the group lost the member, as after a lost session, the reader runs
     * {@code lost} on {@code loop} and joins again first. Once {@code closed} holds, the close leaves or left the group,
     * so a lost member stays out, and the assignment is empty.
     */
    static CompletableFuture<Optional<ConsumerGroupAssignment>> assignment(
            ReaderTransport transport, BooleanSupplier closed, Runnable lost, Executor loop) {
        return transport
                .assignment()
                .thenComposeAsync(
                        assignment -> {
                            if (assignment.isPresent() || closed.getAsBoolean()) {
                                return CompletableFuture.completedFuture(assignment);
                            }
                            lost.run();
                            return joinGroup(transport).thenCompose(ignored -> transport.assignment());
                        },
                        loop);
    }

    /**
     * Leaves the group of a member. A membership that the server already ended has nothing to leave. A leave whose
     * outcome stays unknown is abandoned, so the metadata writes of the client go on.
     */
    static CompletableFuture<Throwable> leaveGroup(ReaderTransport transport) {
        if (!transport.group()) {
            return CompletableFuture.completedFuture(null);
        }
        return transport
                .leave()
                .exceptionallyCompose(error -> releaseUnknown(transport, error))
                .handle((ignored, error) -> {
                    Throwable cause = error == null ? null : unwrap(error);
                    return cause instanceof IggyServerException server
                                    && server.getRawErrorCode()
                                            == IggyErrorCode.CONSUMER_GROUP_MEMBER_NOT_FOUND.getCode()
                            ? null
                            : cause;
                });
    }

    /** Whether a write ended with an unknown outcome. Until its abandon, the client refuses the later writes of it. */
    static boolean unknownOutcome(Throwable error) {
        return unwrap(error) instanceof IggyOperationException operation
                && operation.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN;
    }

    /**
     * Fails with {@code error}. A write with an unknown outcome blocks the later writes of its partition on the client,
     * so this abandons it first, and a later store of the partition goes through.
     */
    static <T> CompletableFuture<T> releaseUnknown(ReaderTransport transport, Throwable error) {
        Throwable cause = unwrap(error);
        CompletableFuture<Void> released = unknownOutcome(cause)
                ? transport
                        .abandon(((IggyOperationException) cause).getOperationId())
                        .handle((ignored, failure) -> null)
                : CompletableFuture.completedFuture(null);
        return released.thenCompose(ignored -> CompletableFuture.failedFuture(cause));
    }

    /**
     * Sends {@code write}. A refusal as not the owner is no leave either: the session can have changed after the write
     * took its context, or a rebalance gave the partition to the member at an op that no reply of the client named. So
     * after such a refusal, if {@code installed} gives the context of the owner that the server installed, and that
     * context serves the incarnation of the write, the write goes once more with its owner and op. Otherwise the
     * refusal stands.
     */
    static CompletableFuture<Void> ownedWrite(
            ReaderTransport transport,
            long partitionId,
            BigInteger offset,
            PartitionContext context,
            OffsetWrite write,
            Supplier<CompletableFuture<Optional<PartitionContext>>> installed) {
        return write.write(transport, partitionId, offset, context)
                .exceptionallyCompose(refusal -> notOwned(refusal)
                        ? installed
                                .get()
                                .thenCompose(owner -> reowned(context, owner)
                                        .map(fenced -> write.write(transport, partitionId, offset, fenced))
                                        .orElseGet(() -> CompletableFuture.failedFuture(unwrap(refusal))))
                        : CompletableFuture.failedFuture(unwrap(refusal)));
    }

    /**
     * The context of the owner that the server installed for the member on {@code partitionId}. One poll from the next
     * offset without a store, a context or a wait: the server serves owner generation 0 for the installed owner of the
     * member. The poll reads only the context of its reply, so a message that does not decode fails nothing. While
     * {@code retries} holds, a transient refusal polls again after {@code retryIntervalNanos}. The server refuses a
     * poll of one partition over its poll quota as a whole request, so a transient error of the request counts too.
     * Empty after any other failure, and once {@code loop} stopped. Also empty once {@code retries} is false, also for
     * the answer of a poll that was in flight then, so that answer leads to no write.
     */
    static CompletableFuture<Optional<PartitionContext>> installedContext(
            ReaderTransport transport,
            PollOptions options,
            long partitionId,
            ScheduledExecutorService loop,
            long retryIntervalNanos,
            BooleanSupplier retries) {
        CompletableFuture<Optional<PartitionContext>> installed = new CompletableFuture<>();
        pollInstalled(transport, options, partitionId, loop, retryIntervalNanos, retries, installed);
        return installed;
    }

    private static void pollInstalled(
            ReaderTransport transport,
            PollOptions options,
            long partitionId,
            ScheduledExecutorService loop,
            long retryIntervalNanos,
            BooleanSupplier retries,
            CompletableFuture<Optional<PartitionContext>> installed) {
        if (!retries.getAsBoolean()) {
            installed.complete(Optional.empty());
            return;
        }
        pollContext(transport, options, partitionId).whenComplete((context, error) -> {
            if (error == null) {
                installed.complete(retries.getAsBoolean() ? Optional.of(context) : Optional.empty());
            } else if (unwrap(error) instanceof IggyServerException server
                    && transientRefusal(server.getRawErrorCode())
                    && retries.getAsBoolean()) {
                try {
                    loop.schedule(
                            () -> pollInstalled(
                                    transport, options, partitionId, loop, retryIntervalNanos, retries, installed),
                            retryIntervalNanos,
                            TimeUnit.NANOSECONDS);
                } catch (RejectedExecutionException stopped) {
                    installed.complete(Optional.empty());
                }
            } else {
                installed.complete(Optional.empty());
            }
        });
    }

    /** One poll of {@code id} from the next offset without a store or a wait, which reads only the reply context. */
    private static CompletableFuture<PartitionContext> pollContext(
            ReaderTransport transport, PollOptions options, long id) {
        try {
            return transport.pollContext(
                    id,
                    PollingStrategy.next(),
                    1,
                    options.withMinCount(1).withMaxWait(Duration.ZERO).deferredOptions());
        } catch (RuntimeException refused) {
            return CompletableFuture.failedFuture(refused);
        }
    }

    /**
     * The context of a write from {@code context} under {@code installed}, if that serves its incarnation. The client
     * sends a write without an incarnation with the incarnation of its route, so any incarnation serves it.
     */
    private static Optional<PartitionContext> reowned(PartitionContext context, Optional<PartitionContext> installed) {
        return installed
                .filter(owner -> context.incarnation().signum() == 0
                        || owner.incarnation().equals(context.incarnation()))
                .map(owner -> new PartitionContext(context.incarnation(), owner.ownerGeneration(), owner.metadataOp()));
    }

    static boolean notOwned(Throwable error) {
        return unwrap(error) instanceof IggyServerException server
                && server.getRawErrorCode() == IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode();
    }

    /** Whether the server refuses a read only for now: a barrier of an install, a metadata lag, or its capacity. */
    static boolean transientRefusal(int code) {
        return code == IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()
                || code == IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode()
                || code == IggyErrorCode.DEFERRED_POLL_CAPACITY_EXCEEDED.getCode();
    }

    /** Offsets compare within one incarnation and owner. The server raises the metadata op of a history meanwhile. */
    static boolean sameIncarnationAndOwner(PartitionContext left, PartitionContext right) {
        return left.incarnation().equals(right.incarnation())
                && left.ownerGeneration().equals(right.ownerGeneration());
    }

    /** Whether the time {@code at} passed, where zero is no time. */
    static boolean due(long now, long at) {
        return at == 0 || now - at >= 0;
    }

    static void cancel(ScheduledFuture<?> timer) {
        if (timer != null) {
            timer.cancel(false);
        }
    }

    /** The position of a seek at its turn. A seek that its caller cancelled while it waited stops without a request. */
    static CompletableFuture<PollingStrategy> startSeek(CompletableFuture<Void> sought, PollingStrategy position) {
        return sought.isCancelled() ? cancelledSeek() : CompletableFuture.completedFuture(position);
    }

    /** Stops a seek that its caller cancelled before its first write. */
    static <T> CompletableFuture<T> cancelledSeek() {
        return CompletableFuture.failedFuture(new CancellationException("The seek was cancelled"));
    }

    /**
     * Completes the future of the caller of a seek. A caller that stopped waiting after the first write learns nothing
     * of a failure in the background, so the failure is logged. A cancel before the first write fails nothing.
     */
    static void completeSeek(CompletableFuture<Void> sought, long partitionId, Throwable error) {
        if (error == null) {
            sought.complete(null);
        } else if (!sought.completeExceptionally(unwrap(error)) && !(unwrap(error) instanceof CancellationException)) {
            log.warn(
                    "A seek of partition {} failed after its caller stopped waiting: {}",
                    partitionId,
                    unwrap(error).getMessage());
        }
    }

    /** A completed future without {@code error}, and a failed one with it. */
    static CompletableFuture<Void> outcome(Throwable error) {
        return error == null ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(unwrap(error));
    }

    static <T> void complete(CompletableFuture<T> future, Throwable error) {
        if (error == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(unwrap(error));
        }
    }

    static Throwable unwrap(Throwable error) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    static IggyClientException closedError() {
        return new IggyClientException("The partition reader is closed");
    }

    /**
     * Waits for the close of a reader. As in the blocking reader, an interrupt of the waiting thread cancels
     * {@code closing} and throws {@link IggyClientException} with the interrupt flag still set.
     */
    static void awaitClose(CompletableFuture<Void> closing) {
        try {
            closing.get();
        } catch (InterruptedException interrupted) {
            closing.cancel(false);
            Thread.currentThread().interrupt();
            throw new IggyClientException("Interrupted while closing the reader", interrupted);
        } catch (ExecutionException failed) {
            throw failed.getCause() instanceof RuntimeException runtime
                    ? runtime
                    : new CompletionException(failed.getCause());
        }
    }

    static IggyValidationException invalid(String reason) {
        return new IggyValidationException(
                IggyErrorCode.INVALID_COMMAND,
                IggyErrorCode.INVALID_COMMAND.getCode(),
                reason,
                Optional.empty(),
                Optional.empty());
    }

    /** An offset write of one partition, such as {@link #storeBefore}. */
    @FunctionalInterface
    interface OffsetWrite {
        CompletableFuture<Void> write(
                ReaderTransport transport, long partitionId, BigInteger offset, PartitionContext context);
    }
}
