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

package org.apache.iggy.client.async.tcp.vsr;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.util.concurrent.EventExecutor;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * VSR client identity and dedup state, mirroring
 * {@code core/sdk/src/session.rs}.
 *
 * <p>The (client id, request id) pair is the server's dedup key for
 * replicated operations, and the session value is the fence epoch of the
 * latest committed {@code Register}. The bind secret is a bearer credential
 * that authenticates another connection to this identity. Keep it private to
 * the client and never log or expose it.
 */
public final class ConsensusSession {

    static final int BIND_SECRET_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();
    private final byte[] bindSecret = new byte[BIND_SECRET_BYTES];

    private long clientIdLow;
    private long clientIdHigh;
    private Long session;
    private long requestCounter = 1;
    private long generation;
    private long metadataWatermark;
    private boolean shared;
    private ConsensusSession parent;
    private final Map<LaneKey, Ticket> lanes = new HashMap<>();
    private final Map<LaneKey, ArrayDeque<CompletableFuture<Ticket>>> laneAdmissions = new HashMap<>();
    private final Map<Long, Ticket> retained = new HashMap<>();
    // The outcomes of the last maxPendingOperations proved replays, oldest first. A reset keeps them, because
    // request ids never repeat within a client.
    private final ArrayDeque<ProvedOutcome> proved = new ArrayDeque<>();
    private CompletableFuture<Void> admissionChanged = new CompletableFuture<>();
    private int maxPendingOperations = 256;
    private long maxPendingBytes = 64L * 1024 * 1024;
    private int pendingOperations;
    private long pendingBytes;

    public ConsensusSession() {
        regenerateClientId();
    }

    /**
     * Returns the zero request id of Register without changing its identity.
     * Repeating a registration after a lost reply must resolve the same epoch.
     *
     * <p>The request counter survives explicit resets too, so late replies
     * cannot collide with pending requests from a replacement identity.
     */
    synchronized long beginRegister() {
        return 0;
    }

    /** Binds the fence epoch returned by a committed Register reply. */
    synchronized void bind(long sessionEpoch) {
        if (parent != null) {
            parent.bind(sessionEpoch);
            return;
        }
        if (sessionEpoch == 0) {
            throw new IllegalStateException("Register reply carried a zero session epoch");
        }
        if (session != null && session != sessionEpoch) {
            throw new IllegalStateException("Register reply changed the bound session epoch");
        }
        this.session = sessionEpoch;
        generation++;
    }

    /**
     * Replicated ops (metadata and partition) consume the monotonic VSR dedup
     * unsigned counter. Negative Java values preserve the upper half of the
     * wire u64 range; zero marks exhaustion after the final valid id.
     *
     * <p>Exhaustion is terminal for this instance. {@link #beginRegister()}
     * deliberately carries the counter across a re-login to keep pending-reply
     * correlation keys unique, so reconnecting cannot rewind it; only a new
     * client instance starts a fresh sequence.
     *
     * <p>A shared session draws the id from its parent, see {@link #bindShared}.
     */
    long nextRequestId() {
        ConsensusSession numbering;
        synchronized (this) {
            numbering = parent;
            if (numbering == null) {
                if (session == null) {
                    throw new IggyNotConnectedException("Not authenticated, call login first");
                }
                if (requestCounter == 0) {
                    throw new IggyServerException(IggyErrorCode.REQUEST_ID_EXHAUSTED.getCode());
                }
                return requestCounter++;
            }
        }
        // Other shared-session accessors nest locks in child-before-parent order.
        return numbering.nextRequestId();
    }

    long nextCorrelationId() {
        synchronized (this) {
            if (parent != null) {
                return parent.nextCorrelationId();
            }
            if (session == null) {
                if (requestCounter == 0) {
                    throw new IggyServerException(IggyErrorCode.REQUEST_ID_EXHAUSTED.getCode());
                }
                return requestCounter++;
            }
        }
        return nextRequestId();
    }

    public synchronized long[] identity() {
        if (parent != null) {
            return parent.identity();
        }
        return new long[] {clientIdLow, clientIdHigh, boundSession()};
    }

    synchronized long currentRequestId() {
        return requestCounter;
    }

    synchronized long sessionOrZero() {
        if (parent != null) {
            return parent.sessionOrZero();
        }
        return session == null ? 0 : session;
    }

    public synchronized long boundSession() {
        if (parent != null) {
            return parent.boundSession();
        }
        if (session == null) {
            throw new IggyNotConnectedException("Not authenticated, call login first");
        }
        return session;
    }

    public synchronized boolean isBound() {
        return parent == null ? session != null : parent.isBound();
    }

    /** Ends the local identity after explicit logout or a refused session bind. */
    public void reset() {
        ConsensusSession owner = owner();
        List<Ticket> active;
        synchronized (owner) {
            active = owner.clearIdentity();
        }
        active.forEach(Ticket::complete);
    }

    /** Ends the local identity unless a reset already replaced this client id, and says whether it did. */
    public boolean resetIfCurrent(long clientLow, long clientHigh) {
        ConsensusSession owner = owner();
        List<Ticket> active;
        synchronized (owner) {
            if (owner.clientIdLow != clientLow || owner.clientIdHigh != clientHigh) {
                return false;
            }
            active = owner.clearIdentity();
        }
        active.forEach(Ticket::complete);
        return true;
    }

    private List<Ticket> clearIdentity() {
        session = null;
        regenerateClientId();
        generation++;
        return List.copyOf(lanes.values());
    }

    public synchronized byte[] bindSecret() {
        return parent == null ? bindSecret.clone() : parent.bindSecret();
    }

    public synchronized byte[] bindSecret(long clientLow, long clientHigh, long epoch) {
        if (parent != null) {
            return parent.bindSecret(clientLow, clientHigh, epoch);
        }
        if (session == null || session != epoch || clientIdLow != clientLow || clientIdHigh != clientHigh) {
            throw new IggyNotConnectedException("Poll attachment no longer belongs to the parent session");
        }
        return bindSecret.clone();
    }

    /**
     * Shares the logical identity with a physical connection. Replicated requests
     * take their ids from the owner's counter: the partition deduplicates by client
     * and request id whichever connection carries them, so a counter of this
     * connection would reuse ids the server already answered.
     */
    public synchronized void follow(ConsensusSession owner) {
        if (owner == this) {
            throw new IggyInvalidArgumentException("A physical connection needs a distinct session owner");
        }
        parent = owner.owner();
        shared = true;
    }

    public synchronized void bindShared(
            ConsensusSession parent, long clientLow, long clientHigh, long epoch, byte[] secret) {
        if (parent == null
                || parent == this
                || (clientLow == 0 && clientHigh == 0)
                || epoch == 0
                || secret == null
                || secret.length != BIND_SECRET_BYTES) {
            throw new IggyInvalidArgumentException("Shared session requires another parent session, a nonzero client,"
                    + " a nonzero epoch and a " + BIND_SECRET_BYTES + "-byte bind secret");
        }
        bind(epoch);
        clientIdLow = clientLow;
        clientIdHigh = clientHigh;
        System.arraycopy(secret, 0, bindSecret, 0, BIND_SECRET_BYTES);
        this.parent = parent.owner();
        shared = true;
    }

    synchronized void onChannelCreated() {
        if (shared) {
            generation++;
        }
    }

    public synchronized long generation() {
        return generation;
    }

    public synchronized long metadataWatermark() {
        return parent == null ? metadataWatermark : parent.metadataWatermark();
    }

    synchronized void observeMetadata(long commit) {
        if (parent != null) {
            parent.observeMetadata(commit);
            return;
        }
        if (Long.compareUnsigned(commit, metadataWatermark) > 0) {
            metadataWatermark = commit;
        }
    }

    synchronized long clientIdLow() {
        return parent == null ? clientIdLow : parent.clientIdLow();
    }

    synchronized long clientIdHigh() {
        return parent == null ? clientIdHigh : parent.clientIdHigh();
    }

    public ConsensusSession owner() {
        ConsensusSession numbering;
        synchronized (this) {
            numbering = parent;
        }
        return numbering == null ? this : numbering.owner();
    }

    public synchronized void configurePendingOperations(int count, long bytes) {
        if (count <= 0 || bytes <= 0 || pendingOperations != 0 || pendingBytes != 0) {
            throw new IggyInvalidArgumentException(
                    "Pending operation limits must be positive and configured before use");
        }
        maxPendingOperations = count;
        maxPendingBytes = bytes;
    }

    public CompletableFuture<Ticket> acquire(
            LaneKey lane, boolean mutation, long frameBytes, long deadline, EventExecutor executor) {
        ConsensusSession owner = owner();
        if (owner != this) {
            return owner.acquire(lane, mutation, frameBytes, deadline, executor);
        }
        CompletableFuture<Ticket> result = new CompletableFuture<>();
        var timer = executor.schedule(
                () -> result.completeExceptionally(
                        new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode())),
                Math.max(0, deadline - System.nanoTime()),
                TimeUnit.NANOSECONDS);
        synchronized (this) {
            laneAdmissions.computeIfAbsent(lane, ignored -> new ArrayDeque<>()).addLast(result);
        }
        result.whenComplete((ticket, error) -> {
            timer.cancel(false);
            boolean wakeNext;
            synchronized (this) {
                var waiting = laneAdmissions.get(lane);
                wakeNext = waiting.getFirst() == result && !lanes.containsKey(lane);
                waiting.remove(result);
                if (waiting.isEmpty()) {
                    laneAdmissions.remove(lane);
                }
            }
            if (wakeNext) {
                changed();
            }
        });
        acquireWhenAvailable(lane, mutation, frameBytes, deadline, result);
        return result;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void acquireWhenAvailable(
            LaneKey lane, boolean mutation, long frameBytes, long deadline, CompletableFuture<Ticket> result) {
        CompletableFuture<Void> changed = null;
        Ticket admitted = null;
        RuntimeException failure = null;
        synchronized (this) {
            if (result.isDone()) {
                return;
            }
            Ticket blocking = lanes.get(lane);
            if (blocking != null && blocking.retained) {
                failure = new IggyOperationException(IggyErrorCode.LANE_BLOCKED, blocking.operationId);
            } else if (mutation && frameBytes > maxPendingBytes) {
                failure = new IggyInvalidArgumentException("Request exceeds the pending operation byte limit");
            } else if (deadline - System.nanoTime() <= 0) {
                failure = new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
            } else if (laneAdmissions.get(lane).getFirst() != result
                    || blocking != null
                    || (mutation
                            && (pendingOperations >= maxPendingOperations
                                    || frameBytes > maxPendingBytes - pendingBytes))) {
                changed = admissionChanged;
            } else {
                admitted = new Ticket(this, lane, mutation, frameBytes, generation);
                lanes.put(lane, admitted);
                if (mutation) {
                    pendingOperations++;
                    pendingBytes += frameBytes;
                }
            }
        }
        if (failure != null) {
            result.completeExceptionally(failure);
        } else if (admitted != null) {
            if (!result.complete(admitted)) {
                admitted.complete();
            }
        } else {
            changed.thenRun(() -> acquireWhenAvailable(lane, mutation, frameBytes, deadline, result));
        }
    }

    public void checkRetained(LaneKey lane) {
        ConsensusSession owner = owner();
        synchronized (owner) {
            Ticket ticket = owner.lanes.get(lane);
            if (ticket != null && ticket.retained) {
                throw new IggyOperationException(IggyErrorCode.LANE_BLOCKED, ticket.operationId);
            }
        }
    }

    /**
     * Claims the retained operation for a replay. While another replay of it runs, the claim waits for that
     * replay to end. A wait still running at {@code deadline} fails with {@code OPERATION_UNCERTAIN}, as in
     * Rust. An operation that is not retained fails with {@code OPERATION_NOT_RETAINED}, and
     * {@link #provedOutcome} can still hold its outcome.
     */
    public CompletableFuture<Ticket> resume(long operationId, long deadline, EventExecutor executor) {
        ConsensusSession owner = owner();
        if (owner != this) {
            return owner.resume(operationId, deadline, executor);
        }
        CompletableFuture<Ticket> result = new CompletableFuture<>();
        resumeWhenIdle(operationId, null, result);
        if (result.isDone()) {
            return result;
        }
        // Only a wait arms the deadline, so a claim that needs no wait never fails for it.
        try {
            var timer = executor.schedule(
                    () -> result.completeExceptionally(
                            new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, operationId)),
                    Math.max(0, deadline - System.nanoTime()),
                    TimeUnit.NANOSECONDS);
            result.whenComplete((ticket, error) -> timer.cancel(false));
        } catch (RejectedExecutionException stopped) {
            result.completeExceptionally(stopped);
        }
        return result;
    }

    private void resumeWhenIdle(long operationId, Ticket running, CompletableFuture<Ticket> result) {
        CompletableFuture<Void> changed = null;
        Ticket claimed = null;
        Ticket awaited = null;
        synchronized (this) {
            if (result.isDone()) {
                return;
            }
            Ticket ticket = retained.get(operationId);
            // A replay that an abandon left running can still prove the outcome for this wait.
            Ticket replay = ticket != null ? ticket : running;
            if (ticket != null && !ticket.resolving) {
                ticket.resolving = true;
                claimed = ticket;
            } else if (replay != null && !replay.completed) {
                changed = admissionChanged;
                awaited = replay;
            }
        }
        if (changed != null) {
            Ticket next = awaited;
            changed.thenRun(() -> resumeWhenIdle(operationId, next, result));
        } else if (claimed == null) {
            result.completeExceptionally(new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId));
        } else if (!result.complete(claimed)) {
            // The wait ended first, so the next resolver can claim the operation, unless an abandon released it.
            claimed.retain();
        }
    }

    /**
     * Returns the reply body that a replay proved for the operation, or throws the committed rejection that it
     * proved. Without a kept outcome it throws {@code OPERATION_NOT_RETAINED}.
     */
    public byte[] provedOutcome(long operationId) {
        ConsensusSession owner = owner();
        ProvedOutcome outcome;
        synchronized (owner) {
            outcome = owner.proved.stream()
                    .filter(proof -> proof.operationId() == operationId)
                    .findFirst()
                    .orElseThrow(() -> new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId));
        }
        if (outcome.body() == null) {
            throw IggyServerException.fromTcpResponse(outcome.rejection(), new byte[0]);
        }
        return outcome.body().clone();
    }

    /**
     * Drops the retained operation and frees its pending operation slot at once. A running replay of it keeps the
     * lane and the frame until it ends, as in Rust, so no newer request of the lane overtakes it.
     */
    public void abandon(long operationId) {
        Ticket ticket;
        boolean idle;
        ConsensusSession owner = owner();
        synchronized (owner) {
            ticket = owner.retained.remove(operationId);
            if (ticket == null) {
                throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
            }
            ticket.retained = false;
            ticket.abandoned = true;
            owner.pendingOperations--;
            idle = !ticket.resolving;
        }
        if (idle) {
            ticket.complete();
        } else {
            owner.changed();
        }
    }

    private void changed() {
        CompletableFuture<Void> signal;
        synchronized (this) {
            signal = admissionChanged;
            admissionChanged = new CompletableFuture<>();
        }
        signal.complete(null);
    }

    private void regenerateClientId() {
        RANDOM.nextBytes(bindSecret);
        do {
            clientIdLow = RANDOM.nextLong();
            // Keep generated IDs outside the server-only range.
            clientIdHigh = RANDOM.nextLong() & Long.MAX_VALUE;
        } while (clientIdLow == 0 && clientIdHigh == 0);
    }

    public record LaneKey(long incarnation, long partition) {
        public static final LaneKey METADATA = new LaneKey(0, 0);
    }

    /** A copy of the reply body of a proved commit, or no body and the code of a proved committed rejection. */
    private record ProvedOutcome(long operationId, byte[] body, int rejection) {}

    /** One lane admission owns one immutable frame; writer aliases keep its bytes charged. */
    public static final class Ticket {
        private final ConsensusSession owner;
        private final LaneKey lane;
        private final boolean mutation;
        private final long bytes;
        private final long generation;
        private ByteBuf frame;
        private long operationId;
        private int commandCode;
        private byte[] routingBody;
        private int writers;
        private boolean completed;
        private boolean retained;
        private boolean resolving;
        // An abandon freed the pending operation slot already.
        private boolean abandoned;
        private boolean sent;
        private boolean bytesReleased;

        private Ticket(ConsensusSession owner, LaneKey lane, boolean mutation, long bytes, long generation) {
            this.owner = owner;
            this.lane = lane;
            this.mutation = mutation;
            this.bytes = bytes;
            this.generation = generation;
        }

        public void freeze(ByteBuf request, int command, byte[] target) {
            synchronized (owner) {
                if (completed || generation != owner.generation || frame != null) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                operationId = VsrHeaders.readRequestId(request);
                commandCode = command;
                routingBody = target;
                frame = request.retainedDuplicate();
                writers++;
            }
        }

        public boolean hasFrame() {
            synchronized (owner) {
                return frame != null;
            }
        }

        public ByteBuf frame() {
            synchronized (owner) {
                if (completed || frame == null) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                writers++;
                return frame.retainedDuplicate();
            }
        }

        public boolean renewRequestId(ByteBuf attemptFrame) {
            synchronized (owner) {
                if (completed || retained || abandoned || generation != owner.generation) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                if (writers != 1) {
                    return false;
                }
                operationId = owner.nextRequestId();
                attemptFrame.setLongLE(attemptFrame.readerIndex() + VsrHeaders.REQUEST_ID_OFFSET, operationId);
                int floorOffset = attemptFrame.readerIndex() + VsrHeaders.REQUEST_METADATA_OP_OFFSET;
                long capturedFloor = attemptFrame.getLongLE(floorOffset);
                if (lane.equals(LaneKey.METADATA) && Long.compareUnsigned(owner.metadataWatermark, capturedFloor) > 0) {
                    attemptFrame.setLongLE(floorOffset, owner.metadataWatermark);
                }
                return true;
            }
        }

        public PartitionContext context() {
            synchronized (owner) {
                if (frame == null) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                int base = frame.readerIndex();
                return new PartitionContext(
                        new BigInteger(
                                Long.toUnsignedString(frame.getLongLE(base + VsrHeaders.REQUEST_INCARNATION_OFFSET))),
                        new BigInteger(Long.toUnsignedString(
                                frame.getLongLE(base + VsrHeaders.REQUEST_OWNER_GENERATION_OFFSET))),
                        new BigInteger(
                                Long.toUnsignedString(frame.getLongLE(base + VsrHeaders.REQUEST_METADATA_OP_OFFSET))));
            }
        }

        public int commandCode() {
            return commandCode;
        }

        public byte[] routingBody() {
            return routingBody;
        }

        public long operationId() {
            return operationId;
        }

        public void startWrite() {
            synchronized (owner) {
                if (completed) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                sent = true;
                writers++;
            }
        }

        public void finishWrite() {
            synchronized (owner) {
                writers--;
                releaseBytesIfUnused();
            }
            owner.changed();
        }

        /**
         * Keeps what a replay proved, the reply body or the committed rejection, for later resolves of its
         * operation. It runs before the ticket completes, so a resolve that no longer finds the operation finds
         * the outcome. A replay keeps it also after an abandon or a reset completed its ticket. Calls that never
         * became uncertain keep nothing.
         */
        public void recordOutcome(ByteBuf body, IggyServerException rejection) {
            synchronized (owner) {
                if (!resolving) {
                    return;
                }
                owner.proved.addLast(new ProvedOutcome(
                        operationId,
                        rejection == null ? ByteBufUtil.getBytes(body) : null,
                        rejection == null ? 0 : rejection.getRawErrorCode()));
                while (owner.proved.size() > owner.maxPendingOperations) {
                    owner.proved.removeFirst();
                }
            }
        }

        public RuntimeException retain() {
            boolean saved;
            synchronized (owner) {
                saved = !completed && !abandoned && mutation && sent && generation == owner.generation;
                if (saved) {
                    retained = true;
                    resolving = false;
                    owner.retained.put(operationId, this);
                }
            }
            if (saved) {
                owner.changed();
                return new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, operationId);
            }
            complete();
            if (mutation && sent) {
                return new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, operationId);
            }
            return new IggyServerException(
                    sent
                            ? IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode()
                            : IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
        }

        public void complete() {
            synchronized (owner) {
                if (completed) {
                    return;
                }
                completed = true;
                owner.lanes.remove(lane, this);
                owner.retained.remove(operationId, this);
                if (mutation && !abandoned) {
                    owner.pendingOperations--;
                }
                if (frame != null) {
                    frame.release();
                    frame = null;
                }
                releaseBytesIfUnused();
            }
            owner.changed();
        }

        private void releaseBytesIfUnused() {
            if (mutation && completed && writers == 0 && !bytesReleased) {
                bytesReleased = true;
                owner.pendingBytes -= bytes;
            }
        }
    }
}
