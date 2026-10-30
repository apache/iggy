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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    public static final int DEFAULT_MAX_PENDING_OPERATIONS = 256;
    public static final long DEFAULT_MAX_PENDING_OPERATION_BYTES = 64L * 1024 * 1024;
    static final int BIND_SECRET_BYTES = 32;
    private static final long FETCH_RECOVERY_BYTES = 2L * Long.BYTES;
    private static final long FETCH_TARGET_BYTES = 2L * Long.BYTES;
    // No lineage is negative, see signingIn().
    private static final long NO_SIGN_IN = -1;
    private static final SecureRandom RANDOM = new SecureRandom();
    // One order for the lanes of every request, with the progress lane of a partition before its mutation lane.
    private static final Comparator<LaneKey> ADMISSION_ORDER = Comparator.comparingLong(LaneKey::partition)
            .thenComparingLong(LaneKey::incarnation)
            .thenComparing(LaneKey::progress, Comparator.reverseOrder());
    private final byte[] bindSecret = new byte[BIND_SECRET_BYTES];

    private long clientIdLow;
    private long clientIdHigh;
    private long registerIdentifierLow;
    private long registerIdentifierHigh;
    private Long session;
    private long requestCounter = 1;
    private long generation;
    // See lineage().
    private long lineage;
    // The lineage in which a sign-in of the caller runs, see signingIn().
    private long signInLineage = NO_SIGN_IN;
    // See beganSignIn().
    private boolean signInBegun;
    // The automatic sign-in that the server refused, and the lineage that the refusal ended.
    private Throwable signInRefusal;
    private long refusedLineage;
    private long metadataWatermark;
    private boolean shared;
    private volatile ConsensusSession parent;
    private final Map<LaneKey, Ticket> lanes = new HashMap<>();
    private final Map<LaneKey, ArrayDeque<CompletableFuture<Ticket>>> laneAdmissions = new HashMap<>();
    private final Map<Long, Ticket> retained = new HashMap<>();
    private final Map<Long, FetchRecovery> retainedFetches = new HashMap<>();
    private final Map<LaneKey, FetchRecovery> blockedFetchLanes = new HashMap<>();
    private int fetchReservations;
    private long fetchBytes;
    // The outcomes of the last maxPendingOperations proved replays, oldest first. A reset keeps them, because
    // request ids never repeat within a client.
    private final ArrayDeque<ProvedOutcome> proved = new ArrayDeque<>();
    private CompletableFuture<Void> admissionChanged = new CompletableFuture<>();
    private int maxPendingOperations = DEFAULT_MAX_PENDING_OPERATIONS;
    private long maxPendingBytes = DEFAULT_MAX_PENDING_OPERATION_BYTES;
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
    void bind(long sessionEpoch) {
        bindWithoutSignal(sessionEpoch);
        owner().changed();
    }

    /** The reply handler signals after releasing the identity-check monitor. */
    void bindWithoutSignal(long sessionEpoch) {
        ConsensusSession owner;
        synchronized (this) {
            owner = parent;
            if (owner == null) {
                if (sessionEpoch == 0) {
                    throw new IllegalStateException("Register reply carried a zero session epoch");
                }
                if (session != null && session != sessionEpoch) {
                    throw new IllegalStateException("Register reply changed the bound session epoch");
                }
                this.session = sessionEpoch;
                generation++;
            }
        }
        if (owner != null) {
            owner.bindWithoutSignal(sessionEpoch);
        }
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
            active = owner.clearIdentity(false);
        }
        active.forEach(Ticket::complete);
    }

    /**
     * Ends the local identity after a logout unless a reset already replaced this client id, and says whether it
     * did. The caller signed out, so the lineage ends too.
     */
    public boolean resetIfCurrent(long clientLow, long clientHigh) {
        return clearIdentityIfCurrent(clientLow, clientHigh, false, true);
    }

    /**
     * {@link #resetIfCurrent} for the logout that a sign-in of the caller sends first. The lineage stays, as in Rust,
     * because that sign-in ended it as it began.
     */
    boolean resetForSignIn(long clientLow, long clientHigh) {
        return clearIdentityIfCurrent(clientLow, clientHigh, false, false);
    }

    /**
     * Ends the local identity of a session that the server lost, unless a reset already replaced this client id,
     * and says whether it did. Unlike {@link #resetIfCurrent}, the calls that hold or wait for a lane keep it, so
     * they keep their order: a call that has not sent its frame starts again under the next session, see
     * {@link Ticket#prepareWrite}. The retained operations end, as in Rust, because the next session cannot replay
     * them.
     */
    public boolean replaceLostSession(long clientLow, long clientHigh) {
        return clearIdentityIfCurrent(clientLow, clientHigh, true, false);
    }

    /**
     * A lost session keeps the live lanes and the lineage, because the client recovers it. A logout ends both. The
     * logout that a sign-in sends first ends the lanes and keeps the lineage, because that sign-in ended it.
     */
    private boolean clearIdentityIfCurrent(long clientLow, long clientHigh, boolean keepLiveLanes, boolean endLineage) {
        ConsensusSession owner = owner();
        List<Ticket> ended;
        synchronized (owner) {
            if (owner.clientIdLow != clientLow || owner.clientIdHigh != clientHigh) {
                return false;
            }
            ended = owner.clearIdentity(keepLiveLanes);
            if (endLineage) {
                owner.lineage++;
            }
        }
        ended.forEach(Ticket::complete);
        return true;
    }

    private List<Ticket> clearIdentity(boolean keepLiveLanes) {
        session = null;
        regenerateClientId();
        generation++;
        return List.copyOf(keepLiveLanes ? retained.values() : lanes.values());
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

    /**
     * The sign-in lineage of the caller, as in Rust. It rises when the caller signs in or out and when the server
     * refuses an automatic sign-in, and a recovery by the client keeps it. A call continues only in the lineage that
     * it started in, so it never runs under a session that the caller opened after it started.
     */
    public long lineage() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            return owner.lineage;
        }
    }

    /** Ends the lineage as a sign-in or a sign-out of the caller starts, and wakes the calls that wait for a session. */
    public void endLineage() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            owner.lineage++;
        }
        owner.changed();
    }

    /** {@link #endLineage} for a sign-in of the caller, which then runs in the new lineage, see {@link #signingIn}. */
    public void beginSignIn() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            owner.signInLineage = ++owner.lineage;
            owner.signInBegun = true;
        }
        owner.changed();
    }

    /**
     * Whether the caller ever began a sign-in. From then on, a call that finds no session and none on its way ends
     * with 40, as in Rust, instead of asking for a first sign-in.
     */
    public boolean beganSignIn() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            return owner.signInBegun;
        }
    }

    public void finishSignIn() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            owner.signInLineage = NO_SIGN_IN;
        }
    }

    /**
     * Whether a sign-in of the caller runs. Like {@code sign_in_blocked} in Rust, a call that needs a session then
     * ends with 40 at once and does not wait for the session of that sign-in. The sign-in stops running at
     * {@link #finishSignIn} or at the next end of the lineage, such as a failed sign-in or a close.
     */
    public boolean signingIn() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            return owner.signInLineage == owner.lineage;
        }
    }

    /**
     * Ends the lineage for an automatic sign-in that the server refused with {@code refusal}, and says whether it
     * did. No session comes without the caller then. A reset or a bind since that sign-in started at the owner
     * generation {@code generation} belongs to a newer sign-in, which keeps its lineage.
     */
    public boolean refuseSignIn(long generation, Throwable refusal) {
        ConsensusSession owner = owner();
        synchronized (owner) {
            if (owner.generation != generation) {
                return false;
            }
            owner.signInRefusal = unwrap(refusal);
            owner.refusedLineage = owner.lineage;
            owner.lineage++;
        }
        owner.changed();
        return true;
    }

    /**
     * The error of a call that started in {@code lineage} and that the end of that lineage stopped before it sent
     * its frame: 40. Its cause is the sign-in refusal that ended the lineage, else {@code cause} when there is one.
     */
    public IggyServerException lineageEnded(long lineage, Throwable cause) {
        ConsensusSession owner = owner();
        Throwable refusal;
        synchronized (owner) {
            refusal = owner.refusedLineage == lineage ? owner.signInRefusal : null;
        }
        Throwable reason = refusal != null ? refusal : unwrap(cause);
        IggyServerException unauthenticated = new IggyServerException(IggyErrorCode.UNAUTHENTICATED.getCode());
        if (reason != null) {
            unauthenticated.initCause(reason);
        }
        return unauthenticated;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
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

    synchronized long registerIdentifierLow() {
        return parent == null ? registerIdentifierLow : parent.registerIdentifierLow();
    }

    synchronized long registerIdentifierHigh() {
        return parent == null ? registerIdentifierHigh : parent.registerIdentifierHigh();
    }

    /**
     * Takes no monitor. A link that holds its session waits for the owner, so a caller that holds the owner and
     * resolves the owner of another link, for the lineage of a call that started there, must not wait for that link.
     */
    public ConsensusSession owner() {
        ConsensusSession numbering = parent;
        return numbering == null ? this : numbering.owner();
    }

    public synchronized void configurePendingOperations(int count, long bytes) {
        if (count <= 0 || bytes <= 0 || pendingOperations != 0 || pendingBytes != 0 || fetchReservations != 0) {
            throw new IggyInvalidArgumentException(
                    "Pending operation limits must be positive and configured before use");
        }
        maxPendingOperations = count;
        maxPendingBytes = bytes;
    }

    /**
     * Admits a request to {@code lanes}. Every request takes its lanes in ascending partition order, so two requests
     * never each wait for a lane that the other one reserved. A request joins the queue of a lane only once it heads
     * the queue of every lane before it, and it takes all its lanes once it heads every queue and no ticket holds any
     * of them. An offset mutation takes its progress turn before joining the partition mutation queue.
     */
    public CompletableFuture<Ticket> acquire(
            List<LaneKey> lanes, boolean mutation, long frameBytes, long deadline, EventExecutor executor) {
        return acquire(lanes, mutation, frameBytes, deadline, executor, false);
    }

    public CompletableFuture<Ticket> acquire(
            List<LaneKey> lanes,
            boolean mutation,
            long frameBytes,
            long deadline,
            EventExecutor executor,
            boolean inspect) {
        ConsensusSession owner = owner();
        if (owner != this) {
            return owner.acquire(lanes, mutation, frameBytes, deadline, executor, inspect);
        }
        CompletableFuture<Ticket> result = new CompletableFuture<>();
        List<LaneKey> order = admissionOrder(lanes, mutation);
        var timer = executor.schedule(
                () -> result.completeExceptionally(
                        new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode())),
                Math.max(0, deadline - System.nanoTime()),
                TimeUnit.NANOSECONDS);
        synchronized (this) {
            laneAdmissions
                    .computeIfAbsent(order.get(0), ignored -> new ArrayDeque<>())
                    .addLast(result);
        }
        result.whenComplete((ticket, error) -> {
            timer.cancel(false);
            boolean wakeNext = false;
            synchronized (this) {
                for (LaneKey lane : order) {
                    wakeNext |= removeAdmission(lane, result);
                }
            }
            if (wakeNext) {
                changed();
            }
        });
        acquireWhenAvailable(order, mutation, frameBytes, deadline, result, 1, inspect);
        return result;
    }

    private static List<LaneKey> admissionOrder(List<LaneKey> lanes, boolean mutation) {
        List<LaneKey> order = new ArrayList<>(lanes.size() + 1);
        for (LaneKey lane : lanes) {
            order.add(lane);
            if (mutation && lane.progress()) {
                order.add(new LaneKey(lane.incarnation(), lane.partition()));
            }
        }
        order.sort(ADMISSION_ORDER);
        return List.copyOf(order);
    }

    private boolean removeAdmission(LaneKey lane, CompletableFuture<Ticket> result) {
        var waiting = laneAdmissions.get(lane);
        if (waiting == null) {
            return false;
        }
        boolean wakeNext = waiting.getFirst() == result && !lanes.containsKey(lane);
        waiting.remove(result);
        if (waiting.isEmpty()) {
            laneAdmissions.remove(lane);
        }
        return wakeNext;
    }

    /**
     * Admits {@code result} once it heads the queue of each lane of {@code order} and no ticket holds any of them.
     * The request is in the queues of the first {@code queued} lanes.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void acquireWhenAvailable(
            List<LaneKey> order,
            boolean mutation,
            long frameBytes,
            long deadline,
            CompletableFuture<Ticket> result,
            int queued,
            boolean inspect) {
        CompletableFuture<Void> changed = null;
        Ticket admitted = null;
        RuntimeException failure = null;
        int joined = queued;
        synchronized (this) {
            if (result.isDone()) {
                return;
            }
            IggyOperationException retainedFailure = null;
            for (LaneKey lane : order) {
                FetchRecovery fetch = blockedFetchLanes.get(lane);
                if (fetch != null && !inspect) {
                    retainedFailure = new IggyOperationException(
                            IggyErrorCode.PARTITION_LANE_BLOCKED, fetch.operationId, lane.partition());
                    break;
                }
                Ticket blocking = lanes.get(lane);
                if (blocking != null && blocking.retained) {
                    retainedFailure = blockedLane(lane, blocking);
                    break;
                }
            }
            if (retainedFailure != null) {
                failure = retainedFailure;
            } else if (mutation && frameBytes > maxPendingBytes) {
                failure = new IggyInvalidArgumentException("Request exceeds the pending operation byte limit");
            } else if (deadline - System.nanoTime() <= 0) {
                failure = new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
            } else {
                // A store enters the mutation queue only after its progress turn,
                // so a parked poll cannot hold back producers through that store.
                int headed = 0;
                while (headed < joined
                        && laneAdmissions.get(order.get(headed)).getFirst() == result
                        && !lanes.containsKey(order.get(headed))) {
                    headed++;
                    if (headed == joined && joined < order.size()) {
                        laneAdmissions
                                .computeIfAbsent(order.get(joined), ignored -> new ArrayDeque<>())
                                .addLast(result);
                        joined++;
                    }
                }
                if (headed < order.size()
                        || (mutation
                                && (pendingOperations >= maxPendingOperations
                                        || frameBytes > maxPendingBytes - pendingBytes))) {
                    changed = admissionChanged;
                } else {
                    admitted = new Ticket(this, order, mutation, frameBytes, generation);
                    for (LaneKey lane : order) {
                        lanes.put(lane, admitted);
                    }
                    if (mutation) {
                        pendingOperations++;
                        pendingBytes += frameBytes;
                    }
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
            int next = joined;
            changed.thenRun(() -> acquireWhenAvailable(order, mutation, frameBytes, deadline, result, next, inspect));
        }
    }

    public void checkRetained(LaneKey lane) {
        checkRetained(lane, false);
    }

    public void checkRetained(LaneKey lane, boolean mutation) {
        checkRetained(lane, mutation, false);
    }

    public void checkRetained(LaneKey lane, boolean mutation, boolean inspect) {
        ConsensusSession owner = owner();
        synchronized (owner) {
            FetchRecovery fetch = owner.blockedFetchLanes.get(lane);
            if (fetch != null && !inspect) {
                throw new IggyOperationException(
                        IggyErrorCode.PARTITION_LANE_BLOCKED, fetch.operationId, lane.partition());
            }
            Ticket ticket = owner.lanes.get(lane);
            if (ticket != null && ticket.retained) {
                throw blockedLane(lane, ticket);
            }
            if (mutation && lane.progress()) {
                Ticket blocking = owner.lanes.get(new LaneKey(lane.incarnation(), lane.partition()));
                if (blocking != null && blocking.retained) {
                    throw blockedLane(lane, blocking);
                }
            }
        }
    }

    private static IggyOperationException blockedLane(LaneKey lane, Ticket ticket) {
        return lane.equals(LaneKey.METADATA)
                ? new IggyOperationException(IggyErrorCode.LANE_BLOCKED, ticket.operationId)
                : new IggyOperationException(IggyErrorCode.LANE_BLOCKED, ticket.operationId, lane.partition());
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
        synchronized (this) {
            if (retainedFetches.containsKey(operationId)) {
                return CompletableFuture.failedFuture(
                        new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, operationId));
            }
        }
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
            FetchRecovery fetch = owner.retainedFetches.remove(operationId);
            if (fetch != null) {
                fetch.abandoned = true;
                owner.fetchReservations--;
                owner.fetchBytes -= fetch.bytes;
                fetch.lanes.forEach(lane -> owner.blockedFetchLanes.remove(lane, fetch));
                return;
            }
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

    /** Waits for an owed automatic sign-in without reserving a mutation lane. */
    public CompletableFuture<Void> awaitSession(long lineage, long deadline, EventExecutor executor) {
        return owner().awaitSession(lineage, deadline, executor, null);
    }

    private CompletableFuture<Void> awaitSession(long lineage, long deadline, EventExecutor executor, Ticket ticket) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        awaitSession(lineage, result, ticket);
        if (result.isDone()) {
            return result;
        }
        try {
            var timer = executor.schedule(
                    () -> result.completeExceptionally(
                            new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode())),
                    Math.max(0, deadline - System.nanoTime()),
                    TimeUnit.NANOSECONDS);
            result.whenComplete((ignored, error) -> timer.cancel(false));
        } catch (RejectedExecutionException stopped) {
            result.completeExceptionally(stopped);
        }
        return result;
    }

    private void awaitSession(long lineage, CompletableFuture<Void> result, Ticket ticket) {
        CompletableFuture<Void> changed = null;
        boolean signedOut;
        boolean ended;
        synchronized (this) {
            if (result.isDone()) {
                return;
            }
            signedOut = this.lineage != lineage;
            ended = ticket != null && ticket.completed;
            if (!signedOut && !ended && session == null) {
                changed = admissionChanged;
            }
        }
        if (signedOut) {
            result.completeExceptionally(this.lineageEnded(lineage, null));
        } else if (ended) {
            result.completeExceptionally(ticket.closedWaitError());
        } else if (changed != null) {
            changed.thenRun(() -> awaitSession(lineage, result, ticket));
        } else {
            result.complete(null);
        }
    }

    void changed() {
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
            registerIdentifierLow = RANDOM.nextLong();
            registerIdentifierHigh = RANDOM.nextLong();
        } while (registerIdentifierLow == 0 && registerIdentifierHigh == 0);
        do {
            clientIdLow = RANDOM.nextLong();
            // Keep generated IDs outside the server-only range.
            clientIdHigh = RANDOM.nextLong() & Long.MAX_VALUE;
        } while (clientIdLow == 0 && clientIdHigh == 0);
    }

    /** The first unresolved fetch, if any, for reporting after connection cleanup. */
    public IggyOperationException unresolvedFetch() {
        ConsensusSession owner = owner();
        synchronized (owner) {
            return owner.retainedFetches.keySet().stream()
                    .min(Long::compareUnsigned)
                    .map(id -> new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, id))
                    .orElse(null);
        }
    }

    public IggyOperationException fetchRecovery(long operationId) {
        ConsensusSession owner = owner();
        synchronized (owner) {
            return owner.retainedFetches.containsKey(operationId)
                    ? new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, operationId)
                    : null;
        }
    }

    public record LaneKey(long incarnation, long partition, boolean progress) {
        public static final LaneKey METADATA = new LaneKey(0, 0);

        public LaneKey(long incarnation, long partition) {
            this(incarnation, partition, false);
        }
    }

    /** A copy of the reply body of a proved commit, or no body and the code of a proved committed rejection. */
    private record ProvedOutcome(long operationId, byte[] body, int rejection) {}

    private static final class FetchRecovery {
        private final long bytes;
        private List<LaneKey> lanes;
        private long operationId;
        private boolean retained;
        private boolean abandoned;

        private FetchRecovery(List<LaneKey> lanes, long bytes) {
            this.lanes = lanes;
            this.bytes = bytes;
        }
    }

    /** One lane admission owns one immutable frame; writer aliases keep its bytes charged. */
    public static final class Ticket {
        private final ConsensusSession owner;
        private final List<LaneKey> lanes;
        private final boolean mutation;
        private final long bytes;
        private long generation;
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
        private boolean restarting;
        private FetchRecovery fetch;
        private boolean fetchSettled;
        private boolean committingPoll;
        private CompletableFuture<Void> attemptSettled;

        private Ticket(ConsensusSession owner, List<LaneKey> lanes, boolean mutation, long bytes, long generation) {
            this.owner = owner;
            this.lanes = lanes;
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
                if (fetch != null) {
                    fetch.operationId = operationId;
                }
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
                if (fetch != null) {
                    fetch.operationId = operationId;
                }
                attemptFrame.setLongLE(attemptFrame.readerIndex() + VsrHeaders.REQUEST_ID_OFFSET, operationId);
                int floorOffset = attemptFrame.readerIndex() + VsrHeaders.REQUEST_METADATA_OP_OFFSET;
                long capturedFloor = attemptFrame.getLongLE(floorOffset);
                if (lanes.contains(LaneKey.METADATA)
                        && Long.compareUnsigned(owner.metadataWatermark, capturedFloor) > 0) {
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

        /** Reserves bounded target metadata, independent of session-bound replay frames. */
        public void reserveFetch() {
            synchronized (owner) {
                if (completed) {
                    throw new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
                }
                if (fetch != null) {
                    return;
                }
                long metadataBytes = FETCH_RECOVERY_BYTES + lanes.size() * FETCH_TARGET_BYTES;
                if (owner.fetchReservations >= owner.maxPendingOperations
                        || metadataBytes > owner.maxPendingBytes - owner.fetchBytes) {
                    throw new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
                }
                fetch = new FetchRecovery(lanes, metadataBytes);
                owner.fetchReservations++;
                owner.fetchBytes += metadataBytes;
            }
        }

        /**
         * Marks a poll that stores its offset on the server. A lost reply leaves its outcome unknown, which a later
         * read of the partition shows, so it blocks nothing.
         */
        public void markCommittingPoll() {
            synchronized (owner) {
                committingPoll = true;
            }
        }

        /** Whether this is a poll or fetch that stores the offsets that it reads. */
        public boolean commitsOnRead() {
            synchronized (owner) {
                return committingPoll || fetch != null;
            }
        }

        /** Records {@code settled}, which completes once the attempt that a writer started has its outcome. */
        public void attemptStarted(CompletableFuture<Void> settled) {
            synchronized (owner) {
                attemptSettled = settled;
            }
        }

        /** Completes once the last started attempt has its outcome, or null before any attempt. */
        public CompletableFuture<Void> attemptSettled() {
            synchronized (owner) {
                return attemptSettled;
            }
        }

        public boolean committingFetch() {
            synchronized (owner) {
                return fetch != null;
            }
        }

        /** Settles a valid table before record decoding, retaining only uncertain target sections. */
        public void settleFetch(List<Long> uncertainPartitions) {
            synchronized (owner) {
                fetchSettled = true;
                if (!uncertainPartitions.isEmpty()) {
                    retainFetch(lanes.stream()
                            .filter(lane -> uncertainPartitions.contains(lane.partition()))
                            .toList());
                }
            }
        }

        private void retainFetch(List<LaneKey> affected) {
            if (fetch.retained || fetch.abandoned || completed) {
                return;
            }
            fetch.retained = true;
            fetch.lanes = affected;
            owner.retainedFetches.put(operationId, fetch);
            affected.forEach(lane -> owner.blockedFetchLanes.put(lane, fetch));
        }

        /**
         * Prepares the next write of the call that holds this ticket and started in {@code lineage}, and says
         * whether the ticket moved to a new session, so that the call draws a new request id. A frame that went out
         * under a replaced session, or before the lineage ended, never goes out again, so this throws the
         * uncertainty of its call. A call that has not sent its frame ends with 40 once its lineage ended, also when
         * a sign-out ended its ticket. Else it keeps its lane, and while no session is bound or its link is down,
         * this throws {@code TRANSIENT_NOT_ACCEPTED} and the call starts again, see {@link #takeRestart}. An ended
         * ticket leaves the refusal to the write.
         */
        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        public boolean prepareWrite(boolean linkActive, long lineage) {
            boolean ended;
            boolean signedOut;
            synchronized (owner) {
                restarting = false;
                signedOut = owner.lineage != lineage;
                boolean current = generation == owner.generation && owner.session != null;
                if (!signedOut && (completed || (current && (sent || linkActive)))) {
                    return false;
                }
                ended = sent;
                if (!ended && !signedOut && owner.session != null && linkActive) {
                    generation = owner.generation;
                    releaseFrame();
                    return true;
                }
                restarting = !ended && !signedOut;
            }
            if (ended) {
                throw retain();
            }
            throw signedOut
                    ? owner.lineageEnded(lineage, null)
                    : new IggyServerException(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
        }

        /**
         * Whether the last {@link #prepareWrite} asked the call to start again once a session is bound, see
         * {@link #awaitSession}. Only the first ask after that refusal gets the answer, so an attempt that the
         * restart started and that fails before its write is not taken for another restart.
         */
        public boolean takeRestart() {
            synchronized (owner) {
                boolean restart = restarting;
                restarting = false;
                return restart;
            }
        }

        /**
         * Completes once a session is bound in {@code lineage}, so that a call that has not sent its frame can start
         * again under it. The end of the lineage fails the wait with 40, so the call never runs under a session that
         * the caller opened after it started. A wait still running at {@code deadline} fails with
         * {@code TRANSIENT_NOT_ACCEPTED}, like a lane wait. A ticket that a close ended fails the wait at once.
         */
        public CompletableFuture<Void> awaitSession(long lineage, long deadline, EventExecutor executor) {
            return owner.awaitSession(lineage, deadline, executor, this);
        }

        private RuntimeException closedWaitError() {
            return sent
                    ? new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId)
                    : new IggyNotConnectedException("The connection closed before the request was sent");
        }

        public void startWrite() {
            synchronized (owner) {
                // A frame of a replaced session never goes out, see prepareWrite.
                if (completed || generation != owner.generation) {
                    throw new IggyOperationException(IggyErrorCode.OPERATION_NOT_RETAINED, operationId);
                }
                sent = true;
                writers++;
            }
        }

        /**
         * After a 58 or 89 refusal, an 88 lifecycle refusal or a namespace miss, counts the frame as sent only while
         * an earlier attempt can have taken effect, as in Rust. So a call that no node admitted starts again under a
         * new session instead of ending uncertain.
         */
        public void settleSent(boolean uncertain) {
            synchronized (owner) {
                sent = uncertain;
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

        private boolean retainUnknownFetch() {
            if (fetch == null || !sent || fetchSettled) {
                return false;
            }
            retainFetch(lanes);
            return true;
        }

        private boolean retainMutation() {
            if (completed || abandoned || !mutation || !sent || generation != owner.generation) {
                return false;
            }
            retained = true;
            resolving = false;
            owner.retained.put(operationId, this);
            return true;
        }

        public RuntimeException retain() {
            boolean saved;
            boolean fetchUnknown;
            boolean pollUnknown;
            synchronized (owner) {
                fetchUnknown = retainUnknownFetch();
                saved = retainMutation();
                pollUnknown = committingPoll && sent;
            }
            if (fetchUnknown || pollUnknown) {
                complete();
                return new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, operationId);
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
                if (fetch != null) {
                    if (sent && !fetchSettled) {
                        retainFetch(lanes);
                    }
                    if (!fetch.retained && !fetch.abandoned) {
                        owner.fetchReservations--;
                        owner.fetchBytes -= fetch.bytes;
                    }
                }
                completed = true;
                for (LaneKey lane : lanes) {
                    owner.lanes.remove(lane, this);
                }
                owner.retained.remove(operationId, this);
                if (mutation && !abandoned) {
                    owner.pendingOperations--;
                }
                releaseFrame();
                releaseBytesIfUnused();
            }
            owner.changed();
        }

        private void releaseFrame() {
            if (frame != null) {
                frame.release();
                frame = null;
            }
        }

        private void releaseBytesIfUnused() {
            if (mutation && completed && writers == 0 && !bytesReleased) {
                bytesReleased = true;
                owner.pendingBytes -= bytes;
            }
        }
    }
}
