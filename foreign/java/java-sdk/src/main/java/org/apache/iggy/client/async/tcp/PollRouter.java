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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Keeps the membership-owning coordinator separate from auto-commit data polls.
 * Polls have no deduplication key; only explicit non-admission permits replay.
 */
final class PollRouter {
    private static final int MAX_ROUTES = 4096;
    private static final int MAX_CONNECTIONS = 256;
    private static final int MAX_PENDING_POLLS = 4096;
    private static final int POLL_PARAMETERS_BYTES = 14;
    private static final int ATTACHMENT_BYTES = 32;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final long RETRY_INTERVAL_MILLIS = 50;

    /**
     * Deferred exchanges hold their connection for the whole readiness wait, so
     * each is an exclusive permit rather than a Netty pool lease. The bound
     * matches the Rust SDK's {@code MAX_DEFERRED_CONNECTIONS}.
     */
    private static final int MAX_DEFERRED_LEASES = 16;

    private static final int DEFERRED_TRAILER_BYTES = 24;
    private static final long NANOS_PER_MICRO = 1000;
    private static final long DEFERRED_RETRY_MAX_INTERVAL_MILLIS = 1000;

    private final Supplier<AsyncTcpConnection> coordinator;
    private final Function<ConnectionInfo, AsyncTcpConnection> connectData;
    private final Map<String, Route> routes = new HashMap<>();
    private final Map<ConnectionInfo, Slot> connections = new HashMap<>();
    private final Set<Poll> pending = new HashSet<>();
    private final Set<DeferredPoll> pendingDeferred = new HashSet<>();
    private final List<DeferredLease> deferredLeases = new ArrayList<>();
    private final Deque<LeaseWaiter> leaseWaiters = new ArrayDeque<>();
    private long metadataWatermark;

    PollRouter(Supplier<AsyncTcpConnection> coordinator, Function<ConnectionInfo, AsyncTcpConnection> connectData) {
        this.coordinator = coordinator;
        this.connectData = connectData;
    }

    CompletableFuture<ByteBuf> poll(ByteBuf payload) {
        Poll poll;
        try {
            String key = ByteBufUtil.hexDump(
                    payload, payload.readerIndex(), payload.readableBytes() - POLL_PARAMETERS_BYTES);
            poll = new Poll(key, ByteBufUtil.getBytes(payload));
        } finally {
            payload.release();
        }
        synchronized (this) {
            if (pending.size() >= MAX_PENDING_POLLS) {
                return CompletableFuture.failedFuture(new IggyClientException("Too many pending primary polls"));
            }
            pending.add(poll);
        }
        var timeout =
                coordinator.get().eventLoop().schedule(poll::expire, POLL_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
        poll.result().whenComplete((response, error) -> {
            timeout.cancel(false);
            synchronized (this) {
                pending.remove(poll);
                if (error != null) {
                    routes.remove(poll.key());
                }
            }
            if (error != null) {
                poll.discardConnection();
            }
        });
        attempt(poll);
        return poll.result();
    }

    /**
     * Runs one deferred poll on a dedicated data connection, so a readiness wait
     * never occupies the coordinator's request drain.
     *
     * <p>A clustered auto-commit poll resolves its primary through command 103 and
     * sends 106. Every other deferred poll reaches the coordinator's own endpoint
     * on an auxiliary channel, attaches the parent's logical session, and sends
     * 105. Both deadlines are the caller's: they are never restarted by a retry,
     * and only an explicit non-admission permits one.
     *
     * @param payload              the ordinary poll body, without the deferred trailer
     * @param options              the readiness, byte and request-time limits
     * @param primary              whether to route to the partition primary
     * @param waitDeadlineNanos    absolute {@link System#nanoTime()} readiness deadline
     * @param requestDeadlineNanos absolute {@link System#nanoTime()} request deadline
     * @return a future completing with the reply body
     */
    CompletableFuture<ByteBuf> pollDeferred(
            ByteBuf payload,
            DeferredPollOptions options,
            boolean primary,
            long waitDeadlineNanos,
            long requestDeadlineNanos) {
        DeferredPoll poll;
        synchronized (this) {
            if (pending.size() + pendingDeferred.size() >= MAX_PENDING_POLLS) {
                payload.release();
                return CompletableFuture.failedFuture(new IggyClientException("Too many pending deferred polls"));
            }
            try {
                poll = new DeferredPoll(
                        deferredRouteKey(payload, primary),
                        ByteBufUtil.getBytes(payload),
                        options,
                        primary,
                        waitDeadlineNanos,
                        requestDeadlineNanos);
            } finally {
                payload.release();
            }
            pendingDeferred.add(poll);
        }
        poll.result().whenComplete((response, error) -> {
            synchronized (this) {
                pendingDeferred.remove(poll);
                if (error != null && poll.key() != null) {
                    routes.remove(poll.key());
                }
            }
            if (error != null) {
                abandonLeaseWork(poll);
            }
        });
        attemptDeferred(poll);
        return poll.result();
    }

    synchronized CompletableFuture<Void> clearSession(AsyncTcpConnection previous) {
        if (previous != null) {
            observeMetadata(previous.metadataWatermark());
        }
        routes.clear();
        failPending();
        return CompletableFuture.allOf(closeSlots(), closeDeferredLeases());
    }

    private void failPending() {
        for (Poll poll : Set.copyOf(pending)) {
            poll.result().completeExceptionally(uncommitted());
        }
        for (DeferredPoll poll : Set.copyOf(pendingDeferred)) {
            poll.result().completeExceptionally(uncommitted());
        }
        for (LeaseWaiter waiter : List.copyOf(leaseWaiters)) {
            waiter.acquired().completeExceptionally(uncommitted());
        }
        leaseWaiters.clear();
    }

    private CompletableFuture<Void> closeSlots() {
        CompletableFuture<?>[] closing =
                connections.values().stream().map(Slot::close).toArray(CompletableFuture[]::new);
        connections.clear();
        return CompletableFuture.allOf(closing);
    }

    private CompletableFuture<Void> closeDeferredLeases() {
        CompletableFuture<?>[] closing = deferredLeases.stream()
                .map(DeferredLease::take)
                .filter(Objects::nonNull)
                .map(AsyncTcpConnection::close)
                .toArray(CompletableFuture[]::new);
        deferredLeases.clear();
        return CompletableFuture.allOf(closing);
    }

    private void attempt(Poll poll) {
        if (!poll.beginAttempt()) {
            return;
        }
        route(poll).thenCompose(route -> enqueue(route, poll)).whenComplete((response, error) -> {
            if (poll.result().isDone()) {
                if (response != null) {
                    response.release();
                }
                return;
            }
            if (error == null) {
                if (!poll.result().complete(response)) {
                    response.release();
                }
                return;
            }
            synchronized (this) {
                routes.remove(poll.key());
            }
            if (isNotAccepted(error)) {
                if (!poll.retryRefusal()) {
                    return;
                }
                coordinator
                        .get()
                        .eventLoop()
                        .schedule(() -> attempt(poll), RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            } else {
                poll.result().completeExceptionally(unwrap(error));
            }
        });
    }

    private void attemptDeferred(DeferredPoll poll) {
        if (poll.result().isDone()) {
            return;
        }
        if (poll.requestDeadlineNanos - System.nanoTime() <= 0) {
            poll.result().completeExceptionally(deferredTimeout());
            return;
        }
        deferredRoute(poll)
                .thenCompose(route -> runDeferred(route, poll))
                .whenComplete((response, error) -> completeDeferredAttempt(poll, response, error));
    }

    private void completeDeferredAttempt(DeferredPoll poll, ByteBuf response, Throwable error) {
        if (poll.result().isDone()) {
            if (response != null) {
                response.release();
            }
            return;
        }
        if (error == null) {
            if (!poll.result().complete(response)) {
                response.release();
            }
            return;
        }
        synchronized (this) {
            if (poll.key() != null) {
                routes.remove(poll.key());
            }
        }
        // Only an explicit refusal proves the poll was never admitted. Anything
        // else - a lost reply, a disconnect, a malformed body - may have been
        // served and auto-committed, so replaying it would skip messages.
        if (!isNotAccepted(error) || !retryAfterRefusal(poll)) {
            poll.result().completeExceptionally(unwrap(error));
        }
    }

    /** Schedules the next attempt inside the original deadline, never past it. */
    private boolean retryAfterRefusal(DeferredPoll poll) {
        long backoffMillis = poll.nextBackoffMillis();
        AsyncTcpConnection parent = coordinator.get();
        long backoffNanos = TimeUnit.MILLISECONDS.toNanos(backoffMillis);
        if (parent == null || poll.requestDeadlineNanos - System.nanoTime() <= backoffNanos) {
            return false;
        }
        try {
            parent.eventLoop().schedule(() -> attemptDeferred(poll), backoffMillis, TimeUnit.MILLISECONDS);
            return true;
        } catch (RuntimeException loopGone) {
            return false;
        }
    }

    /**
     * A clustered auto-commit poll asks the coordinator which node owns the
     * partition. Every other deferred poll stays on the coordinator's endpoint
     * and builds its attachment locally: command 103 refuses a manual poll, so
     * it cannot supply one.
     */
    private CompletableFuture<Route> deferredRoute(DeferredPoll poll) {
        if (poll.primary) {
            return route(poll);
        }
        AsyncTcpConnection parent = coordinator.get();
        if (parent == null || !parent.isAuthenticated()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        var snapshot = parent.sessionSnapshot();
        if (snapshot.isEmpty()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        var session = snapshot.get();
        synchronized (this) {
            observeMetadata(session.metadataWatermark());
            Attachment attachment = new Attachment(
                            session.clientIdLow(),
                            session.clientIdHigh(),
                            session.session(),
                            session.metadataWatermark())
                    .withWatermark(metadataWatermark);
            return CompletableFuture.completedFuture(
                    new Route(parent.endpoint(), attachment, parent, session.generation()));
        }
    }

    private CompletableFuture<ByteBuf> runDeferred(Route route, DeferredPoll poll) {
        return acquireLease(route, poll).thenCompose(lease -> prepareLease(lease, route, poll)
                .thenCompose(ignored -> sendDeferred(lease, route, poll))
                .whenComplete((response, error) -> releaseLease(lease, poll, error)));
    }

    /**
     * Takes one of the 16 exclusive deferred permits. The Netty pool lease is
     * released once a frame is dispatched, so it cannot express ownership that
     * spans a readiness wait and its reply body.
     */
    private CompletableFuture<DeferredLease> acquireLease(Route route, DeferredPoll poll) {
        CompletableFuture<DeferredLease> acquired = new CompletableFuture<>();
        AsyncTcpConnection stale = null;
        DeferredLease ready;
        LeaseWaiter waiter = null;
        synchronized (this) {
            ready = idleLease(route.endpoint());
            if (ready == null && deferredLeases.size() < MAX_DEFERRED_LEASES) {
                ready = new DeferredLease(route.endpoint());
                deferredLeases.add(ready);
            }
            if (ready == null) {
                ready = idleLease(null);
                if (ready != null) {
                    stale = ready.repoint(route.endpoint());
                }
            }
            if (ready == null) {
                waiter = new LeaseWaiter(route.endpoint(), poll, acquired);
                leaseWaiters.add(waiter);
                poll.waiter = waiter;
            } else {
                ready.owner = poll;
                poll.lease = ready;
            }
        }
        if (stale != null) {
            stale.close();
        }
        if (ready != null) {
            acquired.complete(ready);
            return acquired;
        }
        return awaitLease(poll, waiter, acquired);
    }

    /** Requires the monitor. A null endpoint matches any idle lease. */
    private DeferredLease idleLease(ConnectionInfo endpoint) {
        for (DeferredLease lease : deferredLeases) {
            if (lease.owner == null && (endpoint == null || endpoint.equals(lease.endpoint))) {
                return lease;
            }
        }
        return null;
    }

    private CompletableFuture<DeferredLease> awaitLease(
            DeferredPoll poll, LeaseWaiter waiter, CompletableFuture<DeferredLease> acquired) {
        AsyncTcpConnection parent = coordinator.get();
        if (parent == null) {
            failWaiter(waiter, notAccepted());
            return acquired;
        }
        long remaining = Math.max(0, poll.requestDeadlineNanos - System.nanoTime());
        try {
            var expiry = parent.eventLoop()
                    .schedule(() -> failWaiter(waiter, deferredTimeout()), remaining, TimeUnit.NANOSECONDS);
            acquired.whenComplete((lease, error) -> expiry.cancel(false));
        } catch (RuntimeException loopGone) {
            failWaiter(waiter, notAccepted());
        }
        return acquired;
    }

    private void failWaiter(LeaseWaiter waiter, Throwable error) {
        boolean queued;
        synchronized (this) {
            queued = leaseWaiters.remove(waiter);
        }
        if (queued) {
            waiter.acquired().completeExceptionally(error);
        }
    }

    private CompletableFuture<Void> prepareLease(DeferredLease lease, Route route, DeferredPoll poll) {
        if (poll.result().isDone() || !routeIsCurrent(route)) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        CompletableFuture<Void> ready;
        if (lease.connection == null) {
            AsyncTcpConnection data = connectData.apply(route.endpoint());
            lease.connection = data;
            ready = data.connectWithoutHeartbeat().thenCompose(ignored -> {
                if (poll.result().isDone() || lease.connection != data) {
                    // A connection that finished dialling after the caller gave
                    // up is closed here rather than installed in the lease.
                    data.close();
                    return CompletableFuture.failedFuture(notAccepted());
                }
                var authentication = route.parent().authenticationSnapshot();
                if (authentication.isEmpty()) {
                    return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated"));
                }
                var login = authentication.get();
                return data.send(login.commandCode(), login.payload()).thenAccept(ByteBuf::release);
            });
        } else {
            ready = CompletableFuture.completedFuture(null);
        }
        return ready.thenCompose(ignored -> attachLease(lease, route, poll)).exceptionallyCompose(error -> {
            // Setup failed before the poll was written, so nothing was
            // admitted and the caller may route again.
            lease.discard(poll);
            return CompletableFuture.failedFuture(connectionFailed(error) ? notAccepted() : unwrap(error));
        });
    }

    private CompletableFuture<Void> attachLease(DeferredLease lease, Route route, DeferredPoll poll) {
        if (poll.result().isDone() || !routeIsCurrent(route)) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        AsyncTcpConnection data = lease.connection;
        if (data == null) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        AttachedSession current = lease.attachment;
        if (current != null && current.covers(data, route.attachment())) {
            return CompletableFuture.completedFuture(null);
        }
        return data.send(
                        CommandCode.System.ATTACH_CONSUMER_SESSION,
                        route.attachment().encode())
                .thenAccept(response -> {
                    response.release();
                    if (lease.connection == data
                            && lease.owner == poll
                            && !poll.result().isDone()) {
                        lease.attachment = new AttachedSession(data, data.sessionGeneration(), route.attachment());
                    }
                });
    }

    private CompletableFuture<ByteBuf> sendDeferred(DeferredLease lease, Route route, DeferredPoll poll) {
        if (poll.result().isDone() || !routeIsCurrent(route)) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        AsyncTcpConnection data = lease.connection;
        if (data == null) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        // Both budgets are reduced by the same elapsed time before every send,
        // so a retry inherits what is left instead of restarting the wait.
        long now = System.nanoTime();
        long requestTimeoutMicros = Math.max(0, poll.requestDeadlineNanos - now) / NANOS_PER_MICRO;
        if (requestTimeoutMicros == 0) {
            return CompletableFuture.failedFuture(deferredTimeout());
        }
        long waitMicros = Math.max(0, poll.waitDeadlineNanos - now) / NANOS_PER_MICRO;
        int code = poll.primary
                ? CommandCode.Messages.POLL_DEFERRED_ON_PRIMARY.getValue()
                : CommandCode.Messages.POLL_DEFERRED.getValue();
        ByteBuf wire = encodeDeferred(poll, waitMicros, requestTimeoutMicros);
        // Once the poll is on the wire its outcome is reported as it happened: a
        // locally expired budget stays a timeout and a server code stays that
        // code. Nothing here is replayed, so no failure needs relabelling.
        return data.sendDeferredPoll(code, wire, poll.requestDeadlineNanos, poll.options.maxBytes());
    }

    private static ByteBuf encodeDeferred(DeferredPoll poll, long waitMicros, long requestTimeoutMicros) {
        return Unpooled.buffer(poll.payload().length + DEFERRED_TRAILER_BYTES)
                .writeBytes(poll.payload())
                .writeLongLE(waitMicros)
                .writeIntLE((int) poll.options.minCount())
                .writeIntLE((int) poll.options.maxBytes())
                .writeLongLE(requestTimeoutMicros);
    }

    private void releaseLease(DeferredLease lease, DeferredPoll poll, Throwable error) {
        AsyncTcpConnection discarded;
        LeaseWaiter next;
        synchronized (this) {
            if (lease.owner != poll) {
                return;
            }
            discarded = settleLease(lease, poll, error);
            lease.owner = null;
            next = leaseWaiters.poll();
            if (next != null) {
                discarded = handOverLease(lease, next, discarded);
            }
        }
        if (discarded != null) {
            discarded.close();
        }
        if (next != null) {
            next.acquired().complete(lease);
        }
    }

    /** Requires the monitor. Returns a connection the caller must close. */
    private static AsyncTcpConnection settleLease(DeferredLease lease, DeferredPoll poll, Throwable error) {
        if (error == null) {
            return null;
        }
        if (connectionFailed(error) || poll.result().isCancelled()) {
            return lease.take();
        }
        if (isNotAccepted(error)) {
            // The server refused this exchange, so the attachment it was made
            // under is no longer known to be installed.
            lease.attachment = null;
        }
        return null;
    }

    /**
     * Requires the monitor. A repointed lease has no connection left to take, so
     * at most one connection needs closing.
     */
    private static AsyncTcpConnection handOverLease(
            DeferredLease lease, LeaseWaiter next, AsyncTcpConnection discarded) {
        AsyncTcpConnection repointed = next.endpoint().equals(lease.endpoint) ? null : lease.repoint(next.endpoint());
        lease.owner = next.poll();
        next.poll().lease = lease;
        return discarded != null ? discarded : repointed;
    }

    private void abandonLeaseWork(DeferredPoll poll) {
        LeaseWaiter waiter = poll.waiter;
        if (waiter != null) {
            failWaiter(waiter, new CancellationException());
        }
        DeferredLease lease = poll.lease;
        if (lease != null) {
            // A reply that arrives for a poll nobody waits for would desynchronize
            // the next exchange on this channel, so the channel goes with it.
            lease.discard(poll);
        }
    }

    private static String deferredRouteKey(ByteBuf payload, boolean primary) {
        return primary
                ? ByteBufUtil.hexDump(payload, payload.readerIndex(), payload.readableBytes() - POLL_PARAMETERS_BYTES)
                : null;
    }

    private static IggyTimeoutException deferredTimeout() {
        return new IggyTimeoutException("Deferred poll exceeded its request timeout");
    }

    private CompletableFuture<Route> route(RoutedPoll poll) {
        AsyncTcpConnection parent = coordinator.get();
        if (!parent.isAuthenticated()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        synchronized (this) {
            observeMetadata(parent.metadataWatermark());
            Route cached = routes.get(poll.key());
            if (cached != null && routeIsCurrent(cached)) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        return parent.send(CommandCode.Messages.GET_POLL_ROUTING, Unpooled.wrappedBuffer(poll.payload()))
                .thenApply(response -> decodeRoute(parent, poll, response))
                .exceptionallyCompose(error -> {
                    if (!connectionFailed(error) || poll.result().isDone()) {
                        return CompletableFuture.failedFuture(unwrap(error));
                    }
                    return coordinator
                            .get()
                            .send(CommandCode.System.PING, Unpooled.EMPTY_BUFFER)
                            .thenApply(response -> {
                                response.release();
                                throw notAccepted();
                            });
                });
    }

    private Route decodeRoute(AsyncTcpConnection parent, RoutedPoll poll, ByteBuf response) {
        try {
            if (response.readableBytes() < ATTACHMENT_BYTES) {
                throw new IggyClientException("Truncated primary poll session attachment");
            }
            Attachment attachment = new Attachment(
                    response.readLongLE(), response.readLongLE(), response.readLongLE(), response.readLongLE());
            var node = BytesDeserializer.readClusterNode(response);
            if (response.isReadable() || node.endpoints().tcp() == 0) {
                throw new IggyClientException("Invalid TCP primary poll routing response");
            }
            synchronized (this) {
                observeMetadata(parent.metadataWatermark());
                Route route = new Route(
                        new ConnectionInfo(node.ip(), node.endpoints().tcp()),
                        attachment.withWatermark(metadataWatermark),
                        parent,
                        parent.sessionGeneration());
                if (routes.size() >= MAX_ROUTES) {
                    routes.clear();
                }
                if (!poll.result().isDone() && coordinator.get() == parent) {
                    routes.put(poll.key(), route);
                }
                return route;
            }
        } catch (IndexOutOfBoundsException | IggyMalformedResponseException error) {
            throw new IggyClientException("Invalid TCP primary poll routing response", error);
        } finally {
            response.release();
        }
    }

    private synchronized boolean routeIsCurrent(Route route) {
        AsyncTcpConnection parent = coordinator.get();
        observeMetadata(parent.metadataWatermark());
        return route.parent == parent
                && route.generation == parent.sessionGeneration()
                && Long.compareUnsigned(route.attachment.watermark, metadataWatermark) >= 0;
    }

    private CompletableFuture<ByteBuf> enqueue(Route route, Poll poll) {
        synchronized (this) {
            Slot slot = connections.get(route.endpoint);
            if (slot == null) {
                if (connections.size() >= MAX_CONNECTIONS && !evictIdleConnection()) {
                    return CompletableFuture.failedFuture(notAccepted());
                }
                slot = new Slot();
                connections.put(route.endpoint, slot);
            }
            return slot.poll(route, poll);
        }
    }

    private boolean evictIdleConnection() {
        var iterator = connections.entrySet().iterator();
        while (iterator.hasNext()) {
            Slot slot = iterator.next().getValue();
            if (slot.isIdle()) {
                iterator.remove();
                slot.close();
                return true;
            }
        }
        return false;
    }

    private void observeMetadata(long watermark) {
        if (Long.compareUnsigned(watermark, metadataWatermark) > 0) {
            metadataWatermark = watermark;
        }
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? unwrap(error.getCause()) : error;
    }

    private static boolean isNotAccepted(Throwable error) {
        return unwrap(error) instanceof IggyServerException server
                && server.getRawErrorCode() == AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED;
    }

    private static boolean connectionFailed(Throwable error) {
        Throwable cause = unwrap(error);
        return cause instanceof IggyConnectionException
                || cause instanceof IggyNotConnectedException
                || cause instanceof IggyTimeoutException
                || cause instanceof IOException
                || (cause instanceof IggyServerException server
                        && (server.getRawErrorCode() == IggyErrorCode.STALE_CLIENT.getCode()
                                || server.getRawErrorCode() == IggyErrorCode.UNAUTHENTICATED.getCode()));
    }

    private static IggyServerException notAccepted() {
        return IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0]);
    }

    private static IggyServerException uncommitted() {
        return IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_COMMITTED, new byte[0]);
    }

    private final class Slot {
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        private volatile AsyncTcpConnection connection;
        private volatile AttachedSession attachment;
        private Poll activePoll;

        CompletableFuture<ByteBuf> poll(Route route, Poll poll) {
            CompletableFuture<Void> previous;
            CompletableFuture<Void> gate = new CompletableFuture<>();
            synchronized (this) {
                previous = tail;
                tail = gate;
            }
            CompletableFuture<ByteBuf> result =
                    previous.handle((ignored, error) -> null).thenCompose(ignored -> pollOnConnection(route, poll));
            result.whenComplete((response, error) -> gate.complete(null));
            return result;
        }

        synchronized boolean isIdle() {
            return tail.isDone();
        }

        private CompletableFuture<ByteBuf> pollOnConnection(Route route, Poll poll) {
            if (poll.result().isDone() || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            synchronized (this) {
                activePoll = poll;
                poll.activeSlot = this;
            }
            return prepare(route, poll)
                    .exceptionallyCompose(error -> {
                        close();
                        return CompletableFuture.failedFuture(connectionFailed(error) ? notAccepted() : unwrap(error));
                    })
                    .thenCompose(ignored -> sendPoll(route, poll))
                    .whenComplete((response, error) -> {
                        synchronized (this) {
                            activePoll = null;
                            poll.activeSlot = null;
                        }
                    });
        }

        private CompletableFuture<ByteBuf> sendPoll(Route route, Poll poll) {
            if (poll.result().isDone() || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            AsyncTcpConnection data = connection;
            AttachedSession attached = attachment;
            if (data == null || attached == null) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            return data.sendPrimaryPoll(Unpooled.wrappedBuffer(poll.payload()), attached.generation)
                    .whenComplete((response, error) -> {
                        if (error != null) {
                            if (isNotAccepted(error)) {
                                attachment = null;
                            } else {
                                close();
                            }
                        }
                    })
                    .exceptionallyCompose(error ->
                            CompletableFuture.failedFuture(connectionFailed(error) ? uncommitted() : unwrap(error)));
        }

        private CompletableFuture<Void> prepare(Route route, Poll poll) {
            CompletableFuture<Void> ready;
            if (connection == null) {
                AsyncTcpConnection data = connectData.apply(route.endpoint);
                connection = data;
                if (poll.result().isDone()) {
                    close();
                    return CompletableFuture.failedFuture(uncommitted());
                }
                ready = data.connect().thenCompose(ignored -> {
                    if (poll.result().isDone()) {
                        return CompletableFuture.failedFuture(uncommitted());
                    }
                    var authentication = route.parent.authenticationSnapshot();
                    if (authentication.isEmpty()) {
                        return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated"));
                    }
                    var login = authentication.get();
                    return data.send(login.commandCode(), login.payload()).thenAccept(ByteBuf::release);
                });
            } else {
                ready = CompletableFuture.completedFuture(null);
            }
            return ready.thenCompose(ignored -> attach(route, poll));
        }

        private CompletableFuture<Void> attach(Route route, Poll poll) {
            if (poll.result().isDone() || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            AsyncTcpConnection data = connection;
            if (data == null) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            AttachedSession current = attachment;
            if (current != null && current.covers(data, route.attachment)) {
                return CompletableFuture.completedFuture(null);
            }
            return data.send(CommandCode.System.ATTACH_CONSUMER_SESSION, route.attachment.encode())
                    .thenAccept(response -> {
                        response.release();
                        synchronized (this) {
                            if (connection == data
                                    && activePoll == poll
                                    && !poll.result().isDone()) {
                                attachment = new AttachedSession(data, data.sessionGeneration(), route.attachment);
                            }
                        }
                    });
        }

        CompletableFuture<Void> close() {
            AsyncTcpConnection previous;
            synchronized (this) {
                previous = connection;
                connection = null;
                attachment = null;
            }
            return previous == null ? CompletableFuture.completedFuture(null) : previous.close();
        }

        void cancel(Poll poll) {
            AsyncTcpConnection previous;
            synchronized (this) {
                if (activePoll != poll) {
                    return;
                }
                previous = connection;
                connection = null;
                attachment = null;
            }
            if (previous != null) {
                previous.close();
            }
        }
    }

    /**
     * A poll that resolves a route through command 103 and replays nothing on
     * its own: the key identifies the route cache entry, never a dedup slot.
     */
    private abstract static class RoutedPoll {
        private final String key;
        private final byte[] payload;
        private final CompletableFuture<ByteBuf> result = new CompletableFuture<>();

        RoutedPoll(String key, byte[] payload) {
            this.key = key;
            this.payload = payload;
        }

        String key() {
            return key;
        }

        byte[] payload() {
            return payload;
        }

        CompletableFuture<ByteBuf> result() {
            return result;
        }
    }

    private static final class Poll extends RoutedPoll {
        private final long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        private volatile Slot activeSlot;
        private boolean retryingRefusal;

        private Poll(String key, byte[] payload) {
            super(key, payload);
        }

        private void discardConnection() {
            Slot slot = activeSlot;
            if (slot != null) {
                slot.cancel(this);
            }
        }

        private synchronized boolean beginAttempt() {
            if (result().isDone()) {
                return false;
            }
            retryingRefusal = false;
            return true;
        }

        private synchronized boolean retryRefusal() {
            retryingRefusal = true;
            if (deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MILLIS)) {
                result().completeExceptionally(notAccepted());
                return false;
            }
            return !result().isDone();
        }

        private synchronized void expire() {
            result().completeExceptionally(retryingRefusal ? notAccepted() : uncommitted());
        }
    }

    private static final class DeferredPoll extends RoutedPoll {
        private final DeferredPollOptions options;
        private final boolean primary;
        private final long waitDeadlineNanos;
        private final long requestDeadlineNanos;
        private long retryIntervalMillis = RETRY_INTERVAL_MILLIS;
        private volatile LeaseWaiter waiter;
        private volatile DeferredLease lease;

        private DeferredPoll(
                String key,
                byte[] payload,
                DeferredPollOptions options,
                boolean primary,
                long waitDeadlineNanos,
                long requestDeadlineNanos) {
            super(key, payload);
            this.options = options;
            this.primary = primary;
            this.waitDeadlineNanos = waitDeadlineNanos;
            this.requestDeadlineNanos = requestDeadlineNanos;
        }

        private synchronized long nextBackoffMillis() {
            long current = retryIntervalMillis;
            retryIntervalMillis = Math.min(retryIntervalMillis * 2, DEFERRED_RETRY_MAX_INTERVAL_MILLIS);
            return current;
        }
    }

    /**
     * One exclusive deferred permit and the connection it currently owns. The
     * owner and endpoint are guarded by the router's monitor; the connection and
     * attachment are read by the owning exchange and cleared by a cancellation.
     */
    private static final class DeferredLease {
        private volatile ConnectionInfo endpoint;
        private volatile AsyncTcpConnection connection;
        private volatile AttachedSession attachment;
        private volatile DeferredPoll owner;

        private DeferredLease(ConnectionInfo endpoint) {
            this.endpoint = endpoint;
        }

        private AsyncTcpConnection take() {
            AsyncTcpConnection previous = connection;
            connection = null;
            attachment = null;
            return previous;
        }

        private AsyncTcpConnection repoint(ConnectionInfo next) {
            endpoint = next;
            return take();
        }

        private void discard(DeferredPoll poll) {
            AsyncTcpConnection previous;
            synchronized (this) {
                if (owner != poll) {
                    return;
                }
                previous = take();
            }
            if (previous != null) {
                previous.close();
            }
        }
    }

    private record LeaseWaiter(ConnectionInfo endpoint, DeferredPoll poll, CompletableFuture<DeferredLease> acquired) {}

    private record AttachedSession(AsyncTcpConnection connection, long generation, Attachment attachment) {
        boolean covers(AsyncTcpConnection data, Attachment required) {
            return connection == data && generation == data.sessionGeneration() && attachment.covers(required);
        }
    }

    private record Route(ConnectionInfo endpoint, Attachment attachment, AsyncTcpConnection parent, long generation) {}

    private record Attachment(long clientLow, long clientHigh, long session, long watermark) {
        boolean covers(Attachment required) {
            return clientLow == required.clientLow
                    && clientHigh == required.clientHigh
                    && session == required.session
                    && Long.compareUnsigned(watermark, required.watermark) >= 0;
        }

        Attachment withWatermark(long floor) {
            return Long.compareUnsigned(watermark, floor) >= 0
                    ? this
                    : new Attachment(clientLow, clientHigh, session, floor);
        }

        ByteBuf encode() {
            return Unpooled.buffer(ATTACHMENT_BYTES)
                    .writeLongLE(clientLow)
                    .writeLongLE(clientHigh)
                    .writeLongLE(session)
                    .writeLongLE(watermark);
        }
    }
}
