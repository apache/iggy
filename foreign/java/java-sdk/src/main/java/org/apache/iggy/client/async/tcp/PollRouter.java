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
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    private final Supplier<AsyncTcpConnection> coordinator;
    private final Function<ConnectionInfo, AsyncTcpConnection> connectData;
    private final Map<String, Route> routes = new HashMap<>();
    private final Map<ConnectionInfo, Slot> connections = new HashMap<>();
    private final Set<Poll> pending = new HashSet<>();
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
        poll.result.whenComplete((response, error) -> {
            timeout.cancel(false);
            synchronized (this) {
                pending.remove(poll);
                if (error != null) {
                    routes.remove(poll.key);
                }
            }
            if (error != null) {
                poll.discardConnection();
            }
        });
        attempt(poll);
        return poll.result;
    }

    synchronized CompletableFuture<Void> clearSession(AsyncTcpConnection previous) {
        if (previous != null) {
            observeMetadata(previous.metadataWatermark());
        }
        routes.clear();
        for (Poll poll : Set.copyOf(pending)) {
            poll.result.completeExceptionally(uncommitted());
        }
        CompletableFuture<?>[] closing =
                connections.values().stream().map(Slot::close).toArray(CompletableFuture[]::new);
        connections.clear();
        return CompletableFuture.allOf(closing);
    }

    private void attempt(Poll poll) {
        if (!poll.beginAttempt()) {
            return;
        }
        route(poll).thenCompose(route -> enqueue(route, poll)).whenComplete((response, error) -> {
            if (poll.result.isDone()) {
                if (response != null) {
                    response.release();
                }
                return;
            }
            if (error == null) {
                if (!poll.result.complete(response)) {
                    response.release();
                }
                return;
            }
            synchronized (this) {
                routes.remove(poll.key);
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
                poll.result.completeExceptionally(unwrap(error));
            }
        });
    }

    private CompletableFuture<Route> route(Poll poll) {
        AsyncTcpConnection parent = coordinator.get();
        if (!parent.isAuthenticated()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        synchronized (this) {
            observeMetadata(parent.metadataWatermark());
            Route cached = routes.get(poll.key);
            if (cached != null && routeIsCurrent(cached)) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        return parent.send(CommandCode.Messages.GET_POLL_ROUTING, Unpooled.wrappedBuffer(poll.payload))
                .thenApply(response -> decodeRoute(parent, poll, response))
                .exceptionallyCompose(error -> {
                    if (!connectionFailed(error) || poll.result.isDone()) {
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

    private Route decodeRoute(AsyncTcpConnection parent, Poll poll, ByteBuf response) {
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
                if (!poll.result.isDone() && coordinator.get() == parent) {
                    routes.put(poll.key, route);
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
            if (poll.result.isDone() || !routeIsCurrent(route)) {
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
            if (poll.result.isDone() || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            AsyncTcpConnection data = connection;
            AttachedSession attached = attachment;
            if (data == null || attached == null) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            return data.sendPrimaryPoll(Unpooled.wrappedBuffer(poll.payload), attached.generation)
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
                if (poll.result.isDone()) {
                    close();
                    return CompletableFuture.failedFuture(uncommitted());
                }
                ready = data.connect().thenCompose(ignored -> {
                    if (poll.result.isDone()) {
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
            if (poll.result.isDone() || !routeIsCurrent(route)) {
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
                            if (connection == data && activePoll == poll && !poll.result.isDone()) {
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

    private static final class Poll {
        private final String key;
        private final byte[] payload;
        private final CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        private final long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        private volatile Slot activeSlot;
        private boolean retryingRefusal;

        private Poll(String key, byte[] payload) {
            this.key = key;
            this.payload = payload;
        }

        private void discardConnection() {
            Slot slot = activeSlot;
            if (slot != null) {
                slot.cancel(this);
            }
        }

        private synchronized boolean beginAttempt() {
            if (result.isDone()) {
                return false;
            }
            retryingRefusal = false;
            return true;
        }

        private synchronized boolean retryRefusal() {
            retryingRefusal = true;
            if (deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MILLIS)) {
                result.completeExceptionally(notAccepted());
                return false;
            }
            return !result.isDone();
        }

        private synchronized void expire() {
            result.completeExceptionally(retryingRefusal ? notAccepted() : uncommitted());
        }
    }

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
