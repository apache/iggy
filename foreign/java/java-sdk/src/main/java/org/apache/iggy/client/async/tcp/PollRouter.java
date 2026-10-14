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
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.vsr.VsrRequestEncoder;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.partition.PartitionRoute;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Routes partition requests through shared-session connections to their primary,
 * keeping the membership-owning coordinator separate.
 * Only explicit non-admission permits rerouting.
 */
final class PollRouter {
    static final int POLL_PARAMETERS_BYTES = 14;

    static final Duration POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final int MAX_ROUTES = 4096;
    private static final int MAX_CONNECTIONS = 256;
    private static final int MAX_PENDING_POLLS = 4096;
    private static final long RETRY_INTERVAL_MILLIS = 50;

    private final Supplier<AsyncTcpConnection> coordinator;
    private final Function<ConnectionInfo, AsyncTcpConnection> connectData;
    private final Duration pollTimeout;
    private final Map<String, Route> routes = new HashMap<>();
    private final Map<ConnectionInfo, Slot> connections = new HashMap<>();
    private final Set<Poll> pending = new HashSet<>();
    private volatile int rosterSize;
    private final Map<Integer, ConnectionInfo> replicas = new HashMap<>();
    private final Map<String, Long> partitionCounts = new HashMap<>();

    PollRouter(Supplier<AsyncTcpConnection> coordinator, Function<ConnectionInfo, AsyncTcpConnection> connectData) {
        this(coordinator, connectData, POLL_TIMEOUT);
    }

    PollRouter(
            Supplier<AsyncTcpConnection> coordinator,
            Function<ConnectionInfo, AsyncTcpConnection> connectData,
            Duration pollTimeout) {
        this.coordinator = coordinator;
        this.connectData = connectData;
        this.pollTimeout = pollTimeout;
    }

    CompletableFuture<ByteBuf> poll(ByteBuf payload, Optional<PartitionContext> context) {
        return partitionRequest(CommandCode.Messages.POLL.getValue(), payload, context);
    }

    CompletableFuture<ByteBuf> partitionRequest(int command, ByteBuf payload, Optional<PartitionContext> context) {
        byte[] request = takeBytes(payload);
        try {
            byte[] query = partitionQuery(command, request);
            return send(new Poll(query, command, request, pollTimeout, context));
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    CompletableFuture<ByteBuf> sendMessages(ByteBuf payload, ByteBuf query) {
        byte[] request = takeBytes(payload);
        byte[] target = takeBytes(query);
        return send(new Poll(target, CommandCode.Messages.SEND.getValue(), request, pollTimeout, Optional.empty()));
    }

    CompletableFuture<Long> partitionCount(StreamId stream, TopicId topic) {
        ByteBuf query = BytesSerializer.toBytes(stream);
        ByteBuf topicBytes = BytesSerializer.toBytes(topic);
        try {
            query.writeBytes(topicBytes);
            query.writeIntLE(0);
        } finally {
            topicBytes.release();
        }
        byte[] target = takeBytes(query);
        String topicKey = ByteBufUtil.hexDump(target, 0, target.length - Integer.BYTES);
        synchronized (this) {
            Long count = partitionCounts.get(topicKey);
            if (count != null) {
                return CompletableFuture.completedFuture(count);
            }
        }
        Poll lookup = new Poll(
                target, CommandCode.Messages.GET_PARTITION_ROUTE.getValue(), target, pollTimeout, Optional.empty());
        return send(lookup).thenApply(response -> {
            response.release();
            return lookup.partitionsCount;
        });
    }

    synchronized void clear() {
        routes.clear();
        partitionCounts.clear();
    }

    synchronized void observeTopology(ClusterMetadata metadata) {
        replicas.clear();
        rosterSize = metadata.nodes().size();
        for (var node : metadata.nodes()) {
            if (node.endpoints().tcp() != 0) {
                replicas.put(
                        node.replicaId(),
                        new ConnectionInfo(node.ip(), node.endpoints().tcp()));
            }
        }
    }

    private static byte[] partitionQuery(int command, byte[] request) {
        ByteBuf body = Unpooled.wrappedBuffer(request);
        try {
            int consumerKind = body.readUnsignedByte();
            skipIdentifier(body);
            int streamStart = body.readerIndex();
            skipIdentifier(body);
            skipIdentifier(body);
            int partitionStart = body.readerIndex();
            boolean explicit = body.readUnsignedByte() != 0;
            long partition = body.readUnsignedIntLE();
            if (!explicit) {
                if (command == CommandCode.ConsumerOffset.STORE.getValue()
                        || command == CommandCode.ConsumerOffset.DELETE.getValue()
                        || (command == CommandCode.Messages.POLL.getValue() && consumerKind == 2)) {
                    throw new IggyInvalidArgumentException("A group poll or offset mutation requires a partition id");
                }
                partition = 0;
                body.setByte(partitionStart, 1);
                body.setIntLE(partitionStart + 1, 0);
            }
            ByteBuf query = Unpooled.buffer(partitionStart - streamStart + Integer.BYTES);
            query.writeBytes(request, streamStart, partitionStart - streamStart).writeIntLE((int) partition);
            return takeBytes(query);
        } catch (IndexOutOfBoundsException error) {
            throw new IggyInvalidArgumentException("Truncated partition target");
        } finally {
            body.release();
        }
    }

    private static void skipIdentifier(ByteBuf body) {
        int kind = body.readUnsignedByte();
        int length = body.readUnsignedByte();
        if ((kind == 1 && length != Integer.BYTES) || (kind != 1 && kind != 2) || length == 0) {
            throw new IggyInvalidArgumentException("Invalid partition target identifier");
        }
        body.skipBytes(length);
    }

    private static byte[] takeBytes(ByteBuf payload) {
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            payload.release();
        }
    }

    private CompletableFuture<ByteBuf> send(Poll poll) {
        synchronized (this) {
            if (pending.size() >= MAX_PENDING_POLLS) {
                return CompletableFuture.failedFuture(
                        new IggyClientException("Too many pending primary polls and offset writes"));
            }
            pending.add(poll);
        }
        var timeout = coordinator.get().eventLoop().schedule(poll::expire, pollTimeout.toNanos(), TimeUnit.NANOSECONDS);
        poll.result.whenComplete((response, error) -> {
            timeout.cancel(false);
            synchronized (this) {
                pending.remove(poll);
                if (error != null && poll.dataCommand != CommandCode.Messages.SEND.getValue()) {
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

    synchronized CompletableFuture<Void> clearSession() {
        clear();
        for (Poll poll : Set.copyOf(pending)) {
            poll.result.completeExceptionally(uncommitted());
        }
        CompletableFuture<?>[] closing =
                connections.values().stream().map(Slot::close).toArray(CompletableFuture[]::new);
        connections.clear();
        return CompletableFuture.allOf(closing);
    }

    private void attempt(Poll poll) {
        if (poll.result.isDone()) {
            return;
        }
        poll.dataSent = false;
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
            failedAttempt(poll, error);
        });
    }

    private void failedAttempt(Poll poll, Throwable error) {
        if (poll.retryingRefusal && unwrap(error) instanceof IggyTimeoutException) {
            poll.result.completeExceptionally(notAccepted());
            return;
        }
        IggyServerException refusal = AsyncTcpConnection.findServerError(error);
        boolean historyRefresh = poll.canRefreshHistory(refusal);
        boolean notPrimary = refusal != null && refusal.getRawErrorCode() == AsyncTcpConnection.NOT_PRIMARY;
        invalidateRoute(poll, error, historyRefresh, notPrimary);
        if (poll.isLifecycleRefusal(refusal)) {
            retryLifecycle(poll, refusal);
            return;
        }
        if (!poll.terminalRoutingRefusal && (isNotAccepted(error) || notPrimary || historyRefresh)) {
            retry(poll, historyRefresh, notPrimary);
        } else {
            poll.result.completeExceptionally(unwrap(error));
        }
    }

    private synchronized void invalidateRoute(Poll poll, Throwable error, boolean historyRefresh, boolean notPrimary) {
        if (historyRefresh) {
            routes.keySet().removeIf(key -> key.startsWith(poll.topicKey()));
            partitionCounts.remove(poll.topicKey());
        } else if (isNotAccepted(error) || notPrimary || poll.dataCommand != CommandCode.Messages.SEND.getValue()) {
            routes.remove(poll.key);
        }
    }

    private void retryLifecycle(Poll poll, IggyServerException refusal) {
        long delay = poll.lifecycleRetryMillis;
        if (poll.deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(delay)) {
            poll.result.completeExceptionally(refusal);
            return;
        }
        poll.lifecycleRefusal = refusal;
        poll.state.renewRequestId();
        poll.lifecycleRetryMillis = Math.min(delay * 2, AsyncTcpConnection.LIFECYCLE_RETRY_MAX_INTERVAL_MS);
        coordinator.get().eventLoop().schedule(() -> attempt(poll), delay, TimeUnit.MILLISECONDS);
    }

    private void retry(Poll poll, boolean historyRefresh, boolean notPrimary) {
        if (historyRefresh) {
            poll.refreshedHistory = true;
            poll.context.set(null);
            poll.state = new AsyncTcpConnection.TransientFailoverState();
        }
        if (!poll.retryRefusal()) {
            return;
        }
        long delay = (notPrimary && !poll.notPrimarySeen) || historyRefresh ? 0 : RETRY_INTERVAL_MILLIS;
        poll.notPrimarySeen |= notPrimary;
        coordinator.get().eventLoop().schedule(() -> attempt(poll), delay, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Route> route(Poll poll) {
        AsyncTcpConnection parent = coordinator.get();
        if (!parent.isAuthenticated()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        synchronized (this) {
            Route cached = routes.get(poll.key);
            if (cached != null && routeIsCurrent(cached)) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        CompletableFuture<Void> roster = rosterSize == 0
                ? refreshRoster(parent, poll.deadline).exceptionallyCompose(error -> {
                    poll.terminalRoutingRefusal = true;
                    return CompletableFuture.failedFuture(unwrap(error));
                })
                : CompletableFuture.completedFuture(null);
        return roster.thenCompose(ignored -> parent.sendRouted(
                        CommandCode.Messages.GET_PARTITION_ROUTE.getValue(),
                        Unpooled.wrappedBuffer(poll.routingBody),
                        0,
                        poll.deadline,
                        new AsyncTcpConnection.TransientFailoverState(new PartitionContext(
                                BigInteger.ZERO, BigInteger.ZERO, BigInteger.valueOf(parent.metadataWatermark())))))
                .thenCompose(response -> {
                    PartitionRoute decoded;
                    try {
                        decoded = PartitionRoute.decode(response);
                    } finally {
                        response.release();
                    }
                    CompletableFuture<Void> known;
                    synchronized (this) {
                        known = replicas.containsKey(decoded.primaryReplica()) || connectData == null
                                ? CompletableFuture.completedFuture(null)
                                : refreshRoster(parent, poll.deadline);
                    }
                    return known.thenApply(ignored -> cacheRoute(parent, poll, decoded));
                });
    }

    private CompletableFuture<Void> refreshRoster(AsyncTcpConnection parent, long deadline) {
        return parent.sendRouted(
                        CommandCode.System.GET_CLUSTER_METADATA.getValue(),
                        Unpooled.EMPTY_BUFFER,
                        0,
                        deadline,
                        new AsyncTcpConnection.TransientFailoverState())
                .thenAccept(response -> {
                    try {
                        observeTopology(BytesDeserializer.readClusterMetadata(response));
                    } finally {
                        response.release();
                    }
                });
    }

    private synchronized Route cacheRoute(AsyncTcpConnection parent, Poll poll, PartitionRoute decoded) {
        ConnectionInfo endpoint = replicas.get(decoded.primaryReplica());
        if (decoded.partitionsCount() == 0) {
            throw new IggyMalformedResponseException("Partition route contains no partitions");
        }
        if (rosterSize == 0 || (endpoint == null && connectData != null)) {
            poll.terminalRoutingRefusal = true;
            throw notAccepted();
        }
        long generation = parent.sessionGeneration();
        long[] identity = parent.sessionIdentity();
        Attachment attachment = new Attachment(
                identity[0], identity[1], identity[2], decoded.metadataOp().longValue());
        byte[] secret = parent.bindSecret(identity[0], identity[1], identity[2]);
        Route route = new Route(
                endpoint,
                attachment,
                parent,
                generation,
                secret,
                decoded.context(),
                decoded.partitionsCount(),
                rosterSize <= 1 || connectData == null);
        if (routes.size() >= MAX_ROUTES) {
            clear();
        }
        if (coordinator.get() != parent || parent.sessionGeneration() != generation) {
            throw notAccepted();
        }
        routes.put(poll.key, route);
        partitionCounts.put(poll.topicKey(), decoded.partitionsCount());
        return route;
    }

    private synchronized boolean routeIsCurrent(Route route) {
        AsyncTcpConnection parent = coordinator.get();
        return route.parent == parent
                && route.generation == parent.sessionGeneration()
                && Long.compareUnsigned(route.attachment.watermark, parent.metadataWatermark()) >= 0;
    }

    private CompletableFuture<ByteBuf> enqueue(Route route, Poll poll) {
        if (poll.result.isDone()) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        poll.partitionsCount = route.partitionsCount;
        if (poll.dataCommand == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
            return CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
        }
        poll.routed();
        poll.context.compareAndSet(null, route.context);
        poll.state.setContext(poll.context.get());
        if (route.local) {
            poll.dataSent = true;
            return route.parent.sendRouted(
                    poll.dataCommand, Unpooled.wrappedBuffer(poll.payload), 0, poll.deadline, poll.state);
        }
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
            if (!attached.covers(data, route.attachment.withWatermark(route.parent.metadataWatermark()))) {
                return attach(route, poll).thenCompose(ignored -> sendPoll(route, poll));
            }
            // No poll monitor here: an idle slot sends under the router monitor, while expire()
            // completes the poll, whose callback takes the router, under the poll monitor.
            poll.context.compareAndSet(null, route.context);
            poll.dataSent = true;
            return data.sendRouted(
                            poll.dataCommand,
                            Unpooled.wrappedBuffer(poll.payload),
                            attached.generation,
                            poll.deadline,
                            poll.state)
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
                data.bindSharedSession(
                        route.parent,
                        route.attachment.clientLow,
                        route.attachment.clientHigh,
                        route.attachment.session,
                        route.bindSecret);
                connection = data;
                if (poll.result.isDone()) {
                    close();
                    return CompletableFuture.failedFuture(uncommitted());
                }
                ready = data.connect();
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
            Attachment required = route.attachment.withWatermark(route.parent.metadataWatermark());
            if (current != null && current.covers(data, required)) {
                return CompletableFuture.completedFuture(null);
            }
            return data.sendRouted(
                            CommandCode.System.BIND_SESSION.getValue(),
                            required.encode(route.bindSecret),
                            0,
                            poll.deadline,
                            new AsyncTcpConnection.TransientFailoverState())
                    .thenAccept(response -> {
                        response.release();
                        synchronized (this) {
                            if (connection == data && activePoll == poll && !poll.result.isDone()) {
                                attachment = new AttachedSession(data, data.sessionGeneration(), required);
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

    /** One poll or offset write, routed by the routing command and sent as the data command. */
    private static final class Poll {
        private final String key;
        private final byte[] routingBody;
        private final int dataCommand;
        private final byte[] payload;
        private final CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        private final long deadline;
        private volatile Slot activeSlot;
        // The caller's context, or else the first route's, kept for every retry of this poll.
        private final AtomicReference<PartitionContext> context;
        private boolean retryingRefusal;
        private boolean notPrimarySeen;
        private boolean refreshedHistory;
        private boolean terminalRoutingRefusal;
        private boolean dataSent;
        private long lifecycleRetryMillis = RETRY_INTERVAL_MILLIS;
        private IggyServerException lifecycleRefusal;
        private long partitionsCount;
        private AsyncTcpConnection.TransientFailoverState state = new AsyncTcpConnection.TransientFailoverState();

        private Poll(
                byte[] routingBody,
                int dataCommand,
                byte[] payload,
                Duration pollTimeout,
                Optional<PartitionContext> callerContext) {
            this.key = ByteBufUtil.hexDump(routingBody);
            this.routingBody = routingBody;
            this.dataCommand = dataCommand;
            this.payload = payload;
            this.deadline = System.nanoTime() + pollTimeout.toNanos();
            this.context = new AtomicReference<>(callerContext.orElse(null));
        }

        private boolean isLifecycleRefusal(IggyServerException refusal) {
            return dataSent && refusal != null && refusal.getRawErrorCode() == IggyErrorCode.LIFECYCLE_BUSY.getCode();
        }

        private boolean canRefreshHistory(IggyServerException refusal) {
            return refusal != null
                    && refusal.getRawErrorCode() == IggyErrorCode.HISTORY_UNAVAILABLE.getCode()
                    && dataCommand == CommandCode.Messages.SEND.getValue()
                    && !refreshedHistory;
        }

        private String topicKey() {
            return key.substring(0, key.length() - 2 * Integer.BYTES);
        }

        private void discardConnection() {
            Slot slot = activeSlot;
            if (slot != null) {
                slot.cancel(this);
            }
        }

        // A refusal leaves nothing in flight on a primary until the next route is known,
        // so the flag must survive the route lookup that follows it.
        private synchronized void routed() {
            retryingRefusal = false;
            lifecycleRefusal = null;
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
            result.completeExceptionally(
                    lifecycleRefusal != null ? lifecycleRefusal : retryingRefusal ? notAccepted() : uncommitted());
        }
    }

    private record AttachedSession(AsyncTcpConnection connection, long generation, Attachment attachment) {
        boolean covers(AsyncTcpConnection data, Attachment required) {
            return connection == data && generation == data.sessionGeneration() && attachment.covers(required);
        }
    }

    private record Route(
            ConnectionInfo endpoint,
            Attachment attachment,
            AsyncTcpConnection parent,
            long generation,
            byte[] bindSecret,
            PartitionContext context,
            long partitionsCount,
            boolean local) {}

    private record Attachment(long clientLow, long clientHigh, long session, long watermark) {
        private Attachment {
            if ((clientLow == 0 && clientHigh == 0) || session == 0) {
                throw new IggyClientException("Invalid primary poll session identity");
            }
        }

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

        ByteBuf encode(byte[] bindSecret) {
            return VsrRequestEncoder.bindSession(
                    UnpooledByteBufAllocator.DEFAULT, clientLow, clientHigh, session, watermark, bindSecret);
        }
    }
}
