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
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.client.async.tcp.vsr.VsrRequestEncoder;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.partition.PartitionRoute;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
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
    static final int MAX_CONCURRENT_DIALS = 4;
    private static final int MAX_ROUTES = 4096;
    private static final int MAX_CONNECTIONS = 256;
    private static final int MAX_PENDING_POLLS = 4096;
    private static final long RETRY_INTERVAL_MILLIS = 50;
    private static final long MAX_RETRY_INTERVAL_MILLIS = 1000;
    private static final long MAX_REDIAL_MILLIS = 2000;

    private final Supplier<AsyncTcpConnection> coordinator;
    private final Function<ConnectionInfo, AsyncTcpConnection> connectData;
    private final Duration pollTimeout;
    private final Map<TargetKey, Route> routes = new LinkedHashMap<>(16, 0.75f, true);
    private Supplier<ConnectionInfo> coordinatorEndpoint;
    private ConnectionInfo configuredEndpoint;
    private int activeDials;
    private long topologyGeneration;
    private final Map<ConnectionInfo, Slot> connections = new HashMap<>();
    private final Set<Poll> pending = new HashSet<>();
    private volatile int rosterSize;
    private final NavigableMap<Integer, ConnectionInfo> replicas = new TreeMap<>();
    // Read without the monitor: a slot opens its link under the slot monitor, and the client reads this there to size
    // the dial timeout.
    private volatile ConnectionInfo metadataPrimary;
    private final Map<TargetKey, Long> partitionCounts = new HashMap<>();

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

    void coordinatorEndpoint(Supplier<ConnectionInfo> endpoint) {
        coordinatorEndpoint = endpoint;
    }

    void configuredEndpoint(ConnectionInfo endpoint) {
        configuredEndpoint = endpoint;
    }

    synchronized boolean ownsConnection(AsyncTcpConnection connection) {
        return connections.values().stream().anyMatch(slot -> slot.connection == connection);
    }

    CompletableFuture<AsyncTcpConnection> connectionFor(ConnectionInfo endpoint, long deadline) {
        Route route = attachmentRoute(endpoint);
        Slot slot = slotFor(endpoint);
        // A close of the slot during its bind clears the link, and the bind can still end without an error.
        return slot.prepare(route, deadline).thenApply(ignored -> {
            AsyncTcpConnection prepared = slot.connection;
            if (prepared == null) {
                throw notAccepted();
            }
            return prepared;
        });
    }

    private Route attachmentRoute(ConnectionInfo endpoint) {
        AsyncTcpConnection parent = coordinator.get();
        long[] identity = parent.sessionIdentity();
        return new Route(
                endpoint,
                new Attachment(identity[0], identity[1], identity[2], parent.metadataWatermark()),
                parent,
                parent.sessionOwner().generation(),
                parent.bindSecret(),
                PartitionContext.EMPTY,
                0,
                false,
                0,
                false,
                null);
    }

    private void maintain(ConnectionInfo endpoint, Slot slot, long generation, long delay) {
        synchronized (this) {
            if (generation != topologyGeneration || connections.get(endpoint) != slot) {
                return;
            }
            slot.maintenance = coordinator
                    .get()
                    .eventLoop()
                    .schedule(
                            () -> {
                                synchronized (PollRouter.this) {
                                    if (generation != topologyGeneration || connections.get(endpoint) != slot) {
                                        return;
                                    }
                                }
                                CompletableFuture<Void> ready;
                                try {
                                    ready = slot.prepare(
                                            attachmentRoute(endpoint),
                                            System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_REDIAL_MILLIS));
                                } catch (RuntimeException error) {
                                    ready = CompletableFuture.failedFuture(error);
                                }
                                ready.whenComplete((ignored, error) -> {
                                    long next = error == null
                                            ? MAX_REDIAL_MILLIS
                                            : Math.min(MAX_REDIAL_MILLIS, Math.max(RETRY_INTERVAL_MILLIS, delay * 2));
                                    maintain(
                                            endpoint,
                                            slot,
                                            generation,
                                            Math.min(
                                                    MAX_REDIAL_MILLIS,
                                                    next
                                                            + ThreadLocalRandom.current()
                                                                    .nextLong(RETRY_INTERVAL_MILLIS)));
                                });
                            },
                            delay,
                            TimeUnit.MILLISECONDS);
        }
    }

    private CompletableFuture<Void> beforeDeadline(CompletableFuture<Void> source, long deadline) {
        if (source.isDone()) {
            return source;
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        var timer = coordinator
                .get()
                .eventLoop()
                .schedule(
                        () -> result.completeExceptionally(notAccepted()),
                        Math.max(0, deadline - System.nanoTime()),
                        TimeUnit.NANOSECONDS);
        source.whenComplete((ignored, error) -> {
            if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(error);
            }
        });
        result.whenComplete((ignored, error) -> timer.cancel(false));
        return result;
    }

    private CompletableFuture<Void> dial(AsyncTcpConnection data, long deadline) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        dialWhenAvailable(data, deadline, result);
        return result;
    }

    private void dialWhenAvailable(AsyncTcpConnection data, long deadline, CompletableFuture<Void> result) {
        if (result.isDone()) {
            return;
        }
        // Outside the monitor: the waiters of the dial fail inline, and they take the slot, poll and client monitors.
        if (deadline - System.nanoTime() <= 0) {
            result.completeExceptionally(notAccepted());
            return;
        }
        synchronized (this) {
            if (activeDials >= MAX_CONCURRENT_DIALS) {
                coordinator
                        .get()
                        .eventLoop()
                        .schedule(
                                () -> dialWhenAvailable(data, deadline, result),
                                RETRY_INTERVAL_MILLIS,
                                TimeUnit.MILLISECONDS);
                return;
            }
            activeDials++;
        }
        data.connect().whenComplete((ignored, error) -> {
            synchronized (this) {
                activeDials--;
            }
            if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(error);
            }
        });
    }

    synchronized void clearBindings() {
        connections.values().forEach(slot -> slot.attachment = null);
    }

    CompletableFuture<ByteBuf> poll(ByteBuf payload, Optional<PartitionContext> context) {
        return partitionRequest(CommandCode.Messages.POLL.getValue(), payload, context);
    }

    long operationDeadline() {
        return System.nanoTime() + pollTimeout.toNanos();
    }

    CompletableFuture<ByteBuf> partitionRequest(int command, ByteBuf payload, Optional<PartitionContext> context) {
        return partitionRequest(command, payload, context, operationDeadline());
    }

    CompletableFuture<ByteBuf> partitionRequest(
            int command, ByteBuf payload, Optional<PartitionContext> context, long deadline) {
        return partitionRequest(command, payload, context, deadline, startCall());
    }

    CompletableFuture<ByteBuf> partitionRequest(
            int command,
            ByteBuf payload,
            Optional<PartitionContext> context,
            long deadline,
            AsyncTcpConnection.TransientFailoverState state) {
        try {
            byte[] query = partitionQuery(command, payload);
            Poll poll = new Poll(query, command, payload, deadline, context);
            poll.state = state;
            return send(poll);
        } catch (RuntimeException error) {
            payload.release();
            return CompletableFuture.failedFuture(error);
        }
    }

    CompletableFuture<ByteBuf> sendMessages(ByteBuf payload, ByteBuf query) {
        return sendMessages(payload, query, operationDeadline());
    }

    CompletableFuture<ByteBuf> sendMessages(ByteBuf payload, ByteBuf query, long deadline) {
        return sendMessages(payload, query, deadline, startCall());
    }

    CompletableFuture<ByteBuf> sendMessages(
            ByteBuf payload, ByteBuf query, long deadline, AsyncTcpConnection.TransientFailoverState state) {
        byte[] target = takeBytes(query);
        Poll poll = new Poll(target, CommandCode.Messages.SEND.getValue(), payload, deadline, Optional.empty());
        poll.state = state;
        return send(poll);
    }

    CompletableFuture<Long> partitionCount(
            StreamId stream, TopicId topic, long deadline, AsyncTcpConnection.TransientFailoverState state) {
        if (state.signedOut()) {
            return CompletableFuture.failedFuture(state.lineageEnded(null));
        }
        ByteBuf query = BytesSerializer.toBytes(stream);
        ByteBuf topicBytes = BytesSerializer.toBytes(topic);
        try {
            query.writeBytes(topicBytes);
            query.writeIntLE(0);
        } finally {
            topicBytes.release();
        }
        byte[] target = takeBytes(query);
        TargetKey topicKey = TargetKey.of(target).topic();
        synchronized (this) {
            Long count = partitionCounts.get(topicKey);
            if (count != null) {
                return CompletableFuture.completedFuture(count);
            }
        }
        Poll lookup = new Poll(
                target,
                CommandCode.Messages.GET_PARTITION_ROUTE.getValue(),
                Unpooled.wrappedBuffer(target),
                deadline,
                Optional.empty());
        lookup.state = state;
        return AsyncTcpConnection.mapResponse(send(lookup), response -> {
            response.release();
            return lookup.partitionsCount;
        });
    }

    synchronized void invalidateTopic(StreamId stream, TopicId topic) {
        ByteBuf prefix = BytesSerializer.toBytes(stream);
        ByteBuf topicBytes = BytesSerializer.toBytes(topic);
        try {
            prefix.writeBytes(topicBytes);
            TargetKey key = TargetKey.of(ByteBufUtil.getBytes(prefix));
            partitionCounts.remove(key);
            routes.keySet().removeIf(route -> route.startsWith(key));
        } finally {
            topicBytes.release();
            prefix.release();
        }
    }

    /**
     * Replays the retained operation once a running replay of it ends. An operation that is no longer retained
     * completes with the outcome that a replay proved for it, without a request.
     */
    CompletableFuture<ByteBuf> resolve(long operationId, Duration timeout) {
        AsyncTcpConnection current = coordinator.get();
        ConsensusSession owner = current.sessionOwner();
        long deadline = System.nanoTime() + timeout.toNanos();
        var resumed = owner.resume(operationId, deadline, current.eventLoop());
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                resumed.cancel(false);
            }
        });
        resumed.whenComplete((ticket, error) -> {
            if (ticket != null) {
                replay(current, ticket, deadline, result);
            } else if (error instanceof IggyOperationException notRetained
                    && notRetained.getErrorCode() == IggyErrorCode.OPERATION_NOT_RETAINED) {
                try {
                    AsyncTcpConnection.completeWithResponse(
                            result, Unpooled.wrappedBuffer(owner.provedOutcome(operationId)), null);
                } catch (RuntimeException proved) {
                    result.completeExceptionally(proved);
                }
            } else {
                result.completeExceptionally(error);
            }
        });
        return result;
    }

    private void replay(
            AsyncTcpConnection current,
            ConsensusSession.Ticket ticket,
            long deadline,
            CompletableFuture<ByteBuf> result) {
        CompletableFuture<ByteBuf> replayed;
        try {
            PartitionContext context = ticket.context();
            var state = new AsyncTcpConnection.TransientFailoverState(context);
            state.restore(ticket);
            ByteBuf frame = ticket.frame();
            ByteBuf payload = frame.slice(
                    frame.readerIndex() + VsrHeaders.HEADER_SIZE, frame.readableBytes() - VsrHeaders.HEADER_SIZE);
            if (ticket.routingBody() == null) {
                replayed = current.send(ticket.commandCode(), payload, deadline, state);
                replayed.whenComplete((response, error) -> {
                    ticket.finishWrite();
                    if (error != null) {
                        state.retainTicket(ticket);
                    }
                });
            } else {
                Poll poll =
                        new Poll(ticket.routingBody(), ticket.commandCode(), payload, deadline, Optional.of(context));
                poll.state = state;
                poll.payloadLease = ticket;
                replayed = send(poll);
            }
        } catch (RuntimeException failure) {
            // Gives the claim back, or ends the operation when an abandon or a reset released it before the replay.
            ticket.retain();
            result.completeExceptionally(failure);
            return;
        }
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                replayed.cancel(false);
            }
        });
        replayed.whenComplete((response, error) -> AsyncTcpConnection.completeWithResponse(result, response, error));
    }

    synchronized void clear() {
        routes.clear();
        partitionCounts.clear();
    }

    void observeTopology(ClusterMetadata metadata) {
        // Outside the monitor, as in clearSession: a close takes the slot monitor, which a slot holds while it opens
        // its link, and fails the waiters of the dial inline.
        applyTopology(metadata).forEach(Slot::close);
    }

    /** Returns the slots whose endpoints left the roster. */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private synchronized List<Slot> applyTopology(ClusterMetadata metadata) {
        replicas.clear();
        ConnectionInfo leader = null;
        rosterSize = metadata.nodes().size();
        for (var node : metadata.nodes()) {
            if (node.endpoints().tcp() != 0) {
                ConnectionInfo endpoint = LeaderAwareness.nodeTarget(metadata, node, configuredEndpoint);
                replicas.put(node.replicaId(), endpoint);
                if (node.role() == ClusterNodeRole.Leader) {
                    leader = endpoint;
                }
            }
        }
        metadataPrimary = leader;
        if (connectData == null || !coordinator.get().isAuthenticated()) {
            return List.of();
        }
        List<Slot> obsolete = new ArrayList<>();
        var entries = connections.entrySet().iterator();
        while (entries.hasNext()) {
            var entry = entries.next();
            if (!replicas.containsValue(entry.getKey())) {
                entries.remove();
                obsolete.add(entry.getValue());
            }
        }
        for (ConnectionInfo endpoint : replicas.values()) {
            Slot slot = connections.get(endpoint);
            if (slot == null && connections.size() < MAX_CONNECTIONS) {
                slot = new Slot();
                if (coordinatorEndpoint != null
                        && LeaderAwareness.isSameSpelling(endpoint, coordinatorEndpoint.get())) {
                    slot.connection = coordinator.get();
                    slot.connecting = CompletableFuture.completedFuture(null);
                    Route route = attachmentRoute(endpoint);
                    slot.attachment =
                            new AttachedSession(slot.connection, slot.connection.sessionGeneration(), route.attachment);
                }
                connections.put(endpoint, slot);
                maintain(endpoint, slot, topologyGeneration, 0);
            }
        }
        return obsolete;
    }

    /** The node that the last roster named as metadata primary, or null if it named none. */
    ConnectionInfo metadataPrimary() {
        return metadataPrimary;
    }

    private static byte[] partitionQuery(int command, ByteBuf request) {
        ByteBuf body = request.duplicate();
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

    ConsensusSession sessionOwner() {
        return coordinator.get().sessionOwner();
    }

    AsyncTcpConnection.TransientFailoverState startCall() {
        var state = new AsyncTcpConnection.TransientFailoverState();
        state.startIn(sessionOwner());
        return state;
    }

    /**
     * Passes the outcome of {@code source} to the returned future in a task of the client event loop. The router
     * completes {@code source} under its monitors, and a caller callback there that waits for the client monitor can
     * close a cycle with a sign-in that holds the client monitor and ends the pending polls. The loop is read once
     * here, so a coordinator that is gone when {@code source} completes cannot strand the caller. The loop runs these
     * tasks in order. A cancel of the returned future cancels {@code source}, and it wins over an outcome that has not
     * reached the loop yet. A loop that rejects the task completes the returned future in place, under the monitors of
     * the completing thread. Netty can also drop a task that it accepts during its final shutdown, and then the
     * returned future never completes.
     */
    <T> CompletableFuture<T> onClientLoop(CompletableFuture<T> source) {
        var loop = coordinator.get().eventLoop();
        CompletableFuture<T> caller = new CompletableFuture<>();
        caller.whenComplete((value, error) -> {
            if (caller.isCancelled()) {
                source.cancel(false);
            }
        });
        source.whenComplete((value, error) -> {
            Runnable relay = () -> {
                if (error == null) {
                    caller.complete(value);
                } else {
                    caller.completeExceptionally(error);
                }
            };
            try {
                loop.execute(relay);
            } catch (RejectedExecutionException loopGone) {
                relay.run();
            }
        });
        return caller;
    }

    private CompletableFuture<ByteBuf> send(Poll poll) {
        poll.result.whenComplete((response, error) -> {
            poll.finish(error);
            synchronized (this) {
                pending.remove(poll);
            }
        });
        synchronized (this) {
            if (pending.size() >= MAX_PENDING_POLLS) {
                poll.result.completeExceptionally(
                        new IggyClientException("Too many pending primary polls and offset writes"));
                return poll.result;
            }
            pending.add(poll);
        }
        try {
            poll.state.startIn(coordinator.get().sessionOwner());
            var timeout = coordinator
                    .get()
                    .eventLoop()
                    .schedule(poll::endPending, Math.max(0, poll.deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            poll.result.whenComplete((response, error) -> timeout.cancel(false));
            attempt(poll);
        } catch (RuntimeException error) {
            poll.fail(error);
        }
        return poll.result;
    }

    CompletableFuture<Void> clearSession() {
        return clearSession(null);
    }

    CompletableFuture<Void> clearSession(AsyncTcpConnection keep) {
        Set<Poll> ending;
        List<Slot> closing;
        synchronized (this) {
            topologyGeneration++;
            clear();
            ending = Set.copyOf(pending);
            closing = List.copyOf(connections.values());
            connections.clear();
        }
        // A poll timeout holds its own monitor while completion callbacks acquire the router's monitor.
        ending.forEach(Poll::endPending);
        return CompletableFuture.allOf(
                closing.stream().map(slot -> slot.close(keep)).toArray(CompletableFuture[]::new));
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void attempt(Poll poll) {
        if (poll.result.isDone()) {
            return;
        }
        if (poll.state.signedOut()) {
            poll.fail(poll.state.lineageEnded(null));
            return;
        }
        poll.dataSent = false;
        if (poll.state.ticket() == null && poll.requiresLane) {
            try {
                synchronized (this) {
                    Route known = routes.get(poll.key);
                    if (known != null) {
                        coordinator.get().sessionOwner().checkRetained(poll.laneFor(known.context));
                    }
                }
            } catch (RuntimeException error) {
                poll.fail(error);
                return;
            }
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
            failedAttempt(poll, error);
        });
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void failedAttempt(Poll poll, Throwable error) {
        if (poll.retryingRefusal && unwrap(error) instanceof IggyTimeoutException) {
            poll.fail(notAccepted());
            return;
        }
        IggyServerException refusal = AsyncTcpConnection.findServerError(error);
        boolean notPrimary = refusal != null && refusal.getRawErrorCode() == AsyncTcpConnection.NOT_PRIMARY;
        boolean lostLink = connectionFailed(error);
        invalidateRoute(poll, error, notPrimary);
        if ((!poll.requiresLane || poll.isMutation())
                && (lostLink
                        || (refusal != null
                                && refusal.getRawErrorCode() == AsyncTcpConnection.TRANSIENT_NOT_COMMITTED))) {
            if (poll.deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(RETRY_INTERVAL_MILLIS)) {
                // A mutation that an attempt can have applied keeps that in its ticket, which fail() retains.
                poll.fail(lostLink && (poll.isMutation() || !poll.dataSent) ? notAccepted() : uncommitted());
            } else {
                coordinator
                        .get()
                        .eventLoop()
                        .schedule(() -> attempt(poll), RETRY_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            }
            return;
        }
        if (poll.restartHistory(refusal)) {
            attempt(poll);
            return;
        }
        if (poll.isLifecycleRefusal(refusal)) {
            retryLifecycle(poll, refusal);
            return;
        }
        if (!poll.terminalRoutingRefusal && (isNotAccepted(error) || notPrimary)) {
            retry(poll, notPrimary);
        } else {
            poll.fail(error);
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private synchronized void invalidateRoute(Poll poll, Throwable error, boolean notPrimary) {
        IggyServerException server = AsyncTcpConnection.findServerError(error);
        boolean lostLink = connectionFailed(error);
        if (server != null && server.getRawErrorCode() == IggyErrorCode.HISTORY_UNAVAILABLE.getCode()) {
            TargetKey topic = poll.key.topic();
            routes.keySet().removeIf(route -> route.startsWith(topic));
            partitionCounts.remove(topic);
        } else if (isNotAccepted(error) || notPrimary || lostLink) {
            Route previous = routes.get(poll.key);
            if (previous != null) {
                routes.put(
                        poll.key,
                        new Route(
                                previous.endpoint,
                                previous.attachment,
                                previous.parent,
                                previous.generation,
                                previous.bindSecret,
                                previous.context,
                                previous.partitionsCount,
                                previous.local,
                                notPrimary && poll.dataSent
                                        ? Math.max(previous.view, Math.min(0xffff_ffffL, poll.routeView + 1))
                                        : previous.view,
                                true,
                                poll.backupMiss
                                        ? metadataPrimary
                                        : notPrimary && poll.dataSent
                                                ? previous.endpoint
                                                : lostLink ? nextReplica(previous.endpoint) : previous.hint));
            }
        }
    }

    /**
     * The replica after {@code failed} in id order, or null if the roster does not name {@code failed}. A view change
     * passes the primary to the next replica, so it is the likely primary after {@code failed}.
     */
    private synchronized ConnectionInfo nextReplica(ConnectionInfo failed) {
        for (var replica : replicas.entrySet()) {
            if (replica.getValue().equals(failed)) {
                var next = replicas.higherEntry(replica.getKey());
                return (next == null ? replicas.firstEntry() : next).getValue();
            }
        }
        return null;
    }

    private void retryLifecycle(Poll poll, IggyServerException refusal) {
        long delay = poll.lifecycleRetryMillis;
        if (poll.deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(delay)) {
            poll.fail(refusal);
            return;
        }
        poll.lifecycleRefusal = refusal;
        poll.state.renewRequestId();
        poll.lifecycleRetryMillis = Math.min(delay * 2, AsyncTcpConnection.LIFECYCLE_RETRY_MAX_INTERVAL_MS);
        coordinator.get().eventLoop().schedule(() -> attempt(poll), delay, TimeUnit.MILLISECONDS);
    }

    private void retry(Poll poll, boolean notPrimary) {
        long delay = notPrimary ? poll.notPrimarySeen ? poll.notPrimaryRetryMillis : 0 : RETRY_INTERVAL_MILLIS;
        if (!poll.retryRefusal(delay)) {
            return;
        }
        if (notPrimary && poll.notPrimarySeen) {
            poll.notPrimaryRetryMillis = Math.min(delay * 2, MAX_RETRY_INTERVAL_MILLIS);
        }
        poll.notPrimarySeen |= notPrimary;
        coordinator.get().eventLoop().schedule(() -> attempt(poll), delay, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Route> route(Poll poll) {
        AsyncTcpConnection parent = coordinator.get();
        if (!parent.isAuthenticated()) {
            return parent.awaitAuthentication(poll.deadline, poll.state)
                    .thenComposeAsync(ignored -> route(poll), parent.eventLoop());
        }
        Route cached;
        synchronized (this) {
            cached = routes.get(poll.key);
            if (cached != null && routeIsCurrent(cached)) {
                return CompletableFuture.completedFuture(cached);
            }
        }
        CompletableFuture<Void> roster = rosterSize == 0
                ? refreshRoster(parent, poll.deadline).exceptionallyCompose(error -> {
                    poll.terminalRoutingRefusal = true;
                    return CompletableFuture.failedFuture(error);
                })
                : CompletableFuture.completedFuture(null);
        return roster.thenCompose(ignored -> lookupConnection(poll, cached))
                .thenCompose(source -> confirmNamespaceMiss(
                        source,
                        poll,
                        source.sendRouted(
                                CommandCode.Messages.GET_PARTITION_ROUTE.getValue(),
                                Unpooled.wrappedBuffer(poll.routingBody),
                                0,
                                poll.deadline,
                                new AsyncTcpConnection.TransientFailoverState())))
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

    /**
     * Turns a namespace miss that {@code hop} answered into a non-admission unless {@code hop} is the metadata
     * primary, so the attempt routes again from the metadata primary and the deadline ends the call with code 58. A
     * backup can lack metadata that the metadata primary already committed, and it refused the request before
     * admission.
     */
    private CompletableFuture<ByteBuf> confirmNamespaceMiss(
            AsyncTcpConnection hop, Poll poll, CompletableFuture<ByteBuf> exchange) {
        return exchange.exceptionallyCompose(error -> {
            if (!isNamespaceMiss(error) || poll.state.committed()) {
                return CompletableFuture.failedFuture(error);
            }
            // The miss refused the request before admission, so a deadline during the roster read ends the call as
            // not accepted.
            poll.dataSent = false;
            return isMetadataPrimary(hop, poll.deadline).thenCompose(primary -> {
                if (primary) {
                    return CompletableFuture.failedFuture(error);
                }
                poll.backupMiss = true;
                return CompletableFuture.failedFuture(notAccepted());
            });
        });
    }

    /** Whether {@code error} tells that the replica has no such stream, topic or partition. */
    static boolean isNamespaceMiss(Throwable error) {
        IggyServerException refusal = AsyncTcpConnection.findServerError(error);
        return refusal != null
                && switch (refusal.getErrorCode()) {
                    case STREAM_ID_NOT_FOUND,
                            STREAM_NAME_NOT_FOUND,
                            TOPIC_ID_NOT_FOUND,
                            TOPIC_NAME_NOT_FOUND,
                            PARTITION_NOT_FOUND -> true;
                    default -> false;
                };
    }

    /**
     * Whether {@code hop} reaches the metadata primary, whose namespace miss is final. The belief can predate a view
     * change, so a roster read on {@code hop} must still name its node. No lookup reaches a believed metadata primary
     * without a link, so then the roster on {@code hop} is read to update the belief.
     */
    private CompletableFuture<Boolean> isMetadataPrimary(AsyncTcpConnection hop, long deadline) {
        if (connectData == null) {
            return CompletableFuture.completedFuture(true);
        }
        ConnectionInfo believed = metadataPrimary();
        CompletableFuture<Boolean> matches =
                believed != null && hasLink(believed) ? isNode(hop, believed) : CompletableFuture.completedFuture(true);
        return matches.thenCompose(sameNode -> {
            if (!sameNode) {
                return CompletableFuture.completedFuture(false);
            }
            return refreshRoster(hop, deadline)
                    .handle((ignored, error) -> error == null ? metadataPrimary() : null)
                    .thenCompose(
                            named -> named == null ? CompletableFuture.completedFuture(false) : isNode(hop, named));
        });
    }

    /** Whether the slot of {@code endpoint} holds a connection that is bound and has an open channel. */
    private synchronized boolean hasLink(ConnectionInfo endpoint) {
        Slot slot = connections.get(endpoint);
        AsyncTcpConnection link = slot == null ? null : slot.connection;
        return link != null && slot.attachment != null && link.hasOpenChannel();
    }

    /**
     * Whether {@code hop} is a connection to the roster node at {@code endpoint}. The client can name the node of its
     * own connection with another spelling than the roster, such as a hostname for an address.
     */
    private CompletableFuture<Boolean> isNode(AsyncTcpConnection hop, ConnectionInfo endpoint) {
        Slot slot;
        synchronized (this) {
            slot = connections.get(endpoint);
        }
        if (slot != null && slot.connection == hop) {
            return CompletableFuture.completedFuture(true);
        }
        if (hop != coordinator.get() || coordinatorEndpoint == null) {
            return CompletableFuture.completedFuture(false);
        }
        ConnectionInfo configured = coordinatorEndpoint.get();
        if (configured.port() != endpoint.port()) {
            return CompletableFuture.completedFuture(false);
        }
        if (LeaderAwareness.isSameSpelling(configured, endpoint)) {
            return CompletableFuture.completedFuture(true);
        }
        // Address comparison can resolve a hostname. A slow resolver must not block socket reads or deadlines.
        return CompletableFuture.supplyAsync(() -> LeaderAwareness.isSameAddress(configured, endpoint));
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<AsyncTcpConnection> lookupConnection(Poll poll, Route stale) {
        AsyncTcpConnection current = coordinator.get();
        if (connectData == null || coordinatorEndpoint == null) {
            return CompletableFuture.completedFuture(current);
        }
        ConnectionInfo target = null;
        synchronized (this) {
            if (stale != null && stale.hint != null && !poll.lookupVisited.contains(stale.hint)) {
                target = stale.hint;
            } else if (poll.backupMiss && metadataPrimary != null && !poll.lookupVisited.contains(metadataPrimary)) {
                target = metadataPrimary;
            } else if (!poll.lookupVisited.contains(coordinatorEndpoint.get())) {
                target = coordinatorEndpoint.get();
            } else {
                for (ConnectionInfo candidate : replicas.values()) {
                    if (!poll.lookupVisited.contains(candidate)) {
                        target = candidate;
                        break;
                    }
                }
            }
            if (target == null) {
                poll.lookupVisited.clear();
                return CompletableFuture.failedFuture(notAccepted());
            }
            poll.lookupVisited.add(target);
        }
        if (LeaderAwareness.isSameSpelling(target, coordinatorEndpoint.get())) {
            return CompletableFuture.completedFuture(current);
        }
        return connectionFor(target, poll.deadline).exceptionallyCompose(error -> {
            if (poll.result.isDone() || poll.deadline - System.nanoTime() <= 0) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            return lookupConnection(poll, stale);
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

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private synchronized Route cacheRoute(AsyncTcpConnection parent, Poll poll, PartitionRoute decoded) {
        ConnectionInfo endpoint = replicas.get(decoded.primaryReplica());
        if (decoded.partitionsCount() == 0) {
            throw new IggyMalformedResponseException("Partition route contains no partitions");
        }
        if (rosterSize == 0 || (endpoint == null && connectData != null)) {
            poll.terminalRoutingRefusal = true;
            throw notAccepted();
        }
        long generation = parent.sessionOwner().generation();
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
                rosterSize <= 1 || connectData == null,
                decoded.view(),
                false,
                null);
        Route previous = routes.get(poll.key);
        if (previous != null
                && previous.context.incarnation().equals(decoded.incarnation())
                && (decoded.view() < previous.view
                        || decoded.metadataOp().compareTo(previous.context.metadataOp()) < 0)) {
            throw notAccepted();
        }
        if (routes.size() >= MAX_ROUTES && !routes.containsKey(poll.key)) {
            var oldest = routes.keySet().iterator();
            TargetKey evicted = oldest.next();
            oldest.remove();
            partitionCounts.remove(evicted.topic());
        }
        if (parent.sessionOwner() != coordinator.get().sessionOwner()
                || parent.sessionOwner().generation() != generation) {
            throw notAccepted();
        }
        poll.lookupVisited.clear();
        routes.put(poll.key, route);
        partitionCounts.put(poll.key.topic(), decoded.partitionsCount());
        return route;
    }

    private synchronized boolean routeIsCurrent(Route route) {
        AsyncTcpConnection parent = coordinator.get();
        return !route.stale
                && route.parent.sessionOwner() == parent.sessionOwner()
                && route.generation == parent.sessionOwner().generation()
                && Long.compareUnsigned(route.attachment.watermark, parent.metadataWatermark()) >= 0;
    }

    private CompletableFuture<ByteBuf> enqueue(Route route, Poll poll) {
        if (poll.result.isDone()) {
            return CompletableFuture.failedFuture(notAccepted());
        }
        poll.partitionsCount = route.partitionsCount;
        poll.routeView = route.view;
        poll.backupMiss = false;
        if (poll.dataCommand == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
            return CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
        }
        poll.routed();
        poll.context.compareAndSet(null, route.context);
        poll.state.setContext(poll.context.get());
        poll.state.setRoutingBody(poll.routingBody);
        if (poll.requiresLane) {
            poll.state.setLane(poll.laneFor(poll.context.get()));
        }
        if (route.local) {
            poll.dataSent = true;
            CompletableFuture<ByteBuf> exchange = route.parent.sendRouted(
                    poll.dataCommand, poll.payload.retainedDuplicate(), 0, poll.deadline, poll.state);
            poll.activeExchange = exchange;
            if (poll.result.isDone()) {
                exchange.cancel(false);
            }
            return confirmNamespaceMiss(route.parent, poll, exchange);
        }
        return slotFor(route.endpoint).poll(route, poll);
    }

    private synchronized Slot slotFor(ConnectionInfo endpoint) {
        Slot slot = connections.get(endpoint);
        if (slot == null) {
            if (connections.size() >= MAX_CONNECTIONS) {
                throw notAccepted();
            }
            slot = new Slot();
            connections.put(endpoint, slot);
        }
        return slot;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? unwrap(error.getCause()) : error;
    }

    private static boolean isNotAccepted(Throwable error) {
        return unwrap(error) instanceof IggyServerException server
                && server.getRawErrorCode() == AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED;
    }

    private static boolean connectionFailed(Throwable error) {
        if (error instanceof AsyncTcpConnection.LinkFailure) {
            return true;
        }
        if (error instanceof CompletionException && error.getCause() != null) {
            return connectionFailed(error.getCause());
        }
        return error instanceof IggyConnectionException
                || error instanceof IggyNotConnectedException
                || error instanceof IggyTimeoutException
                || error instanceof IOException;
    }

    private static IggyServerException notAccepted() {
        return IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0]);
    }

    private static IggyServerException uncommitted() {
        return IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_COMMITTED, new byte[0]);
    }

    private final class Slot {
        private volatile AsyncTcpConnection connection;
        private volatile AttachedSession attachment;
        private CompletableFuture<Void> connecting;
        private CompletableFuture<Void> binding;
        private io.netty.util.concurrent.ScheduledFuture<?> maintenance;

        CompletableFuture<ByteBuf> poll(Route route, Poll poll) {
            return prepare(route, poll.deadline).thenCompose(ignored -> sendPoll(route, poll));
        }

        private CompletableFuture<ByteBuf> sendPoll(Route route, Poll poll) {
            if (poll.result.isDone() || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            AsyncTcpConnection data;
            AttachedSession attached;
            synchronized (this) {
                data = connection;
                attached = attachment;
            }
            if (data == null || attached == null) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            if (!attached.covers(data, route.attachment.withWatermark(route.parent.metadataWatermark()))) {
                return attach(route, poll.deadline).thenCompose(ignored -> sendPoll(route, poll));
            }
            poll.dataSent = true;
            CompletableFuture<ByteBuf> exchange = data.sendRouted(
                    poll.dataCommand, poll.payload.retainedDuplicate(), attached.generation, poll.deadline, poll.state);
            poll.activeExchange = exchange;
            if (poll.result.isDone()) {
                exchange.cancel(false);
            }
            return confirmNamespaceMiss(data, poll, exchange);
        }

        private CompletableFuture<Void> prepare(Route route, long deadline) {
            CompletableFuture<Void> ready;
            AsyncTcpConnection created = null;
            synchronized (this) {
                if (connection == null) {
                    created = connectData.apply(route.endpoint);
                    created.bindSharedSession(
                            route.parent,
                            route.attachment.clientLow,
                            route.attachment.clientHigh,
                            route.attachment.session,
                            route.bindSecret);
                    connection = created;
                    connecting = new CompletableFuture<>();
                }
                ready = connecting;
            }
            if (created != null) {
                AsyncTcpConnection data = created;
                dial(data, deadline).whenComplete((ignored, error) -> {
                    if (error != null) {
                        synchronized (this) {
                            if (connection == data) {
                                connection = null;
                                connecting = null;
                                attachment = null;
                            }
                        }
                        data.close();
                        ready.completeExceptionally(error);
                    } else if (!ready.complete(null)) {
                        data.close();
                    }
                });
            }
            return beforeDeadline(ready, deadline).thenCompose(ignored -> attach(route, deadline));
        }

        @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
        private CompletableFuture<Void> attach(Route route, long deadline) {
            AsyncTcpConnection data;
            Attachment required = route.attachment.withWatermark(route.parent.metadataWatermark());
            CompletableFuture<Void> gate;
            boolean start;
            if (deadline - System.nanoTime() <= 0 || !routeIsCurrent(route)) {
                return CompletableFuture.failedFuture(notAccepted());
            }
            synchronized (this) {
                data = connection;
                if (data == null) {
                    return CompletableFuture.failedFuture(notAccepted());
                }
                if (attachment != null && attachment.covers(data, required)) {
                    return CompletableFuture.completedFuture(null);
                }
                start = binding == null;
                if (start) {
                    binding = new CompletableFuture<>();
                }
                gate = binding;
            }
            if (!start) {
                return beforeDeadline(gate, deadline)
                        .handle((ignored, error) -> null)
                        .thenCompose(ignored -> attach(route, deadline));
            }
            data.sendRouted(
                            CommandCode.System.BIND_SESSION.getValue(),
                            required.encode(route.bindSecret),
                            0,
                            deadline,
                            new AsyncTcpConnection.TransientFailoverState())
                    .whenComplete((response, error) -> {
                        if (response != null) {
                            response.release();
                        }
                        synchronized (this) {
                            if (connection == data && error == null) {
                                attachment = new AttachedSession(data, data.sessionGeneration(), required);
                            }
                            if (binding == gate) {
                                binding = null;
                            }
                        }
                        if (error == null) {
                            gate.complete(null);
                        } else {
                            // The bind decides: an unauthenticated reply ended the session that the attach named,
                            // and a failed link says nothing about the session.
                            IggyServerException refusal =
                                    connectionFailed(error) ? null : AsyncTcpConnection.findServerError(error);
                            if (refusal != null
                                    && refusal.getRawErrorCode() == IggyErrorCode.UNAUTHENTICATED.getCode()) {
                                coordinator
                                        .get()
                                        .resetRefusedSession(
                                                required.clientLow, required.clientHigh, refusal.getRawErrorCode());
                            }
                            gate.completeExceptionally(error);
                        }
                    });
            return gate;
        }

        CompletableFuture<Void> close() {
            return close(null);
        }

        CompletableFuture<Void> close(AsyncTcpConnection keep) {
            AsyncTcpConnection previous;
            CompletableFuture<Void> dialing;
            synchronized (this) {
                if (maintenance != null) {
                    maintenance.cancel(false);
                    maintenance = null;
                }
                previous = connection;
                connection = null;
                attachment = null;
                dialing = connecting;
                connecting = null;
                binding = null;
            }
            // Outside the monitor: the waiters of the dial fail inline, and they take the router, poll and client
            // monitors.
            if (dialing != null) {
                dialing.cancel(false);
            }
            return previous == null || previous == keep ? CompletableFuture.completedFuture(null) : previous.close();
        }
    }

    /** One poll or offset write, routed by the routing command and sent as the data command. */
    private static final class Poll {
        private final TargetKey key;
        private final byte[] routingBody;
        private final int dataCommand;
        private final ByteBuf payload;
        private ConsensusSession.Ticket payloadLease;
        private final CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        private final long deadline;
        private volatile CompletableFuture<ByteBuf> activeExchange;
        // The caller's context, or else the first route's, stays fixed within one operation.
        private final AtomicReference<PartitionContext> context;
        private final boolean hasCallerContext;
        // Read once, since the deadline timer can run after the payload was released.
        private final boolean requiresLane;
        private boolean historyRefreshed;
        private boolean retryingRefusal;
        private boolean notPrimarySeen;
        private long notPrimaryRetryMillis = RETRY_INTERVAL_MILLIS;
        private boolean terminalRoutingRefusal;
        private boolean dataSent;
        // The lookups until a route is used start at the metadata primary, which a backup that missed the namespace
        // lags behind.
        private boolean backupMiss;
        private long lifecycleRetryMillis = RETRY_INTERVAL_MILLIS;
        private IggyServerException lifecycleRefusal;
        private long partitionsCount;
        private long routeView;
        private final Set<ConnectionInfo> lookupVisited = new HashSet<>();
        private AsyncTcpConnection.TransientFailoverState state = new AsyncTcpConnection.TransientFailoverState();

        private Poll(
                byte[] routingBody,
                int dataCommand,
                ByteBuf payload,
                long deadline,
                Optional<PartitionContext> callerContext) {
            this.key = TargetKey.of(routingBody);
            this.routingBody = routingBody;
            this.dataCommand = dataCommand;
            this.payload = payload;
            this.deadline = deadline;
            this.context = new AtomicReference<>(callerContext.orElse(null));
            this.hasCallerContext = callerContext.isPresent();
            this.requiresLane = isMutation()
                    || (dataCommand == CommandCode.Messages.POLL.getValue()
                            && (payload.getUnsignedByte(payload.writerIndex() - 1) != 0
                                    || payload.getUnsignedByte(payload.writerIndex() - POLL_PARAMETERS_BYTES)
                                            == PollingKind.Next.asCode()));
        }

        private boolean isMutation() {
            return VsrOperation.operationForCode(dataCommand) != VsrOperation.NON_REPLICATED;
        }

        private ConsensusSession.LaneKey laneFor(PartitionContext fallback) {
            PartitionContext captured = context.get();
            ByteBuf query = Unpooled.wrappedBuffer(routingBody);
            try {
                return new ConsensusSession.LaneKey(
                        (captured == null ? fallback : captured).incarnation().longValue(),
                        query.getUnsignedIntLE(query.writerIndex() - Integer.BYTES));
            } finally {
                query.release();
            }
        }

        private boolean isLifecycleRefusal(IggyServerException refusal) {
            return dataSent
                    && !state.wasUncertain()
                    && refusal != null
                    && refusal.getRawErrorCode() == IggyErrorCode.LIFECYCLE_BUSY.getCode();
        }

        private boolean restartHistory(IggyServerException refusal) {
            if (dataCommand != CommandCode.Messages.SEND.getValue()
                    || hasCallerContext
                    || historyRefreshed
                    || state.wasUncertain()
                    || refusal == null
                    || refusal.getRawErrorCode() != IggyErrorCode.HISTORY_UNAVAILABLE.getCode()) {
                return false;
            }
            historyRefreshed = true;
            if (state.ticket() != null) {
                state.ticket().complete();
            }
            context.set(null);
            state = state.nextHistory();
            lookupVisited.clear();
            return true;
        }

        private void discardConnection() {
            CompletableFuture<ByteBuf> exchange = activeExchange;
            if (exchange != null) {
                exchange.cancel(false);
            }
        }

        // A refusal leaves nothing in flight on a primary until the next route is known,
        // so the flag must survive the route lookup that follows it.
        private synchronized void routed() {
            retryingRefusal = false;
            lifecycleRefusal = null;
        }

        private synchronized boolean retryRefusal(long delayMillis) {
            retryingRefusal = true;
            if (deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(delayMillis)) {
                fail(notAccepted());
                return false;
            }
            return !result.isDone();
        }

        private void finish(Throwable error) {
            if (error != null) {
                discardConnection();
            }
            payload.release();
            if (payloadLease != null) {
                payloadLease.finishWrite();
            }
            if (state.ticket() != null) {
                if (error != null
                        && !state.committed()
                        && (state.wasUncertain()
                                || unwrap(error) instanceof IggyOperationException operationError
                                        && operationError.getErrorCode() == IggyErrorCode.OPERATION_UNCERTAIN)) {
                    state.retainTicket();
                } else {
                    state.ticket().complete();
                }
            }
        }

        private void fail(Throwable error) {
            result.completeExceptionally(
                    state.ticket() != null && state.wasUncertain() && !state.committed()
                            ? state.retainTicket()
                            : unwrap(error));
        }

        /**
         * Ends a poll that a session change or its deadline caught between attempts. A poll that a sign-in or a
         * sign-out of the caller ended ends with 40, unless its mutation can have taken effect.
         */
        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        private synchronized void endPending() {
            // A link writes a request that needs a lane only after the lane admits it, which gives the request a
            // ticket.
            boolean mayBeWritten = dataSent && (!requiresLane || state.ticket() != null);
            fail(
                    state.ticket() != null && dataSent && !retryingRefusal && lifecycleRefusal == null
                            ? state.retainTicket()
                            : state.signedOut()
                                    ? state.lineageEnded(null)
                                    : lifecycleRefusal != null
                                            ? lifecycleRefusal
                                            : retryingRefusal || !mayBeWritten ? notAccepted() : uncommitted());
        }
    }

    /**
     * The target bytes of a partition request, or their stream and topic prefix, compared by content. Nothing writes
     * the bytes after a key holds them.
     */
    private static final class TargetKey {
        private final byte[] target;
        private final int length;
        private final int hash;

        private TargetKey(byte[] target, int length) {
            this.target = target;
            this.length = length;
            int contentHash = 1;
            for (int index = 0; index < length; index++) {
                contentHash = 31 * contentHash + target[index];
            }
            this.hash = contentHash;
        }

        static TargetKey of(byte[] target) {
            return new TargetKey(target, target.length);
        }

        /** The stream and topic of a partition target, without its trailing partition id. */
        TargetKey topic() {
            return new TargetKey(target, length - Integer.BYTES);
        }

        boolean startsWith(TargetKey prefix) {
            return length >= prefix.length && Arrays.equals(target, 0, prefix.length, prefix.target, 0, prefix.length);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TargetKey key && Arrays.equals(target, 0, length, key.target, 0, key.length);
        }

        @Override
        public int hashCode() {
            return hash;
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
            boolean local,
            long view,
            boolean stale,
            ConnectionInfo hint) {}

    private record Attachment(long clientLow, long clientHigh, long session, long watermark) {
        private Attachment {
            if ((clientLow == 0 && clientHigh == 0) || session == 0) {
                throw new IggyClientException("Invalid primary poll session identity");
            }
        }

        boolean covers(Attachment required) {
            return clientLow == required.clientLow && clientHigh == required.clientHigh && session == required.session;
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
