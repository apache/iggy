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
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollRouterTest {
    private static final long CLIENT_LOW = 1;
    private static final long CLIENT_HIGH = 2;
    private static final long SESSION = 3;
    private static final long ROUTE_WATERMARK = 1;
    private static final long INCARNATION = 7;
    private static final int PRIMARY_PORT = 8090;
    private static final int BIND_SECRET_BYTES = 32;
    private static final int PENDING_POLL_CAPACITY = 4096;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_INCARNATION_OFFSET = 200;
    private static final int REQUEST_METADATA_OP_OFFSET = 216;
    private static final long OPERATION_ID = 9;
    private static final PartitionContext ROUTE_CONTEXT =
            new PartitionContext(BigInteger.valueOf(INCARNATION), BigInteger.ZERO, BigInteger.valueOf(ROUTE_WATERMARK));
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration NEVER = Duration.ofDays(1);

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "STREAM_ID_NOT_FOUND",
                "STREAM_NAME_NOT_FOUND",
                "TOPIC_ID_NOT_FOUND",
                "TOPIC_NAME_NOT_FOUND",
                "PARTITION_NOT_FOUND"
            })
    void shouldRetryANamespaceMissFromALaggingBackup(IggyErrorCode missing) throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 1));
        try {
            coordinator.routeReady = CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0]));
            primary.routeReply.completeExceptionally(
                    IggyServerException.fromTcpResponse(missing.getCode(), new byte[0]));
            var sent = sendMessage(coordinator, router);
            assertThat(sent).isNotDone();
            assertThat(coordinator.routes).isEqualTo(1);
            timers.runLast();
            assertThat(primary.routes).isEqualTo(1);
            assertThat(sent).isNotDone();
            assertThat(primary.sentContexts).isEmpty();

            coordinator.routeReady = CompletableFuture.completedFuture(null);
            timers.runLast();
            timers.runLast();
            sent.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(coordinator.routes).isEqualTo(2);
            assertThat(primary.routes).isEqualTo(1);
            assertThat(primary.sentContexts).containsExactly(ROUTE_CONTEXT);
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldTreatOnlyTheRosterLeaderNamespaceMissAsAuthoritative(boolean coordinatorIsLeader) throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 1));
        try {
            coordinator.metadataLeaderPort = coordinatorIsLeader ? PRIMARY_PORT + 1 : PRIMARY_PORT;
            var missing = IggyServerException.fromTcpResponse(IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode(), new byte[0]);
            coordinator.routeReady = CompletableFuture.failedFuture(missing);
            primary.routeReply.completeExceptionally(missing);
            var sent = sendMessage(coordinator, router);
            if (!coordinatorIsLeader) {
                assertThat(sent).isNotDone();
                timers.runLast();
            }
            assertServerError(sent, IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode());
            assertThat(coordinator.routes).isEqualTo(1);
            assertThat(primary.routes).isEqualTo(coordinatorIsLeader ? 0 : 1);
            assertThat(primary.sentContexts).isEmpty();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {"UNAUTHORIZED", "INVALID_CREDENTIALS", "INVALID_FORMAT"})
    void shouldKeepOtherBackupLookupFailuresTerminal(IggyErrorCode error) throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 1));
        try {
            coordinator.routeReady = CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0]));
            primary.routeReply.completeExceptionally(IggyServerException.fromTcpResponse(error.getCode(), new byte[0]));
            var sent = sendMessage(coordinator, router);
            assertThat(sent).isNotDone();
            timers.runLast();
            assertServerError(sent, error.getCode());
            assertThat(coordinator.routes).isEqualTo(1);
            assertThat(primary.routes).isEqualTo(1);
            assertThat(primary.sentContexts).isEmpty();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldKeepAnUnknownLeaderNamespaceMissInsideTheOriginalDeadline() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 1));
        try {
            coordinator.metadataLeaderPort = 0;
            coordinator.routeReady = CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode(), new byte[0]));
            var sent = sendMessage(coordinator, router);
            assertThat(sent).isNotDone();
            assertThat(coordinator.routes).isEqualTo(1);
            timers.scheduled.get(0).run();
            assertServerError(sent, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED);
            assertThat(primary.sentContexts).isEmpty();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldAllowAnotherResolveAfterCancellationBeforeTheExchange(boolean duringBind) throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            var ticket = retainOffset(coordinator);
            byte[] originalFrame = frozenFrame(ticket);
            if (duringBind) {
                primary.binding = new CompletableFuture<>();
            } else {
                coordinator.routeReady = new CompletableFuture<>();
            }
            var cancelled = router.resolve(OPERATION_ID, NEVER);
            assertThat(cancelled).isNotDone();
            assertThat(primary.sentContexts).isEmpty();
            assertThat(cancelled.cancel(false)).isTrue();

            var resolved = router.resolve(OPERATION_ID, NEVER);
            assertThat(resolved).isNotDone();
            primary.beforeSend = ticket::complete;
            if (duringBind) {
                primary.binding.complete(Unpooled.EMPTY_BUFFER);
            } else {
                coordinator.routeReady.complete(null);
            }
            resolved.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).release();
            assertThat(primary.sentContexts).containsExactly(ROUTE_CONTEXT);
            assertThat(primary.sentFrames).hasSize(1);
            assertThat(primary.sentFrames.get(0)).containsExactly(originalFrame);
            assertThatThrownBy(() -> coordinator
                            .sessionOwner()
                            .resume(OPERATION_ID, System.nanoTime() + NEVER.toNanos(), coordinator.eventLoop())
                            .join())
                    .hasCauseInstanceOf(IggyOperationException.class)
                    .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                            .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            coordinator.sessionOwner().reset();
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldKeepTheNextResolverClaimWhenPreviousCompletionReentersRetention() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        var owner = coordinator.sessionOwner();
        try {
            var ticket = retainOffset(coordinator);
            long deadline = System.nanoTime() + NEVER.toNanos();
            owner.resume(OPERATION_ID, deadline, timers).join();
            var previous = new AsyncTcpConnection.TransientFailoverState();
            previous.restore(ticket);
            var waiting = owner.resume(OPERATION_ID, deadline, timers);
            var completed = waiting.thenRun(previous::retainTicket);

            previous.retainTicket();
            completed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            var later = owner.resume(OPERATION_ID, deadline, timers);
            assertThat(later).isNotDone();
            ticket.recordOutcome(Unpooled.EMPTY_BUFFER, null);
            ticket.complete();
            assertThat(owner.provedOutcome(OPERATION_ID)).isEmpty();
        } finally {
            owner.reset();
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReleaseResolverBytesAfterQueueRejectionAndAbandon() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            var ticket = retainOffset(coordinator);
            int frameBytes = frozenFrame(ticket).length;
            coordinator.routeReady = new CompletableFuture<>();
            for (int index = 0; index < PENDING_POLL_CAPACITY; index++) {
                assertThat(router.poll(pollPayload(), Optional.empty())).isNotDone();
            }
            assertThatThrownBy(() -> router.resolve(OPERATION_ID, NEVER).join())
                    .hasCauseInstanceOf(IggyClientException.class)
                    .hasRootCauseMessage("Too many pending primary polls and offset writes");
            coordinator.sessionOwner().abandon(OPERATION_ID);
            var next = coordinator
                    .sessionOwner()
                    .acquire(
                            new ConsensusSession.LaneKey(INCARNATION, 0),
                            true,
                            frameBytes,
                            System.nanoTime() + NEVER.toNanos(),
                            timers);
            assertThat(next).isDone();
            next.join().complete();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            coordinator.sessionOwner().reset();
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldNotDeadlockWhenTheTimeoutFiresWhileAnInlineSendHoldsTheRouter() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            Primary primary = new Primary(group);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
            // The first poll caches the route and leaves its slot idle and attached, so the
            // second one routes, attaches and sends inline while its caller holds the router.
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();

            CompletableFuture<Void> enteredSend = new CompletableFuture<>();
            CompletableFuture<Void> releaseSend = new CompletableFuture<>();
            int timerIndex = timers.scheduled.size();
            primary.beforeSend = () -> {
                enteredSend.complete(null);
                releaseSend.join();
            };
            CompletableFuture<CompletableFuture<ByteBuf>> polled = new CompletableFuture<>();
            Thread caller = start("poll-caller", () -> polled.complete(router.poll(pollPayload(), Optional.empty())));
            enteredSend.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<Void> expired = new CompletableFuture<>();
            Thread timeout = start("poll-timeout", () -> {
                timers.scheduled.get(timerIndex).run();
                expired.complete(null);
            });
            try {
                assertThat(expired).succeedsWithin(TIMEOUT);
            } finally {
                releaseSend.complete(null);
            }
            assertThat(polled).succeedsWithin(TIMEOUT);
            assertServerError(polled.get(), AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
            caller.join(TIMEOUT.toMillis());
            timeout.join(TIMEOUT.toMillis());
            assertThat(caller.isAlive()).isFalse();
            assertThat(timeout.isAlive()).isFalse();
            assertThat(primary.sentContexts).containsExactly(ROUTE_CONTEXT, ROUTE_CONTEXT);
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRefreshCachedHistoryBeforeANewPollAfterMetadataAdvances() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            Primary primary = new Primary(group);
            primary.holdAfterFirst = false;
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            coordinator.watermark = ROUTE_WATERMARK + 1;
            coordinator.context = new PartitionContext(
                    BigInteger.valueOf(INCARNATION + 1), BigInteger.ZERO, BigInteger.valueOf(coordinator.watermark));
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            assertThat(primary.sentContexts).containsExactly(ROUTE_CONTEXT, coordinator.context);
            assertThat(coordinator.routes).isEqualTo(2);
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRefuseAnExtraReplicaWithoutClosingHealthyLinks() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        List<Primary> links = new CopyOnWriteArrayList<>();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> {
                Primary primary = new Primary(group);
                links.add(primary);
                return primary;
            });
            int replicaCapacity = 1 << Byte.SIZE;
            for (int replica = 0; replica < replicaCapacity; replica++) {
                router.connectionFor(
                                new ConnectionInfo("localhost", PRIMARY_PORT + replica),
                                System.nanoTime() + TIMEOUT.toNanos())
                        .get(5, TimeUnit.SECONDS);
            }
            Primary first = links.get(0);
            first.sendRouted(
                            CommandCode.System.PING.getValue(),
                            Unpooled.EMPTY_BUFFER,
                            0,
                            0,
                            new AsyncTcpConnection.TransientFailoverState())
                    .join()
                    .release();
            var pending = first.sendRouted(
                    CommandCode.System.PING.getValue(),
                    Unpooled.EMPTY_BUFFER,
                    0,
                    0,
                    new AsyncTcpConnection.TransientFailoverState());
            assertThatThrownBy(() -> router.connectionFor(
                            new ConnectionInfo("localhost", PRIMARY_PORT + replicaCapacity),
                            System.nanoTime() + TIMEOUT.toNanos()))
                    .isInstanceOf(org.apache.iggy.exception.IggyServerException.class);
            assertThat(links).hasSize(replicaCapacity);
            assertThat(pending).isNotDone();
            assertThat(links).allSatisfy(link -> assertThat(link.closes).hasValue(0));
            router.clearSession().get(5, TimeUnit.SECONDS);
            assertThat(pending).failsWithin(TIMEOUT);
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<SendMessagesResponse> sendMessage(Coordinator coordinator, PollRouter router) {
        return new MessagesTcpClient(() -> coordinator, new ClientRoutingState(), router)
                .sendMessages(
                        StreamId.of(1L), TopicId.of(1L), Partitioning.partitionId(0L), List.of(Message.of("message")));
    }

    private static ConsensusSession.Ticket retainOffset(Coordinator coordinator) {
        ByteBuf payload = pollPayload();
        payload.writerIndex(payload.writerIndex() - PollRouter.POLL_PARAMETERS_BYTES);
        payload.writeLongLE(1);
        int frameBytes = VsrHeaders.HEADER_SIZE + payload.readableBytes();
        var owner = coordinator.sessionOwner();
        owner.configurePendingOperations(1, frameBytes);
        var ticket = owner.acquire(
                        new ConsensusSession.LaneKey(INCARNATION, 0),
                        true,
                        frameBytes,
                        System.nanoTime() + NEVER.toNanos(),
                        coordinator.eventLoop())
                .join();
        ByteBuf frame =
                Unpooled.buffer(frameBytes).writeZero(VsrHeaders.HEADER_SIZE).writeBytes(payload);
        payload.release();
        frame.setLongLE(REQUEST_ID_OFFSET, OPERATION_ID);
        frame.setLongLE(REQUEST_INCARNATION_OFFSET, INCARNATION);
        frame.setLongLE(REQUEST_METADATA_OP_OFFSET, ROUTE_WATERMARK);
        ByteBuf routing = Unpooled.buffer()
                .writeByte(1)
                .writeByte(Integer.BYTES)
                .writeIntLE(1)
                .writeByte(1)
                .writeByte(Integer.BYTES)
                .writeIntLE(1)
                .writeIntLE(0);
        ticket.freeze(frame, CommandCode.ConsumerOffset.STORE.getValue(), ByteBufUtil.getBytes(routing));
        routing.release();
        frame.release();
        ticket.finishWrite();
        ticket.startWrite();
        ticket.finishWrite();
        ticket.retain();
        return ticket;
    }

    private static byte[] frozenFrame(ConsensusSession.Ticket ticket) {
        ByteBuf frame = ticket.frame();
        try {
            return ByteBufUtil.getBytes(frame);
        } finally {
            frame.release();
            ticket.finishWrite();
        }
    }

    private static ByteBuf pollPayload() {
        // The router keys routes by the bytes before the poll parameters and reads nothing else.
        return Unpooled.buffer()
                .writeByte(1)
                .writeByte(1)
                .writeByte(4)
                .writeIntLE(0)
                .writeByte(1)
                .writeByte(4)
                .writeIntLE(1)
                .writeByte(1)
                .writeByte(4)
                .writeIntLE(1)
                .writeByte(1)
                .writeIntLE(0)
                .writeZero(PollRouter.POLL_PARAMETERS_BYTES);
    }

    private static Thread start(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        // A deadlocked thread must not keep the test JVM alive.
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /** Holds every timer back, so the test fires a poll timeout at the instant it needs. */
    private static final class ManualTimers extends DefaultEventLoop {
        private final List<Runnable> scheduled = new CopyOnWriteArrayList<>();

        void runLast() {
            scheduled.get(scheduled.size() - 1).run();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduled.add(command);
            return super.schedule(command, NEVER.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** Never dials: the router only reaches what the subclasses override. */
    private abstract static class OfflineConnection extends AsyncTcpConnection {
        OfflineConnection(IoEventLoopGroup group) {
            super(
                    InetAddress.getLoopbackAddress().getHostAddress(),
                    PRIMARY_PORT,
                    false,
                    Optional.empty(),
                    AsyncTcpConnection.TcpConnectionPoolConfig.builder().build(),
                    Optional.of(group),
                    1,
                    Optional.empty(),
                    Optional.empty(),
                    NEVER,
                    VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                    null,
                    errorCode -> {},
                    (source, cause) -> {});
        }
    }

    private static final class Coordinator extends OfflineConnection {
        private final EventLoop timers;
        private long watermark;
        private PartitionContext context = ROUTE_CONTEXT;
        private int routes;
        private int metadataLeaderPort = PRIMARY_PORT + 1;
        private CompletableFuture<Void> routeReady = CompletableFuture.completedFuture(null);

        Coordinator(IoEventLoopGroup group, EventLoop timers) {
            super(group);
            this.timers = timers;
        }

        @Override
        EventLoop eventLoop() {
            return timers;
        }

        @Override
        boolean isAuthenticated() {
            return true;
        }

        @Override
        long metadataWatermark() {
            return watermark;
        }

        @Override
        byte[] bindSecret(long clientLow, long clientHigh, long epoch) {
            return new byte[BIND_SECRET_BYTES];
        }

        @Override
        long[] sessionIdentity() {
            return new long[] {CLIENT_LOW, CLIENT_HIGH, SESSION};
        }

        @Override
        CompletableFuture<ByteBuf> sendRouted(
                int commandCode, ByteBuf payload, long generation, long deadline, TransientFailoverState state) {
            payload.release();
            if (commandCode == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                return CompletableFuture.completedFuture(AsyncIggyTcpClientTransientFailoverTest.clusterMetadata(
                        PRIMARY_PORT + 1, PRIMARY_PORT, metadataLeaderPort));
            }
            assertThat(commandCode).isEqualTo(CommandCode.Messages.GET_PARTITION_ROUTE.getValue());
            routes++;
            Request routing = new Request(
                    OPERATION_NON_REPLICATED,
                    GET_PARTITION_ROUTE_CODE,
                    0,
                    CLIENT_LOW,
                    CLIENT_HIGH,
                    0,
                    0,
                    0,
                    new byte[0],
                    0);
            return routeReady.thenApply(ignored -> pollRoute(
                    routing,
                    SESSION,
                    context.metadataOp().longValue(),
                    PRIMARY_PORT,
                    context.incarnation().longValue()));
        }
    }

    /** Answers the first poll and leaves every later one in flight until it is closed. */
    private static final class Primary extends OfflineConnection {
        private final List<PartitionContext> sentContexts = new CopyOnWriteArrayList<>();
        private final List<byte[]> sentFrames = new CopyOnWriteArrayList<>();
        private final CompletableFuture<ByteBuf> unanswered = new CompletableFuture<>();
        private final CompletableFuture<ByteBuf> routeReply = new CompletableFuture<>();
        private int routes;
        private CompletableFuture<ByteBuf> binding = CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
        private final AtomicInteger closes = new AtomicInteger();
        private boolean holdAfterFirst = true;
        private Runnable beforeSend = () -> {};

        Primary(IoEventLoopGroup group) {
            super(group);
        }

        @Override
        public CompletableFuture<Void> connect() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        CompletableFuture<ByteBuf> sendRouted(
                int commandCode, ByteBuf payload, long sessionGeneration, long deadline, TransientFailoverState state) {
            payload.release();
            if (commandCode == CommandCode.System.BIND_SESSION.getValue()) {
                return binding;
            }
            if (commandCode == GET_PARTITION_ROUTE_CODE) {
                routes++;
                return routeReply;
            }
            sentContexts.add(state.context());
            if (state.ticket() != null) {
                sentFrames.add(frozenFrame(state.ticket()));
            }
            beforeSend.run();
            return !holdAfterFirst || sentContexts.size() == 1
                    ? CompletableFuture.completedFuture(
                            commandCode == CommandCode.Messages.SEND.getValue() ? Unpooled.EMPTY_BUFFER : emptyPoll(0))
                    : unanswered;
        }

        @Override
        public CompletableFuture<Void> close() {
            closes.incrementAndGet();
            // A closed channel fails what it still awaits on its own event loop, as channelInactive does.
            eventLoop()
                    .execute(() -> unanswered.completeExceptionally(
                            new IggyConnectionException("Connection closed before a response arrived")));
            return CompletableFuture.completedFuture(null);
        }
    }
}
