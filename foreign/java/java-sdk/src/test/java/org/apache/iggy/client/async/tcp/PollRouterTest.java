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
import io.netty.util.ResourceLeakDetector;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEventLoopGroupTest.await;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.connection;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
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
    // Outlasts the queueing of a dial, and passes while the dial waits for a permit.
    private static final Duration QUEUED_DIAL_BUDGET = Duration.ofMillis(200);
    // Shorter than the retry interval of the router, so a failed attempt is the last one.
    private static final Duration LAST_RETRY_WINDOW = Duration.ofMillis(40);
    private static final long LANE_FRAME_BYTES = 1;
    // The lane of partition 0, which sendMessage writes to.
    private static final ConsensusSession.LaneKey PARTITION_LANE = new ConsensusSession.LaneKey(INCARNATION, 0);

    private static ResourceLeakDetector.Level leakDetection;

    @BeforeAll
    static void detectEveryLeak() {
        leakDetection = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterAll
    static void restoreLeakDetection() {
        ResourceLeakDetector.setLevel(leakDetection);
    }

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
            primary.metadataLeaderPort = coordinator.metadataLeaderPort;
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

    /**
     * A send that waits for a lane another operation holds was never written. Its deadline fails it as not accepted,
     * whichever of the lane wait and the poll timeout fires first.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldFailASendThatWaitsForItsLaneAsNotAccepted(boolean laneTimeoutFirst) throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        LaneTimers laneTimers = new LaneTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        primary.laneTimers = laneTimers;
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 1));
        var holder = coordinator
                .sessionOwner()
                .acquire(PARTITION_LANE, true, LANE_FRAME_BYTES, System.nanoTime() + NEVER.toNanos(), group.next())
                .join();
        try {
            var sent = sendMessage(coordinator, router);
            await(() -> !laneTimers.scheduled.isEmpty(), "the send waiting for its lane", TIMEOUT);
            assertThat(sent).isNotDone();
            Runnable pollTimeout = timers.scheduled.get(0);
            Callable<?> laneTimeout = laneTimers.scheduled.get(0);
            if (laneTimeoutFirst) {
                laneTimeout.call();
                pollTimeout.run();
            } else {
                pollTimeout.run();
                laneTimeout.call();
            }
            assertServerError(sent, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED);
        } finally {
            holder.complete();
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            coordinator.sessionOwner().reset();
            laneTimers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
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

    /**
     * A poll over the pending capacity fails at once, and the router holds no more payloads than that capacity. Each
     * held poll ends with an explicit failure, and every payload goes back.
     */
    @Test
    void shouldReleaseEveryPayloadAboveAndWithinThePendingPollCapacity() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> new Primary(group));
        try {
            coordinator.routeReady = new CompletableFuture<>();
            List<ByteBuf> payloads = new ArrayList<>();
            List<CompletableFuture<ByteBuf>> held = new ArrayList<>();
            for (int index = 0; index < PENDING_POLL_CAPACITY; index++) {
                ByteBuf payload = tracked(pollPayload());
                payloads.add(payload);
                held.add(router.poll(payload, Optional.empty()));
            }
            ByteBuf rejected = tracked(pollPayload());
            assertThatThrownBy(() -> router.poll(rejected, Optional.empty()).join())
                    .hasRootCauseMessage("Too many pending primary polls and offset writes");
            assertThat(rejected.refCnt()).isZero();
            assertThat(payloads)
                    .allSatisfy(payload -> assertThat(payload.refCnt()).isOne());

            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(held).allSatisfy(poll -> assertServerError(poll, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED));
            assertThat(payloads)
                    .allSatisfy(payload -> assertThat(payload.refCnt()).isZero());
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /** A reply that comes after its poll timed out goes back, and so does the payload of that poll. */
    @Test
    void shouldReleaseALateReplyAndThePayloadOfARoutedPollThatTimedOut() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            int pollTimeout = timers.scheduled.size();
            ByteBuf late = tracked(emptyPoll(0));
            primary.replies.add(CompletableFuture.completedFuture(late));
            primary.beforeSend = () -> timers.scheduled.get(pollTimeout).run();
            ByteBuf payload = tracked(pollPayload());

            var polled = router.poll(payload, Optional.empty());

            assertServerError(polled, AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
            assertThat(late.refCnt()).isZero();
            assertThat(payload.refCnt()).isZero();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReleaseThePayloadOfACancelledRoutedPollAndCancelItsExchange() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            CompletableFuture<ByteBuf> exchange = new CompletableFuture<>();
            primary.replies.add(exchange);
            ByteBuf payload = tracked(pollPayload());
            var polled = router.poll(payload, Optional.empty());
            assertThat(polled).isNotDone();

            assertThat(polled.cancel(false)).isTrue();

            assertThat(exchange).isCancelled();
            assertThat(payload.refCnt()).isZero();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldBackOffRepeatedNotPrimaryRepliesBeforeRoutingAgain() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            for (int refusal = 0; refusal < 4; refusal++) {
                primary.replies.add(CompletableFuture.failedFuture(
                        IggyServerException.fromTcpResponse(AsyncTcpConnection.NOT_PRIMARY, new byte[0])));
            }
            primary.replies.add(CompletableFuture.completedFuture(emptyPoll(0)));
            var poll = router.poll(pollPayload(), Optional.empty());
            List<Long> pauses = new ArrayList<>();
            for (int refusal = 0; refusal < 4; refusal++) {
                pauses.add(timers.delaysMillis.get(timers.delaysMillis.size() - 1));
                coordinator.routeView = refusal + 1;
                primary.routeReply = coordinator.sendRouted(
                        GET_PARTITION_ROUTE_CODE,
                        Unpooled.EMPTY_BUFFER,
                        0,
                        System.nanoTime() + TIMEOUT.toNanos(),
                        new AsyncTcpConnection.TransientFailoverState());
                timers.runLast();
            }
            poll.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).release();
            assertThat(pauses).containsExactly(0L, 50L, 100L, 200L);
            assertThat(primary.sentContexts).hasSize(6);
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReleaseThePayloadOfARoutedPollRetriedAfterARefusal() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            ByteBuf reply = tracked(emptyPoll(0));
            primary.replies.add(CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0])));
            primary.replies.add(CompletableFuture.completedFuture(reply));
            ByteBuf payload = tracked(pollPayload());
            var polled = router.poll(payload, Optional.empty());
            assertThat(polled).isNotDone();

            timers.runLast();

            assertThat(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isSameAs(reply);
            reply.release();
            assertThat(primary.sentContexts).hasSize(3);
            assertThat(payload.refCnt()).isZero();
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A dead primary answers nothing, so only its lost link can move the partition. The next attempt reads the route
     * again on the replica after the dead one, which a view change makes the next primary.
     */
    @Test
    void shouldRouteAgainOnTheNextReplicaAfterThePrimaryLinkIsLost() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary lost = new Primary(group);
        Primary next = new Primary(group);
        PollRouter router =
                new PollRouter(() -> coordinator, endpoint -> endpoint.port() == PRIMARY_PORT ? lost : next);
        router.coordinatorEndpoint(
                () -> new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT + 2));
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            lost.replies.add(CompletableFuture.failedFuture(new IggyConnectionException("Connection reset")));
            coordinator.routePort = PRIMARY_PORT + 1;
            next.routeReply = coordinator.sendRouted(
                    GET_PARTITION_ROUTE_CODE,
                    Unpooled.EMPTY_BUFFER,
                    0,
                    System.nanoTime() + TIMEOUT.toNanos(),
                    new AsyncTcpConnection.TransientFailoverState());
            var poll = router.poll(pollPayload(), Optional.empty());

            timers.runLast();

            poll.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).release();
            assertThat(lost.sentContexts).hasSize(2);
            assertThat(next.routes).isEqualTo(1);
            assertThat(next.sentContexts).hasSize(1);
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /** A write that never reached a primary before its deadline was not accepted, also when it lost its link. */
    @Test
    void shouldEndAWriteThatLostThePrimaryLinkAtItsDeadlineAsNotAccepted() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        Coordinator coordinator = new Coordinator(group, timers);
        Primary primary = new Primary(group);
        PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
        try {
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            primary.replies.add(CompletableFuture.failedFuture(new IggyConnectionException("Connection reset")));

            var stored = router.partitionRequest(
                    CommandCode.ConsumerOffset.STORE.getValue(),
                    offsetPayload(),
                    Optional.empty(),
                    System.nanoTime() + LAST_RETRY_WINDOW.toNanos());

            assertServerError(stored, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED);
        } finally {
            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A link that refuses the lane of a routed offset write never writes it. The link releases the copy that the
     * router gave it, and the router releases the payload. The parent signs in, because a link checks its bind
     * reply against the session of its parent.
     */
    @Test
    void shouldReleaseTheRoutedPayloadOfAnOffsetWriteWhoseLaneTheLinkRefuses() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 1, loopback);
                ServerSocket primarySocket = new ServerSocket(0, 1, loopback)) {
            var coordinatorServing = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(SESSION));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.clusterMetadata(
                                    coordinatorSocket.getLocalPort(),
                                    primarySocket.getLocalPort(),
                                    coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, SESSION, ROUTE_WATERMARK, primarySocket.getLocalPort(), INCARNATION));
                }
                return pong(request);
            });
            var primaryServing = serve(primarySocket, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
                }
                return pong(request);
            });
            AsyncTcpConnection parent = connection(coordinatorSocket);
            AtomicReference<AsyncTcpConnection> link = new AtomicReference<>();
            PollRouter router = new PollRouter(() -> parent, endpoint -> {
                link.set(connection(primarySocket));
                return link.get();
            });
            router.coordinatorEndpoint(
                    () -> new ConnectionInfo(loopback.getHostAddress(), coordinatorSocket.getLocalPort()));
            try {
                parent.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                new UsersTcpClient(() -> parent)
                        .login("user", "password")
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                retainOffset(parent);
                ByteBuf payload = tracked(offsetPayload());

                var stored =
                        router.partitionRequest(CommandCode.ConsumerOffset.STORE.getValue(), payload, Optional.empty());

                assertThatThrownBy(() -> stored.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.LANE_BLOCKED));
                // The link fails the write in a task of its event loop, which can wake this thread before the
                // router's completion callback in that task releases the payload.
                link.get().eventLoop().submit(() -> {}).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(payload.refCnt()).isZero();
            } finally {
                router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                parent.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                parent.sessionOwner().reset();
            }
            coordinatorServing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            primaryServing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
    void shouldClearTheSessionWhileAPollTimeoutCompletes() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        CompletableFuture<Void> releaseCompletion = new CompletableFuture<>();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            Primary primary = new Primary(group);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
            router.poll(pollPayload(), Optional.empty())
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .release();
            int timerIndex = timers.scheduled.size();
            ByteBuf payload = tracked(pollPayload());
            var polled = router.poll(payload, Optional.empty());
            CompletableFuture<Void> completing = new CompletableFuture<>();
            polled.whenComplete((response, error) -> {
                completing.complete(null);
                releaseCompletion.join();
            });
            CompletableFuture<Void> expired = new CompletableFuture<>();
            Thread timeout = start("poll-timeout", () -> {
                timers.scheduled.get(timerIndex).run();
                expired.complete(null);
            });
            completing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<Void> cleared = new CompletableFuture<>();
            Thread clearing = start("session-clear", () -> {
                router.clearSession().join();
                cleared.complete(null);
            });
            // Force shutdown to overlap the timeout's completion callbacks before they remove the pending poll.
            await(() -> clearing.getState() == Thread.State.BLOCKED, "session clear waiting for the poll", TIMEOUT);
            releaseCompletion.complete(null);

            assertThat(expired).succeedsWithin(TIMEOUT);
            assertThat(cleared).succeedsWithin(TIMEOUT);
            assertServerError(polled, AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
            assertThat(payload.refCnt()).isZero();
            assertThat(primary.closes).hasValue(1);
            timeout.join(TIMEOUT.toMillis());
            clearing.join(TIMEOUT.toMillis());
            assertThat(timeout.isAlive()).isFalse();
            assertThat(clearing.isAlive()).isFalse();
        } finally {
            releaseCompletion.complete(null);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A slot opens its link under the slot monitor, where the client reads the metadata primary to size the dial
     * timeout. A roster read that drops the endpoint of that slot meanwhile closes the slot, so it waits for that
     * monitor, and it must not hold the router while it waits.
     */
    @Test
    void shouldReadTheMetadataPrimaryWhileARosterReadClosesTheSlotThatOpensItsLink() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        CompletableFuture<Void> opening = new CompletableFuture<>();
        CompletableFuture<Void> releaseOpen = new CompletableFuture<>();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            AtomicReference<PollRouter> routerOfLinks = new AtomicReference<>();
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> {
                opening.complete(null);
                releaseOpen.join();
                routerOfLinks.get().metadataPrimary();
                return new Primary(group);
            });
            routerOfLinks.set(router);
            // A roster spells its nodes by address, so it never names the slot of a hostname.
            ConnectionInfo hostname = new ConnectionInfo("localhost", PRIMARY_PORT);
            CompletableFuture<Void> dialed = new CompletableFuture<>();
            Thread dialing = start("slot-open", () -> {
                try {
                    router.connectionFor(hostname, System.nanoTime() + TIMEOUT.toNanos());
                    dialed.complete(null);
                } catch (RuntimeException error) {
                    dialed.completeExceptionally(error);
                }
            });
            opening.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<Void> observed = new CompletableFuture<>();
            ByteBuf metadata = coordinator.roster().join();
            Thread roster = start("roster-read", () -> {
                try {
                    router.observeTopology(BytesDeserializer.readClusterMetadata(metadata));
                } finally {
                    metadata.release();
                }
                observed.complete(null);
            });
            await(() -> roster.getState() == Thread.State.BLOCKED || observed.isDone(), "roster read", TIMEOUT);
            releaseOpen.complete(null);

            assertThat(dialed).succeedsWithin(TIMEOUT);
            assertThat(observed).succeedsWithin(TIMEOUT);
            assertThat(router.metadataPrimary().port()).isEqualTo(PRIMARY_PORT + 1);
            dialing.join(TIMEOUT.toMillis());
            roster.join(TIMEOUT.toMillis());
            assertThat(dialing.isAlive()).isFalse();
            assertThat(roster.isAlive()).isFalse();
        } finally {
            releaseOpen.complete(null);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A dial that waits for a permit fails once its deadline passes. Its waiters fail inline, and one of them can wait
     * for the client monitor, which a sign-in holds while it clears the session. So that failure must not hold the
     * router.
     */
    @Test
    void shouldClearTheSessionWhileAnExpiredQueuedDialFailsItsWaiters() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        CompletableFuture<Void> proceed = new CompletableFuture<>();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> new StalledDial(group));
            String loopback = InetAddress.getLoopbackAddress().getHostAddress();
            long later = System.nanoTime() + TIMEOUT.toNanos();
            for (int port = PRIMARY_PORT; port < PRIMARY_PORT + PollRouter.MAX_CONCURRENT_DIALS; port++) {
                router.connectionFor(new ConnectionInfo(loopback, port), later);
            }
            ConnectionInfo queued = new ConnectionInfo(loopback, PRIMARY_PORT + PollRouter.MAX_CONCURRENT_DIALS);
            int retryIndex = timers.scheduled.size();
            long expiry = System.nanoTime() + QUEUED_DIAL_BUDGET.toNanos();
            assertThat(router.connectionFor(queued, expiry)).isNotDone();
            Runnable retry = timers.scheduled.get(retryIndex);
            Object clientMonitor = new Object();
            CompletableFuture<Void> waiterEnded = new CompletableFuture<>();
            var joined = router.connectionFor(queued, later);
            joined.whenComplete((connection, error) -> {
                synchronized (clientMonitor) {
                    waiterEnded.complete(null);
                }
            });
            await(() -> System.nanoTime() - expiry > 0, "deadline of the queued dial", TIMEOUT);

            CompletableFuture<Void> holding = new CompletableFuture<>();
            CompletableFuture<Void> cleared = new CompletableFuture<>();
            Thread signIn = start("sign-in", () -> {
                synchronized (clientMonitor) {
                    holding.complete(null);
                    proceed.join();
                    router.clearSession();
                }
                cleared.complete(null);
            });
            holding.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Thread dialRetry = start("dial-retry", retry);
            // The expired dial failed the waiter, whose callback now waits for the client monitor.
            await(() -> joined.isDone() && dialRetry.getState() == Thread.State.BLOCKED, "failed waiter", TIMEOUT);
            proceed.complete(null);

            assertThat(cleared).succeedsWithin(TIMEOUT);
            assertThat(waiterEnded).succeedsWithin(TIMEOUT);
            assertServerError(joined, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED);
            signIn.join(TIMEOUT.toMillis());
            dialRetry.join(TIMEOUT.toMillis());
            assertThat(signIn.isAlive()).isFalse();
            assertThat(dialRetry.isAlive()).isFalse();
        } finally {
            proceed.complete(null);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A close ends the dial of its slot, and the waiters of that dial fail inline. They take the router, poll and
     * client monitors, and a holder of the client monitor can wait for the slot monitor, so they must run outside it.
     */
    @Test
    void shouldFailTheWaitersOfADialOutsideTheRouterMonitorsWhenItsSlotCloses() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> new StalledDial(group));
            var waiter = router.connectionFor(
                    new ConnectionInfo(InetAddress.getLoopbackAddress().getHostAddress(), PRIMARY_PORT),
                    System.nanoTime() + TIMEOUT.toNanos());
            CompletableFuture<List<String>> monitors = new CompletableFuture<>();
            waiter.whenComplete((connection, error) -> monitors.complete(heldMonitors()));

            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(waiter).isCompletedExceptionally();
            assertThat(monitors.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("monitors held while the waiters of the dial fail")
                    .noneMatch(monitor -> monitor.startsWith(PollRouter.class.getName()));
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A poll deadline ends the poll under the poll monitor. A callback of that poll can call a synchronized client
     * method, which waits for the client monitor. A sign-in holds that monitor while it ends the pending polls, and
     * that takes the poll monitor. So the poll future of the caller must complete after the poll monitor is free.
     */
    @Test
    void shouldRunAPollCallbackOutsideThePollMonitorWhileASignInEndsThePendingPolls() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        CompletableFuture<Void> proceed = new CompletableFuture<>();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            Primary primary = new Primary(group);
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
            MessagesTcpClient messages = new MessagesTcpClient(() -> coordinator, new ClientRoutingState(), router);
            // The first poll caches the route, and the primary holds the next one.
            pollMessage(messages).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            int deadlineIndex = timers.scheduled.size();
            var polled = pollMessage(messages);
            Object clientMonitor = new Object();
            CompletableFuture<List<String>> monitors = new CompletableFuture<>();
            CompletableFuture<Thread> callback = new CompletableFuture<>();
            CompletableFuture<Void> callbackEnded = new CompletableFuture<>();
            polled.whenComplete((response, error) -> {
                monitors.complete(heldMonitors());
                callback.complete(Thread.currentThread());
                synchronized (clientMonitor) {
                    callbackEnded.complete(null);
                }
            });

            CompletableFuture<Void> holding = new CompletableFuture<>();
            CompletableFuture<Void> cleared = new CompletableFuture<>();
            Thread signIn = start("sign-in", () -> {
                synchronized (clientMonitor) {
                    holding.complete(null);
                    proceed.join();
                    router.clearSession();
                }
                cleared.complete(null);
            });
            holding.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Thread deadline = start("poll-deadline", timers.scheduled.get(deadlineIndex));
            await(
                    () -> callback.isDone() && callback.join().getState() == Thread.State.BLOCKED,
                    "the poll callback waiting for the client monitor",
                    TIMEOUT);
            proceed.complete(null);

            assertThat(cleared).succeedsWithin(TIMEOUT);
            assertThat(callbackEnded).succeedsWithin(TIMEOUT);
            assertServerError(polled, AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
            assertThat(monitors.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .as("monitors held while the poll callback runs")
                    .noneMatch(monitor -> monitor.startsWith(PollRouter.class.getName()));
            signIn.join(TIMEOUT.toMillis());
            deadline.join(TIMEOUT.toMillis());
            assertThat(signIn.isAlive()).isFalse();
            assertThat(deadline.isAlive()).isFalse();
        } finally {
            proceed.complete(null);
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A poll can end at its deadline after its coordinator is gone. The caller must still get the outcome, so the
     * relay to the caller cannot need the coordinator at that point.
     */
    @Test
    void shouldCompleteAPollWhoseCoordinatorIsGoneWhenItEnds() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            AtomicReference<AsyncTcpConnection> current = new AtomicReference<>(coordinator);
            Primary primary = new Primary(group);
            PollRouter router = new PollRouter(current::get, endpoint -> primary);
            MessagesTcpClient messages = new MessagesTcpClient(current::get, new ClientRoutingState(), router);
            // The first poll caches the route, and the primary holds the next one.
            pollMessage(messages).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            int deadlineIndex = timers.scheduled.size();
            var polled = pollMessage(messages);

            current.set(null);
            timers.scheduled.get(deadlineIndex).run();

            assertServerError(polled, AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
        } finally {
            timers.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A sign-in can close a slot while its bind is in flight, and the bind can still succeed. The slot then has no
     * link, so a redial to it must fail instead of publishing a null coordinator.
     */
    @Test
    void shouldRefuseAConnectionWhoseSlotClosesDuringItsBind() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        ManualTimers timers = new ManualTimers();
        try {
            Coordinator coordinator = new Coordinator(group, timers);
            Primary primary = new Primary(group);
            primary.binding = new CompletableFuture<>();
            PollRouter router = new PollRouter(() -> coordinator, endpoint -> primary);
            ConnectionInfo target = new ConnectionInfo("localhost", PRIMARY_PORT);
            var connection = router.connectionFor(target, System.nanoTime() + TIMEOUT.toNanos());
            assertThat(connection)
                    .as("the connection while its bind is in flight")
                    .isNotDone();

            router.clearSession().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            primary.binding.complete(Unpooled.EMPTY_BUFFER);

            assertServerError(connection, AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED);
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

    private static CompletableFuture<PolledMessages> pollMessage(MessagesTcpClient messages) {
        return messages.pollMessages(
                StreamId.of(1L),
                TopicId.of(1L),
                Optional.of(0L),
                Consumer.of(ConsumerId.of(7L)),
                PollingStrategy.offset(BigInteger.ZERO),
                1L,
                false);
    }

    private static ConsensusSession.Ticket retainOffset(AsyncTcpConnection connection) {
        ByteBuf payload = offsetPayload();
        int frameBytes = VsrHeaders.HEADER_SIZE + payload.readableBytes();
        var owner = connection.sessionOwner();
        owner.configurePendingOperations(1, frameBytes);
        var ticket = owner.acquire(
                        new ConsensusSession.LaneKey(INCARNATION, 0),
                        true,
                        frameBytes,
                        System.nanoTime() + NEVER.toNanos(),
                        connection.eventLoop())
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

    /** A StoreConsumerOffset payload for the target of {@link #pollPayload()}. */
    private static ByteBuf offsetPayload() {
        ByteBuf payload = pollPayload();
        payload.writerIndex(payload.writerIndex() - PollRouter.POLL_PARAMETERS_BYTES);
        return payload.writeLongLE(1);
    }

    /** Answers a heartbeat, and fails the mock node on any other request it does not expect. */
    private static Response pong(Request request) {
        if (request.is(CommandCode.System.PING.getValue(), OPERATION_NON_REPLICATED)) {
            return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
        }
        throw new IllegalStateException("Unexpected request: " + request);
    }

    /** A copy that the leak detector tracks, which an unpooled heap buffer is not. */
    private static ByteBuf tracked(ByteBuf source) {
        try {
            return Unpooled.directBuffer(source.readableBytes()).writeBytes(source);
        } finally {
            source.release();
        }
    }

    private static Thread start(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        // A deadlocked thread must not keep the test JVM alive.
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /** The classes of the monitors that the calling thread holds. */
    private static List<String> heldMonitors() {
        ThreadInfo thread = ManagementFactory.getThreadMXBean()
                .getThreadInfo(new long[] {Thread.currentThread().getId()}, true, false)[0];
        return Arrays.stream(thread.getLockedMonitors())
                .map(MonitorInfo::getClassName)
                .toList();
    }

    /** Holds every timer back, so the test fires a poll timeout at the instant it needs. */
    private static final class ManualTimers extends DefaultEventLoop {
        private final List<Runnable> scheduled = new CopyOnWriteArrayList<>();
        private final List<Long> delaysMillis = new CopyOnWriteArrayList<>();

        void runLast() {
            scheduled.get(scheduled.size() - 1).run();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduled.add(command);
            delaysMillis.add(unit.toMillis(delay));
            return super.schedule(command, NEVER.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** Holds the deadline timers of lane admissions, which a lane schedules as callables, until the test runs them. */
    private static final class LaneTimers extends DefaultEventLoop {
        private final List<Callable<?>> scheduled = new CopyOnWriteArrayList<>();

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> command, long delay, TimeUnit unit) {
            scheduled.add(command);
            return super.schedule(command, NEVER.toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    /** Never dials: the router only reaches what the subclasses override. */
    private abstract static class OfflineConnection extends AsyncTcpConnection {
        // The node that the roster read on this connection names as metadata primary.
        int metadataLeaderPort = PRIMARY_PORT + 1;

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

        // A link of this test stays up until the test closes it.
        @Override
        boolean hasOpenChannel() {
            return true;
        }

        CompletableFuture<ByteBuf> roster() {
            return CompletableFuture.completedFuture(AsyncIggyTcpClientTransientFailoverTest.clusterMetadata(
                    PRIMARY_PORT + 1, PRIMARY_PORT, metadataLeaderPort));
        }
    }

    private static final class Coordinator extends OfflineConnection {
        private final EventLoop timers;
        private long watermark;
        private int routeView;
        // The node that a route read on this connection names as primary.
        private int routePort = PRIMARY_PORT;
        private PartitionContext context = ROUTE_CONTEXT;
        private int routes;
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
                return roster();
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
                            routePort,
                            context.incarnation().longValue())
                    .setIntLE(Long.BYTES * 2, routeView));
        }
    }

    /** Answers the first poll and leaves every later one in flight until it is closed. */
    private static final class Primary extends OfflineConnection {
        private final List<PartitionContext> sentContexts = new CopyOnWriteArrayList<>();
        private final List<byte[]> sentFrames = new CopyOnWriteArrayList<>();
        private final CompletableFuture<ByteBuf> unanswered = new CompletableFuture<>();
        private CompletableFuture<ByteBuf> routeReply = new CompletableFuture<>();
        // Outcomes for the next data sends, in order, before the default answers.
        private final Deque<CompletableFuture<ByteBuf>> replies = new ConcurrentLinkedDeque<>();
        private int routes;
        private CompletableFuture<ByteBuf> binding = CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
        private final AtomicInteger closes = new AtomicInteger();
        private boolean holdAfterFirst = true;
        private Runnable beforeSend = () -> {};
        // When set, a send waits for its lane on these timers, as a link admits a send before it writes it.
        private EventLoop laneTimers;

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
            if (commandCode == GET_CLUSTER_METADATA_CODE) {
                return roster();
            }
            if (commandCode == GET_PARTITION_ROUTE_CODE) {
                routes++;
                return routeReply;
            }
            if (laneTimers != null && commandCode == CommandCode.Messages.SEND.getValue()) {
                return sessionOwner()
                        .acquire(PARTITION_LANE, true, LANE_FRAME_BYTES, deadline, laneTimers)
                        .thenApply(ticket -> {
                            ticket.complete();
                            return Unpooled.EMPTY_BUFFER;
                        });
            }
            sentContexts.add(state.context());
            if (state.ticket() != null) {
                sentFrames.add(frozenFrame(state.ticket()));
            }
            beforeSend.run();
            return answer(commandCode);
        }

        private CompletableFuture<ByteBuf> answer(int commandCode) {
            CompletableFuture<ByteBuf> scripted = replies.poll();
            if (scripted != null) {
                return scripted;
            }
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

    /** Dials until the test ends, so it holds its dial permit. */
    private static final class StalledDial extends OfflineConnection {
        StalledDial(IoEventLoopGroup group) {
            super(group);
        }

        @Override
        public CompletableFuture<Void> connect() {
            return new CompletableFuture<>();
        }
    }
}
