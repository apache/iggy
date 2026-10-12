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
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_POLL_ROUTING_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.assertj.core.api.Assertions.assertThat;

class PollRouterTest {
    private static final long CLIENT_LOW = 1;
    private static final long CLIENT_HIGH = 2;
    private static final long SESSION = 3;
    private static final long ROUTE_WATERMARK = 1;
    private static final long INCARNATION = 7;
    private static final int PRIMARY_PORT = 8090;
    private static final int BIND_SECRET_BYTES = 32;
    private static final int PARTITION_KEY_BYTES = 16;
    private static final PartitionContext ROUTE_CONTEXT =
            new PartitionContext(BigInteger.valueOf(INCARNATION), BigInteger.ZERO, BigInteger.ZERO);
    // route() reads the watermark, then route(), pollOnConnection() and attach() check the
    // cached route, all before the route check that guards the send.
    private static final int SEND_CHECK_WATERMARK_READ = 5;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration NEVER = Duration.ofDays(1);

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

            CompletableFuture<Void> fire = new CompletableFuture<>();
            Thread timeout = start("poll-timeout", () -> {
                fire.join();
                timers.last().run();
            });
            coordinator.onWatermarkRead(SEND_CHECK_WATERMARK_READ, () -> {
                fire.complete(null);
                awaitBlocked(timeout);
            });
            CompletableFuture<CompletableFuture<ByteBuf>> polled = new CompletableFuture<>();
            Thread caller = start("poll-caller", () -> polled.complete(router.poll(pollPayload(), Optional.empty())));

            assertThat(polled)
                    .as(() -> "caller " + caller.getState() + ", timeout " + timeout.getState())
                    .succeedsWithin(TIMEOUT);
            // The poll reached the primary before the timeout ended it, so its outcome is unknown.
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

    private static ByteBuf pollPayload() {
        // The router keys routes by the bytes before the poll parameters and reads nothing else.
        return Unpooled.buffer().writeZero(PARTITION_KEY_BYTES + PollRouter.POLL_PARAMETERS_BYTES);
    }

    private static Thread start(String name, Runnable task) {
        Thread thread = new Thread(task, name);
        // A deadlocked thread must not keep the test JVM alive.
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void awaitBlocked(Thread thread) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (thread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    /** Holds every timer back, so the test fires a poll timeout at the instant it needs. */
    private static final class ManualTimers extends DefaultEventLoop {
        private final List<Runnable> scheduled = new CopyOnWriteArrayList<>();

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            scheduled.add(command);
            return super.schedule(command, NEVER.toNanos(), TimeUnit.NANOSECONDS);
        }

        Runnable last() {
            return scheduled.get(scheduled.size() - 1);
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
                    ignored -> {});
        }
    }

    private static final class Coordinator extends OfflineConnection {
        private final EventLoop timers;
        private final AtomicInteger watermarkReadsLeft = new AtomicInteger();
        private volatile Runnable watermarkHook = () -> {};

        Coordinator(IoEventLoopGroup group, EventLoop timers) {
            super(group);
            this.timers = timers;
        }

        /** Runs {@code hook} once, on the thread that makes the {@code reads}-th watermark read from now. */
        void onWatermarkRead(int reads, Runnable hook) {
            watermarkHook = hook;
            watermarkReadsLeft.set(reads);
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
            if (watermarkReadsLeft.decrementAndGet() == 0) {
                watermarkHook.run();
            }
            return super.metadataWatermark();
        }

        @Override
        byte[] bindSecret(long clientLow, long clientHigh, long epoch) {
            return new byte[BIND_SECRET_BYTES];
        }

        @Override
        public CompletableFuture<ByteBuf> send(CommandCode commandCode, ByteBuf payload) {
            payload.release();
            assertThat(commandCode).isEqualTo(CommandCode.Messages.GET_POLL_ROUTING);
            Request routing = new Request(
                    OPERATION_NON_REPLICATED,
                    GET_POLL_ROUTING_CODE,
                    0,
                    CLIENT_LOW,
                    CLIENT_HIGH,
                    0,
                    0,
                    0,
                    new byte[0],
                    0);
            return CompletableFuture.completedFuture(
                    pollRoute(routing, SESSION, ROUTE_WATERMARK, PRIMARY_PORT, INCARNATION));
        }
    }

    /** Answers the first poll and leaves every later one in flight until it is closed. */
    private static final class Primary extends OfflineConnection {
        private final List<PartitionContext> sentContexts = new CopyOnWriteArrayList<>();
        private final CompletableFuture<ByteBuf> unanswered = new CompletableFuture<>();

        Primary(IoEventLoopGroup group) {
            super(group);
        }

        @Override
        public CompletableFuture<Void> connect() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ByteBuf> send(CommandCode commandCode, ByteBuf payload) {
            payload.release();
            assertThat(commandCode).isEqualTo(CommandCode.System.BIND_SESSION);
            return CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
        }

        @Override
        CompletableFuture<ByteBuf> sendOnPrimary(
                int commandCode, ByteBuf payload, long sessionGeneration, PartitionContext context) {
            payload.release();
            sentContexts.add(context);
            return sentContexts.size() == 1 ? CompletableFuture.completedFuture(emptyPoll(0)) : unanswered;
        }

        @Override
        public CompletableFuture<Void> close() {
            // A closed channel fails what it still awaits on its own event loop, as channelInactive does.
            eventLoop()
                    .execute(() -> unanswered.completeExceptionally(
                            new IggyConnectionException("Connection closed before a response arrived")));
            return CompletableFuture.completedFuture(null);
        }
    }
}
