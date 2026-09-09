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
import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.apache.iggy.client.ConnectionInfo;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * One client drives one channel, so the group that it creates for itself runs one loop
 * and shuts down with the client. If the caller supplies a group, the client pins its
 * channel to one loop of that group and never shuts the group down.
 */
class AsyncIggyTcpClientEventLoopGroupTest {

    private static final String OWNED_THREAD_PREFIX = "iggy-tcp-io-";
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofMillis(25);
    // Graceful shutdown waits a two-second quiet period before the loop thread exits.
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(15);

    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;
    private static final int COMMAND_REPLY = 8;
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int PING_CODE = 1;
    private static final int LOGIN_CODE = 38;

    @Test
    void shouldRunOneEventLoopThreadPerClientByDefault() throws Exception {
        try (MockVsrServer server = MockVsrServer.start()) {
            Set<Thread> before = ownedEventLoopThreads();
            AsyncIggyTcpClient client = builder(server).build();
            client.connect().get(5, TimeUnit.SECONDS);

            // Enough ticks for a round-robin heartbeat to wake more than one loop.
            server.awaitPings(8, Duration.ofSeconds(5));
            assertThat(ownedEventLoopThreadsSince(before))
                    .as("one channel needs one loop")
                    .isEqualTo(1);

            client.close().get(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            awaitOwnedEventLoopThreadsSince(before, 0, SHUTDOWN_TIMEOUT);
        }
    }

    @Test
    void shouldLeaveASharedGroupRunningWhenClientsClose() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (MockVsrServer server = MockVsrServer.start()) {
            Set<Thread> before = ownedEventLoopThreads();
            AsyncIggyTcpClient first = builder(server).eventLoopGroup(group).build();
            AsyncIggyTcpClient second = builder(server).eventLoopGroup(group).build();
            first.connect().get(5, TimeUnit.SECONDS);
            second.connect().get(5, TimeUnit.SECONDS);
            assertThat(ownedEventLoopThreadsSince(before))
                    .as("clients on a shared group create no group of their own")
                    .isZero();

            first.close().get(5, TimeUnit.SECONDS);
            assertThat(group.isShuttingDown()).isFalse();
            second.sendBinaryRequest(PING_CODE, new byte[0]).get(5, TimeUnit.SECONDS);

            second.close().get(5, TimeUnit.SECONDS);
            assertThat(group.isShuttingDown()).isFalse();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldPinEachConnectionOnASharedGroupToTheNextLoop() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
        List<EventLoop> loops = new ArrayList<>();
        group.forEach(executor -> loops.add((EventLoop) executor));
        List<AsyncTcpConnection> connections = new ArrayList<>();
        try (MockVsrServer server = MockVsrServer.start()) {
            for (int i = 0; i < loops.size(); i++) {
                AsyncTcpConnection connection = connection(server, group);
                connection.connect().get(5, TimeUnit.SECONDS);
                connections.add(connection);
            }
            server.awaitPings(loops.size() * 2, Duration.ofSeconds(5));

            // A fresh group hands its loops out round-robin from the first. Every
            // extra next() call per connection, such as a pool handed the whole
            // group, skips loops and lands later connections off this sequence.
            assertThat(connections.stream().map(AsyncTcpConnection::eventLoop).toList())
                    .as("one connection takes one slot of the group's round-robin chooser")
                    .containsExactlyElementsOf(loops);
        } finally {
            for (AsyncTcpConnection connection : connections) {
                connection.close().get(5, TimeUnit.SECONDS);
            }
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldCloseQuietlyAfterTheCallerShutTheSharedGroupDown() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (MockVsrServer server = MockVsrServer.start()) {
            AsyncIggyTcpClient client = builder(server).eventLoopGroup(group).build();
            client.connect().get(5, TimeUnit.SECONDS);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);

            // The pool closes on its loop, and a terminated loop rejects that task.
            assertThatCode(() -> client.close().get(5, TimeUnit.SECONDS))
                    .as("nothing is left to release once the group took the channel down")
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void shouldCloseTheChannelHeldByAPendingLoginOnASharedGroup() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (MockVsrServer server = MockVsrServer.start()) {
            server.withholdRegisterReplies();
            // Far past the test bound, so only close() can settle the login.
            AsyncTcpConnection connection = connection(server, group, Duration.ofMinutes(5));
            connection.connect().get(5, TimeUnit.SECONDS);
            CompletableFuture<ByteBuf> login = connection.send(LOGIN_CODE, loginPayload());
            await(() -> server.registers() == 1, "the server holds the login", Duration.ofSeconds(5));

            connection.close().get(5, TimeUnit.SECONDS);

            // The pool only closes idle channels, and a login holds its lease
            // until the reply, so close() must reach that channel itself.
            await(() -> server.closedSockets() == 1, "the login's socket is closed", Duration.ofSeconds(5));
            await(login::isDone, "the pending login settles", Duration.ofSeconds(5));
            assertThat(login).isCompletedExceptionally();
            assertThat(group.isShuttingDown()).isFalse();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldCancelThePendingHeartbeatOnASharedGroupWhenTheConnectionCloses() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (MockVsrServer server = MockVsrServer.start()) {
            AsyncTcpConnection connection = connection(server, group);
            connection.connect().get(5, TimeUnit.SECONDS);
            server.awaitPings(2, Duration.ofSeconds(5));
            await(connection::heartbeatScheduled, "a live connection keeps a tick armed", Duration.ofSeconds(5));

            connection.close().get(5, TimeUnit.SECONDS);

            // A stray timer on a shared loop cannot be seen through the server: the
            // channel is gone, so a tick that still fired would fail before sending.
            assertThat(connection.heartbeatScheduled())
                    .as("close cancels the armed tick instead of leaving it to the shared loop")
                    .isFalse();
            Thread.sleep(HEARTBEAT_INTERVAL.multipliedBy(4).toMillis());
            assertThat(connection.heartbeatScheduled())
                    .as("nothing re-arms the heartbeat after close")
                    .isFalse();
            assertThat(group.isShuttingDown()).isFalse();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReleaseTheGroupOfAConnectionReplacedByRetarget() throws Exception {
        try (MockVsrServer primary = MockVsrServer.start();
                MockVsrServer survivor = MockVsrServer.start()) {
            Set<Thread> before = ownedEventLoopThreads();
            AsyncIggyTcpClient client = builder(primary).build();
            client.connect().get(5, TimeUnit.SECONDS);

            client.retarget(new ConnectionInfo(loopback(), survivor.port())).get(5, TimeUnit.SECONDS);
            assertThat(client.getConnectionInfo().port()).isEqualTo(survivor.port());

            awaitOwnedEventLoopThreadsSince(before, 1, SHUTDOWN_TIMEOUT);
            client.close().get(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            awaitOwnedEventLoopThreadsSince(before, 0, SHUTDOWN_TIMEOUT);
        }
    }

    private static AsyncIggyTcpClientBuilder builder(MockVsrServer server) {
        return AsyncIggyTcpClient.builder()
                .host(loopback())
                .port(server.port())
                .heartbeatInterval(HEARTBEAT_INTERVAL)
                .requestTimeout(Duration.ofSeconds(1));
    }

    private static AsyncTcpConnection connection(MockVsrServer server, IoEventLoopGroup group) {
        return connection(server, group, Duration.ofSeconds(1));
    }

    private static AsyncTcpConnection connection(
            MockVsrServer server, IoEventLoopGroup group, Duration requestTimeout) {
        return new AsyncTcpConnection(
                loopback(),
                server.port(),
                false,
                Optional.empty(),
                new AsyncTcpConnection.TcpConnectionPoolConfig(1000, 3000),
                Optional.of(group),
                1,
                Optional.of(Duration.ofSeconds(1)),
                Optional.of(requestTimeout),
                HEARTBEAT_INTERVAL,
                1024 * 1024,
                null,
                errorCode -> {},
                ignored -> {});
    }

    private static String loopback() {
        return InetAddress.getLoopbackAddress().getHostAddress();
    }

    private static ByteBuf loginPayload() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeIntLE(0);
        payload.writeIntLE(0);
        return payload;
    }

    private static Set<Thread> ownedEventLoopThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith(OWNED_THREAD_PREFIX))
                .collect(Collectors.toSet());
    }

    // Loops that earlier tests left draining die on their own schedule, so
    // only threads born after the snapshot count.
    private static int ownedEventLoopThreadsSince(Set<Thread> before) {
        return (int) ownedEventLoopThreads().stream()
                .filter(thread -> !before.contains(thread))
                .count();
    }

    private static void awaitOwnedEventLoopThreadsSince(Set<Thread> before, int expected, Duration timeout)
            throws Exception {
        await(
                () -> ownedEventLoopThreadsSince(before) == expected,
                "Expected " + expected + " owned event loop threads started by this test",
                timeout);
    }

    private static void await(BooleanSupplier condition, String description, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new TimeoutException(description + " within " + timeout);
            }
            Thread.sleep(20);
        }
    }

    /**
     * Replies success to every request and counts the pings that it saw across all sockets.
     * Register replies can be withheld to keep a login pending on the client.
     */
    private static final class MockVsrServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final List<Socket> accepted = new CopyOnWriteArrayList<>();
        private final AtomicInteger pings = new AtomicInteger();
        private final AtomicInteger registers = new AtomicInteger();
        private final AtomicInteger closedSockets = new AtomicInteger();
        private volatile boolean withholdRegisterReplies;
        private volatile boolean closed;

        private MockVsrServer(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        static MockVsrServer start() throws IOException {
            MockVsrServer server = new MockVsrServer(new ServerSocket(0, 4, InetAddress.getLoopbackAddress()));
            Thread acceptor = new Thread(server::acceptLoop, "mock-vsr-acceptor-" + server.port());
            acceptor.setDaemon(true);
            acceptor.start();
            return server;
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        int pings() {
            return pings.get();
        }

        int registers() {
            return registers.get();
        }

        int closedSockets() {
            return closedSockets.get();
        }

        void withholdRegisterReplies() {
            withholdRegisterReplies = true;
        }

        void awaitPings(int expected, Duration timeout) throws Exception {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (pings.get() < expected) {
                if (System.nanoTime() > deadline) {
                    throw new TimeoutException("Expected " + expected + " pings, saw " + pings.get());
                }
                Thread.sleep(5);
            }
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    accepted.add(socket);
                    Thread exchange = new Thread(() -> exchange(socket), "mock-vsr-exchange-" + port());
                    exchange.setDaemon(true);
                    exchange.start();
                } catch (IOException stopped) {
                    return;
                }
            }
        }

        private void exchange(Socket socket) {
            try (socket) {
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream();
                Request request;
                while (!closed && (request = readRequest(input)) != null) {
                    if (request.operation() == OPERATION_NON_REPLICATED && request.commandCode() == PING_CODE) {
                        pings.incrementAndGet();
                    }
                    if (request.operation() == OPERATION_REGISTER) {
                        registers.incrementAndGet();
                        if (withholdRegisterReplies) {
                            continue;
                        }
                    }
                    byte[] body = request.operation() == OPERATION_REGISTER ? registerBody() : new byte[0];
                    writeResponse(output, request, body);
                }
            } catch (IOException clientWentAway) {
                // A closed client and a closed server look the same here.
            } finally {
                closedSockets.incrementAndGet();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            for (Socket socket : accepted) {
                socket.close();
            }
            serverSocket.close();
        }
    }

    private static Request readRequest(InputStream input) throws IOException {
        byte[] header = input.readNBytes(HEADER_SIZE);
        if (header.length == 0) {
            return null;
        }
        if (header.length != HEADER_SIZE) {
            throw new EOFException("Truncated VSR request header");
        }
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        int size = fields.getInt(SIZE_OFFSET);
        byte[] body = input.readNBytes(size - HEADER_SIZE);
        if (body.length != size - HEADER_SIZE) {
            throw new EOFException("Truncated VSR request body");
        }
        return new Request(
                Byte.toUnsignedInt(header[REQUEST_OPERATION_OFFSET]),
                fields.getInt(REQUEST_CODE_OFFSET),
                fields.getLong(REQUEST_ID_OFFSET));
    }

    private static void writeResponse(OutputStream output, Request request, byte[] body) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE + body.length);
        header[COMMAND_OFFSET] = COMMAND_REPLY;
        fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
        header[REPLY_OPERATION_OFFSET] = (byte) request.operation();
        fields.putInt(REPLY_STATUS_OFFSET, 0);
        output.write(header);
        output.write(body);
        output.flush();
    }

    private static byte[] registerBody() {
        return ByteBuffer.allocate(21)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(0)
                .putInt(1)
                .putLong(42)
                .putInt(11 << 10)
                .put((byte) 0)
                .array();
    }

    private record Request(int operation, int commandCode, long requestId) {}
}
