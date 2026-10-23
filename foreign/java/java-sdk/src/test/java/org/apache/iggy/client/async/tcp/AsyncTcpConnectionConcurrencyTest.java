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
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTcpConnectionConcurrencyTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_CLIENT_OFFSET = 128;
    private static final int CLIENT_ID_BYTES = 2 * Long.BYTES;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;

    private static final int COMMAND_REPLY = 8;
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int OPERATION_LOGOUT = 3;
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int PING_CODE = 1;
    private static final int LOGIN_CODE = 38;
    private static final int LOGOUT_CODE = 39;
    private static final int SEND_MESSAGES_CODE = 101;
    private static final int STORE_OFFSET_CODE = 121;
    private static final int TRANSIENT_NOT_COMMITTED = 57;
    private static final long OPERATION_ID = 9;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReleaseRetainedFramesOnlyWhenClosingTheirSessionOwner(boolean followsSession) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    Request register = readRequest(socket.getInputStream());
                    writeResponse(socket.getOutputStream(), register, registerBody());
                    assertThat(readRequest(socket.getInputStream()).operation()).isEqualTo(OPERATION_SEND_MESSAGES);
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 50);
            if (followsSession) {
                connection.followSession(new ConsensusSession());
            }
            ConsensusSession owner = connection.sessionOwner();
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                var state = new AsyncTcpConnection.TransientFailoverState();
                var request = connection.send(
                        SEND_MESSAGES_CODE,
                        sendMessagesPayload(),
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(2),
                        state);
                assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN));
                server.get(5, TimeUnit.SECONDS);

                ConsensusSession.Ticket ticket = state.ticket();
                ByteBuf alias = ticket.frame();
                ByteBuf allocation = alias;
                while (allocation.unwrap() != null) {
                    allocation = allocation.unwrap();
                }
                alias.release();
                ticket.finishWrite();
                assertThat(allocation.refCnt()).isPositive();

                connection.close().get(5, TimeUnit.SECONDS);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                if (followsSession) {
                    assertThat(owner.resume(ticket.operationId(), deadline, connection.eventLoop())
                                    .join())
                            .isSameAs(ticket);
                    assertThat(allocation.refCnt()).isPositive();
                    owner.abandon(ticket.operationId());
                    assertThat(allocation.refCnt()).isPositive();
                    ticket.retain();
                }
                assertThat(allocation.refCnt()).isZero();
                assertThatThrownBy(() -> owner.resume(ticket.operationId(), deadline, connection.eventLoop())
                                .join())
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
                owner.reset();
            }
        }
    }

    @Test
    void shouldKeepAReplayRetainedWhenItsResolveIsCancelledAfterTheReplyIsMarkedCommitted() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> replayed = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(5));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    Request register = readRequest(input);
                    writeResponse(output, register, registerBody());
                    readRequest(input);
                    Request replay = readRequest(input);
                    replayed.complete(null);
                    release.join();
                    writeResponse(output, replay, new byte[0]);
                    writeResponse(output, readRequest(input), new byte[0]);
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 50);
            ConsensusSession owner = connection.sessionOwner();
            PollRouter router = new PollRouter(() -> connection, endpoint -> {
                throw new IllegalStateException("A metadata replay opens no partition link");
            });
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                var state = new AsyncTcpConnection.TransientFailoverState();
                var request = connection.send(
                        SEND_MESSAGES_CODE,
                        sendMessagesPayload(),
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300),
                        state);
                assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class);
                long operation = state.ticket().operationId();
                var cancelled = router.resolve(operation, Duration.ofSeconds(5));
                replayed.get(5, TimeUnit.SECONDS);
                // After the write of the replay ends, only its reply makes the event loop wait for the session.
                connection.eventLoop().submit(() -> {}).get(5, TimeUnit.SECONDS);
                Thread eventLoop =
                        connection.eventLoop().submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
                synchronized (owner) {
                    release.complete(null);
                    // The decoder marks the reply committed before it waits for the session.
                    awaitBlocked(eventLoop);
                    cancelled.cancel(false);
                }

                router.resolve(operation, Duration.ofSeconds(5))
                        .get(5, TimeUnit.SECONDS)
                        .release();
                server.get(5, TimeUnit.SECONDS);
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
                owner.reset();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReplayWaitingResolveAfterThePreviousReplayTimesOutOrIsCancelled(boolean cancel) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> replayReceived = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(5));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    writeResponse(output, readRequest(input), registerBody());
                    Request original = readRequest(input);
                    Request replay = readRequest(input);
                    assertThat(replay.requestId()).isEqualTo(original.requestId());
                    assertThat(replay.header()).containsExactly(original.header());
                    replayReceived.complete(null);
                    Request waitingReplay = readRequest(input);
                    assertThat(waitingReplay.requestId()).isEqualTo(original.requestId());
                    assertThat(waitingReplay.header()).containsExactly(original.header());
                    writeResponse(output, waitingReplay, new byte[0]);
                } catch (IOException error) {
                    replayReceived.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 50);
            PollRouter router = new PollRouter(() -> connection, endpoint -> {
                throw new IllegalStateException("A direct replay opens no partition link");
            });
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                var state = new AsyncTcpConnection.TransientFailoverState();
                var original = connection.send(
                        SEND_MESSAGES_CODE,
                        sendMessagesPayload(),
                        System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300),
                        state);
                assertThatThrownBy(() -> original.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class);
                long operation = state.ticket().operationId();
                var replay = router.resolve(operation, cancel ? Duration.ofSeconds(5) : Duration.ofSeconds(1));
                replayReceived.get(5, TimeUnit.SECONDS);
                var waiting = router.resolve(operation, Duration.ofSeconds(5));
                assertThat(waiting).isNotDone();

                if (cancel) {
                    assertThat(replay.cancel(false)).isTrue();
                } else {
                    assertThatThrownBy(() -> replay.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(IggyOperationException.class);
                }

                waiting.get(5, TimeUnit.SECONDS).release();
                router.resolve(operation, Duration.ofSeconds(1))
                        .get(5, TimeUnit.SECONDS)
                        .release();
                server.get(5, TimeUnit.SECONDS);
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldReleaseTheFrameAliasWhenTheRequestIdCannotBeRenewed() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    Request register = readRequest(input);
                    writeResponse(socket.getOutputStream(), register, registerBody());
                    assertThat(input.read()).isEqualTo(-1);
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 50);
            ConsensusSession owner = connection.sessionOwner();
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                int frameBytes = HEADER_SIZE + Long.BYTES;
                var lane = new ConsensusSession.LaneKey(1, 0);
                owner.configurePendingOperations(1, frameBytes);
                ConsensusSession.Ticket ticket = owner.acquire(
                                lane,
                                true,
                                frameBytes,
                                System.nanoTime() + TimeUnit.SECONDS.toNanos(2),
                                connection.eventLoop())
                        .get(5, TimeUnit.SECONDS);
                ByteBuf frame = Unpooled.buffer(frameBytes).writeZero(frameBytes);
                frame.setLongLE(REQUEST_ID_OFFSET, OPERATION_ID);
                ticket.freeze(frame, STORE_OFFSET_CODE, new byte[0]);
                frame.release();
                ticket.finishWrite();
                ticket.startWrite();
                ticket.finishWrite();
                ticket.retain();

                var state = new AsyncTcpConnection.TransientFailoverState();
                state.restore(ticket);
                state.renewRequestId();
                var replay = connection.sendRouted(
                        STORE_OFFSET_CODE,
                        Unpooled.buffer(),
                        0,
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(2),
                        state);
                assertThatThrownBy(() -> replay.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));

                owner.abandon(OPERATION_ID);
                assertThat(frame.refCnt()).isZero();
                owner.acquire(
                                lane,
                                true,
                                frameBytes,
                                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200),
                                connection.eventLoop())
                        .get(5, TimeUnit.SECONDS)
                        .complete();
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
                owner.reset();
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldFlushRequestFromReplyCallbackWithoutFurtherInboundTraffic() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    Request register = readRequest(input);
                    writeResponse(output, register, registerBody());
                    Request first = readRequest(input);
                    writeResponse(output, first, new byte[0]);
                    Request next = readRequest(input);
                    assertThat(next.requestId()).isNotEqualTo(first.requestId());
                    writeResponse(output, next, "done".getBytes(StandardCharsets.UTF_8));
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = new AsyncTcpConnection(
                    loopback.getHostAddress(),
                    serverSocket.getLocalPort(),
                    false,
                    Optional.empty(),
                    new AsyncTcpConnection.TcpConnectionPoolConfig(1000, 50),
                    Optional.empty(),
                    1,
                    Optional.of(Duration.ofSeconds(1)),
                    Optional.of(Duration.ofSeconds(2)),
                    Duration.ofHours(1),
                    1024 * 1024,
                    null,
                    errorCode -> {},
                    (source, cause) -> {});
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                var sent = connection
                        .send(SEND_MESSAGES_CODE, sendMessagesPayload())
                        .thenCompose(response -> {
                            response.release();
                            return connection.send(SEND_MESSAGES_CODE, sendMessagesPayload());
                        });
                assertResponse(sent, "done");
                server.get(5, TimeUnit.SECONDS);
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldCorrelateConcurrentPartitionResponsesInReverseOrder() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            CompletableFuture<Void> firstRequestRead = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();

                    Request register = readRequest(input);
                    assertThat(register.operation()).isEqualTo(OPERATION_REGISTER);
                    writeResponse(output, register, registerBody());

                    Request firstRequest = readRequest(input);
                    assertThat(firstRequest.operation()).isEqualTo(OPERATION_SEND_MESSAGES);
                    firstRequestRead.complete(null);
                    Request secondRequest = readRequest(input);
                    assertThat(secondRequest.operation()).isEqualTo(OPERATION_SEND_MESSAGES);
                    assertThat(secondRequest.requestId()).isNotEqualTo(firstRequest.requestId());

                    Thread.sleep(200);
                    writeResponse(output, secondRequest, "second".getBytes(StandardCharsets.UTF_8));
                    writeResponse(output, firstRequest, "first".getBytes(StandardCharsets.UTF_8));
                } catch (IOException error) {
                    firstRequestRead.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server failed", error);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Mock VSR server interrupted", error);
                }
            });

            AsyncTcpConnection connection = new AsyncTcpConnection(
                    loopback.getHostAddress(),
                    serverSocket.getLocalPort(),
                    false,
                    Optional.empty(),
                    new AsyncTcpConnection.TcpConnectionPoolConfig(1000, 50),
                    Optional.empty(),
                    1,
                    Optional.of(Duration.ofSeconds(1)),
                    Optional.of(Duration.ofSeconds(2)),
                    Duration.ofHours(1),
                    1024 * 1024,
                    null,
                    errorCode -> {},
                    (source, cause) -> {});
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();

                var firstState = new AsyncTcpConnection.TransientFailoverState(
                        new PartitionContext(BigInteger.valueOf(7), BigInteger.ZERO, BigInteger.ONE));
                firstState.setLane(new ConsensusSession.LaneKey(7, 3));
                var secondState = new AsyncTcpConnection.TransientFailoverState(
                        new PartitionContext(BigInteger.valueOf(8), BigInteger.ZERO, BigInteger.ONE));
                secondState.setLane(new ConsensusSession.LaneKey(8, 4));
                CompletableFuture<ByteBuf> first = connection.sendRouted(
                        SEND_MESSAGES_CODE,
                        sendMessagesPayload(3),
                        0,
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(2),
                        firstState);
                firstRequestRead.get(5, TimeUnit.SECONDS);
                CompletableFuture<ByteBuf> second = connection.sendRouted(
                        SEND_MESSAGES_CODE,
                        sendMessagesPayload(4),
                        0,
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(2),
                        secondState);

                assertResponse(first, "first");
                assertResponse(second, "second");
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldNotStarveHeartbeatWhileApplicationResponseIsPending() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            CompletableFuture<Void> applicationRequestRead = new CompletableFuture<>();
            CompletableFuture<Void> heartbeatRead = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();

                    Request register = readRequest(input);
                    writeResponse(output, register, registerBody());

                    Request application = readRequest(input);
                    assertThat(application.operation()).isEqualTo(OPERATION_SEND_MESSAGES);
                    applicationRequestRead.complete(null);

                    Request heartbeat = readRequest(input);
                    assertThat(heartbeat.operation()).isEqualTo(OPERATION_NON_REPLICATED);
                    assertThat(heartbeat.commandCode()).isEqualTo(PING_CODE);
                    heartbeatRead.complete(null);
                    writeResponse(output, heartbeat, new byte[0]);
                    writeResponse(output, application, "application".getBytes(StandardCharsets.UTF_8));
                } catch (IOException error) {
                    applicationRequestRead.completeExceptionally(error);
                    heartbeatRead.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });

            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofMillis(300), 50);
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();

                CompletableFuture<ByteBuf> application = connection.send(SEND_MESSAGES_CODE, sendMessagesPayload());
                applicationRequestRead.get(5, TimeUnit.SECONDS);
                heartbeatRead.get(5, TimeUnit.SECONDS);
                assertResponse(application, "application");
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReuseCorrelationIdForTransientReplay() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();

                    Request register = readRequest(input);
                    writeResponse(output, register, registerBody());

                    Request firstAttempt = readRequest(input);
                    writeResponse(output, firstAttempt, TRANSIENT_NOT_COMMITTED, new byte[0]);
                    Request secondAttempt = readRequest(input);
                    assertThat(secondAttempt.operation()).isEqualTo(firstAttempt.operation());
                    assertThat(secondAttempt.requestId()).isEqualTo(firstAttempt.requestId());
                    writeResponse(output, secondAttempt, "retried".getBytes(StandardCharsets.UTF_8));
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });

            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 50);
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();

                assertResponse(connection.send(SEND_MESSAGES_CODE, sendMessagesPayload()), "retried");
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldSerializeRegisterAndLogoutThroughTheirResponses() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            CompletableFuture<Void> registerRead = new CompletableFuture<>();
            CompletableFuture<Void> registerConcurrentSendStarted = new CompletableFuture<>();
            CompletableFuture<Void> logoutRead = new CompletableFuture<>();
            CompletableFuture<Void> logoutConcurrentSendStarted = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();

                    Request register = readRequest(input);
                    registerRead.complete(null);
                    registerConcurrentSendStarted.get(2, TimeUnit.SECONDS);
                    assertNoRequest(input, socket);
                    writeResponse(output, register, registerBody());

                    Request pingDuringRegister = readRequest(input);
                    assertThat(pingDuringRegister.commandCode()).isEqualTo(PING_CODE);
                    writeResponse(output, pingDuringRegister, new byte[0]);

                    Request logout = readRequest(input);
                    assertThat(logout.operation()).isEqualTo(OPERATION_LOGOUT);
                    logoutRead.complete(null);
                    logoutConcurrentSendStarted.get(2, TimeUnit.SECONDS);
                    assertNoRequest(input, socket);
                    writeResponse(output, logout, new byte[0]);

                    Request pingDuringLogout = readRequest(input);
                    assertThat(pingDuringLogout.commandCode()).isEqualTo(PING_CODE);
                    writeResponse(output, pingDuringLogout, new byte[0]);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    registerRead.completeExceptionally(error);
                    logoutRead.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server interrupted", error);
                } catch (IOException | ExecutionException | TimeoutException error) {
                    registerRead.completeExceptionally(error);
                    logoutRead.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });

            AsyncTcpConnection connection = newConnection(serverSocket, Duration.ofHours(1), 500);
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                CompletableFuture<ByteBuf> login = connection.send(LOGIN_CODE, loginPayload());
                registerRead.get(5, TimeUnit.SECONDS);
                // Ping needs no session, so the probe waits only for the lease
                // and never for lazy authentication.
                CompletableFuture<ByteBuf> pingDuringRegister = connection.send(PING_CODE, Unpooled.EMPTY_BUFFER);
                registerConcurrentSendStarted.complete(null);
                login.get(5, TimeUnit.SECONDS).release();
                pingDuringRegister.get(5, TimeUnit.SECONDS).release();

                CompletableFuture<ByteBuf> logout = connection.send(LOGOUT_CODE, Unpooled.EMPTY_BUFFER);
                logoutRead.get(5, TimeUnit.SECONDS);
                CompletableFuture<ByteBuf> pingDuringLogout = connection.send(PING_CODE, Unpooled.EMPTY_BUFFER);
                logoutConcurrentSendStarted.complete(null);
                logout.get(5, TimeUnit.SECONDS).release();
                pingDuringLogout.get(5, TimeUnit.SECONDS).release();
            } finally {
                connection.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldKeepATicketlessReadOnTheIdentityItStartedEncoding() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> observed = new CompletableFuture<>();
            CompletableFuture<Request> registered = new CompletableFuture<>();
            CompletableFuture<Void> releaseReply = new CompletableFuture<>();
            var serving = CompletableFuture.runAsync(() -> {
                try (Socket peer = socket.accept()) {
                    Request register = readRequest(peer.getInputStream());
                    registered.complete(register);
                    writeResponse(peer.getOutputStream(), register, registerBody());
                    Request read = readRequest(peer.getInputStream());
                    observed.complete(read);
                    releaseReply.join();
                    writeResponse(peer.getOutputStream(), read, new byte[0]);
                } catch (IOException error) {
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });
            AsyncTcpConnection connection = newConnection(socket, Duration.ofHours(1), 50);
            HeldPayload payload = new HeldPayload();
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();
                var read = connection.send(CommandCode.System.GET_STATS, payload);
                payload.encoding.get(5, TimeUnit.SECONDS);
                CompletableFuture<Void> changed = new CompletableFuture<>();
                Thread caller = new Thread(
                        () -> {
                            connection.sessionOwner().endLineage();
                            connection.sessionOwner().reset();
                            changed.complete(null);
                        },
                        "ticketless-caller-change");
                caller.setDaemon(true);
                caller.start();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!changed.isDone() && caller.getState() != Thread.State.BLOCKED) {
                    assertThat(deadline - System.nanoTime()).isPositive();
                    TimeUnit.MILLISECONDS.sleep(1);
                }
                payload.releaseEncoding.complete(null);
                changed.get(5, TimeUnit.SECONDS);
                releaseReply.complete(null);
                assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class)
                        .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                                .isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode()));
                Request request = observed.get(5, TimeUnit.SECONDS);
                Request register = registered.get(5, TimeUnit.SECONDS);
                assertThat(Arrays.copyOfRange(
                                request.header(), REQUEST_CLIENT_OFFSET, REQUEST_CLIENT_OFFSET + CLIENT_ID_BYTES))
                        .as("the read's client id before the caller replaced it")
                        .containsExactly(Arrays.copyOfRange(
                                register.header(), REQUEST_CLIENT_OFFSET, REQUEST_CLIENT_OFFSET + CLIENT_ID_BYTES));
                serving.get(5, TimeUnit.SECONDS);
            } finally {
                releaseReply.complete(null);
                payload.releaseEncoding.complete(null);
                connection.close().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class HeldPayload extends UnpooledHeapByteBuf {
        private final CompletableFuture<Void> encoding = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseEncoding = new CompletableFuture<>();

        private HeldPayload() {
            super(UnpooledByteBufAllocator.DEFAULT, 0, 0);
        }

        @Override
        public int readableBytes() {
            encoding.complete(null);
            releaseEncoding.join();
            return super.readableBytes();
        }
    }

    private static AsyncTcpConnection newConnection(
            ServerSocket serverSocket, Duration heartbeatInterval, long acquireTimeoutMillis) {
        return new AsyncTcpConnection(
                serverSocket.getInetAddress().getHostAddress(),
                serverSocket.getLocalPort(),
                false,
                Optional.empty(),
                new AsyncTcpConnection.TcpConnectionPoolConfig(1000, acquireTimeoutMillis),
                Optional.empty(),
                1,
                Optional.of(Duration.ofSeconds(1)),
                Optional.of(Duration.ofSeconds(2)),
                heartbeatInterval,
                1024 * 1024,
                null,
                errorCode -> {},
                (source, cause) -> {});
    }

    private static Request readRequest(InputStream input) throws IOException {
        byte[] header = input.readNBytes(HEADER_SIZE);
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
                fields.getLong(REQUEST_ID_OFFSET),
                fields.getInt(REQUEST_CODE_OFFSET),
                header);
    }

    private static void writeResponse(OutputStream output, Request request, byte[] payload) throws IOException {
        writeResponse(output, request, 0, payload);
    }

    private static void writeResponse(OutputStream output, Request request, int status, byte[] payload)
            throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE + payload.length);
        fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
        fields.putInt(REPLY_STATUS_OFFSET, status);
        header[COMMAND_OFFSET] = COMMAND_REPLY;
        header[REPLY_OPERATION_OFFSET] = (byte) request.operation();
        if (status == 0 && request.operation() != OPERATION_NON_REPLICATED) {
            header[220] = 1;
            System.arraycopy(request.header(), 144, header, 128, 16);
            System.arraycopy(request.header(), 128, header, 160, 16);
            System.arraycopy(request.header(), 200, header, 224, Long.BYTES);
            fields.putLong(176, 1);
            fields.putLong(184, 1);
        }
        output.write(header);
        output.write(payload);
        output.flush();
    }

    private static void awaitBlocked(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED) {
            assertThat(deadline - System.nanoTime())
                    .as("%s waits for a lock", thread.getName())
                    .isPositive();
            TimeUnit.MILLISECONDS.sleep(1);
        }
    }

    private static void assertNoRequest(InputStream input, Socket socket) throws IOException {
        socket.setSoTimeout(200);
        assertThatThrownBy(input::read).isInstanceOf(SocketTimeoutException.class);
        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
    }

    private static byte[] registerBody() {
        return ByteBuffer.allocate(21)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(0)
                .putInt(1)
                .putLong(42)
                .putInt((11 << 10) | 1)
                .put((byte) 0)
                .array();
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

    private static ByteBuf sendMessagesPayload() {
        return sendMessagesPayload(3);
    }

    private static ByteBuf sendMessagesPayload(int partition) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeIntLE(22);
        writeNumericIdentifier(payload, 1);
        writeNumericIdentifier(payload, 2);
        payload.writeByte(2);
        payload.writeByte(4);
        payload.writeIntLE(partition);
        payload.writeIntLE(0);
        return payload;
    }

    private static void writeNumericIdentifier(ByteBuf payload, int id) {
        payload.writeByte(1);
        payload.writeByte(4);
        payload.writeIntLE(id);
    }

    private static void assertResponse(CompletableFuture<ByteBuf> responseFuture, String expected) throws Exception {
        ByteBuf response = responseFuture.get(5, TimeUnit.SECONDS);
        try {
            byte[] body = new byte[response.readableBytes()];
            response.readBytes(body);
            assertThat(body).isEqualTo(expected.getBytes(StandardCharsets.UTF_8));
        } finally {
            response.release();
        }
    }

    private record Request(int operation, long requestId, int commandCode, byte[] header) {}
}
