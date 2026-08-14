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
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
    private static final int TRANSIENT_NOT_COMMITTED = 57;

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
                    Optional.of(Duration.ofSeconds(1)),
                    Optional.of(Duration.ofSeconds(2)),
                    Duration.ofHours(1),
                    1024 * 1024,
                    null,
                    () -> {},
                    ignored -> {});
            try {
                connection.connect().get(5, TimeUnit.SECONDS);
                connection
                        .send(LOGIN_CODE, loginPayload())
                        .get(5, TimeUnit.SECONDS)
                        .release();

                CompletableFuture<ByteBuf> first = connection.send(SEND_MESSAGES_CODE, sendMessagesPayload());
                firstRequestRead.get(5, TimeUnit.SECONDS);
                CompletableFuture<ByteBuf> second = connection.send(SEND_MESSAGES_CODE, sendMessagesPayload());

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

    private static AsyncTcpConnection newConnection(
            ServerSocket serverSocket, Duration heartbeatInterval, long acquireTimeoutMillis) {
        return new AsyncTcpConnection(
                serverSocket.getInetAddress().getHostAddress(),
                serverSocket.getLocalPort(),
                false,
                Optional.empty(),
                new AsyncTcpConnection.TcpConnectionPoolConfig(1000, acquireTimeoutMillis),
                Optional.of(Duration.ofSeconds(1)),
                Optional.of(Duration.ofSeconds(2)),
                heartbeatInterval,
                1024 * 1024,
                null,
                () -> {},
                ignored -> {});
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
                fields.getInt(REQUEST_CODE_OFFSET));
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
        output.write(header);
        output.write(payload);
        output.flush();
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
                .putInt(11 << 10)
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
        ByteBuf payload = Unpooled.buffer();
        payload.writeIntLE(22);
        writeNumericIdentifier(payload, 1);
        writeNumericIdentifier(payload, 2);
        payload.writeByte(2);
        payload.writeByte(4);
        payload.writeIntLE(3);
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

    private record Request(int operation, long requestId, int commandCode) {}
}
