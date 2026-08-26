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
import org.apache.iggy.exception.IggyServerException;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientTransientFailoverTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;
    private static final int EVICTION_REASON_OFFSET = 255;

    private static final int COMMAND_REPLY = 8;
    private static final int COMMAND_EVICTION = 13;
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int OPERATION_CREATE_STREAM = 128;
    private static final int GET_CLUSTER_METADATA_CODE = 12;
    private static final int CREATE_STREAM_CODE = 202;
    private static final int TRANSIENT_NOT_ACCEPTED = 58;
    private static final int EVICTION_STALE_CLIENT = 13;

    @Test
    void shouldRecheckLeaderAndReplayNotAcceptedMutation() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket oldLeaderSocket = new ServerSocket(0, 1, loopback);
                ServerSocket newLeaderSocket = new ServerSocket(0, 1, loopback)) {
            AtomicInteger denials = new AtomicInteger();
            AtomicInteger retriedMutations = new AtomicInteger();
            CompletableFuture<Void> oldLeader = serve(
                    oldLeaderSocket,
                    request -> handleOldLeader(
                            request, oldLeaderSocket.getLocalPort(), newLeaderSocket.getLocalPort(), denials));
            CompletableFuture<Void> newLeader = serve(
                    newLeaderSocket,
                    request -> handleNewLeader(
                            request, oldLeaderSocket.getLocalPort(), newLeaderSocket.getLocalPort(), retriedMutations));

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(oldLeaderSocket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(10))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                byte[] response = client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                        .get(10, TimeUnit.SECONDS);

                assertThat(response).isEmpty();
                assertThat(client.getConnectionInfo().port()).isEqualTo(newLeaderSocket.getLocalPort());
                assertThat(denials).hasValueGreaterThan(1);
                assertThat(retriedMutations).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            oldLeader.get(5, TimeUnit.SECONDS);
            newLeader.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReplayTransientImplicitLoginAfterEviction() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            AtomicInteger mutations = new AtomicInteger();
            CompletableFuture<Void> server = serve(serverSocket, 2, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    int attempt = registrations.incrementAndGet();
                    if (attempt == 2) {
                        return Response.success(OPERATION_REGISTER, transientResult(TRANSIENT_NOT_ACCEPTED));
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(attempt));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (mutations.incrementAndGet() == 1) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations).hasValue(3);
                assertThat(mutations).hasValue(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Closing is caller intent, like a logout. A sign-in still in flight when
     * it happens must not put its credentials back: `connect()` clears the
     * closed flag, so the next connection loss would replay a session the
     * caller had ended.
     */
    @Test
    void shouldNotRememberASignInThatLandedAfterClose() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            CompletableFuture<Void> server = serve(serverSocket, 4, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    int attempt = registrations.incrementAndGet();
                    if (attempt > 1) {
                        // The second sign-in is the one racing the close.
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(attempt));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();
            client.connect().get(5, TimeUnit.SECONDS);
            client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

            CompletableFuture<?> racing = client.users().login("iggy", "iggy");
            Thread.sleep(50);
            client.close().get(5, TimeUnit.SECONDS);
            racing.handle((ignored, error) -> null).get(5, TimeUnit.SECONDS);

            assertThat(client.hasRememberedLogin())
                    .as("a sign-in that landed after the close put its credentials back")
                    .isFalse();
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    /**
     * A stale-client eviction is not caller intent: the server's heartbeat
     * verifier sends it after a gc pause or a laptop sleep. A client that
     * signed in by hand recovers from it exactly like one whose credentials
     * were configured (the test above), and the sign-in it recovers with is the
     * one that last succeeded. Same rule in every SDK.
     */
    @Test
    void shouldReviveTheSignInAfterAStaleClientEviction() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            List<String> registeredLogins = new CopyOnWriteArrayList<>();
            AtomicBoolean evict = new AtomicBoolean(true);
            CompletableFuture<Void> server = serve(serverSocket, 6, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    registeredLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (evict.compareAndSet(true, false)) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            // No configured credentials: the only sign-in is the one run below.
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(2))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("handrun", "handrun").get(5, TimeUnit.SECONDS);
                int registrationsBeforeEviction = registrations.get();

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.hasRememberedLogin())
                        .as("an eviction is not a sign-out; the credentials stay")
                        .isTrue();

                // The next request brings the session back, under the sign-in
                // that last succeeded.
                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations.get())
                        .as("the evicted session was not re-established")
                        .isGreaterThan(registrationsBeforeEviction);
                assertThat(registeredLogins.get(registeredLogins.size() - 1)).contains("handrun");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    /**
     * Credentials on the builder and a hand-run sign-in for somebody else: the
     * revived session is the last sign-in, the same rule as on a redial and in
     * every other SDK. The connection re-authenticates a replacement channel
     * from the login it captured, which is that same sign-in, so replaying the
     * configured user here would make one eviction land on a different session
     * depending on which path got there first.
     */
    @Test
    void shouldReviveTheLastSignInRatherThanTheConfiguredOne() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            List<String> registeredLogins = new CopyOnWriteArrayList<>();
            AtomicBoolean evict = new AtomicBoolean(true);
            CompletableFuture<Void> server = serve(serverSocket, 6, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    registeredLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (evict.compareAndSet(true, false)) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .credentials("configured", "configured")
                    .requestTimeout(Duration.ofSeconds(2))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("handrun", "handrun").get(5, TimeUnit.SECONDS);
                int registrationsBeforeEviction = registrations.get();

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations.get())
                        .as("the evicted session was not re-established")
                        .isGreaterThan(registrationsBeforeEviction);
                assertThat(registeredLogins.get(registeredLogins.size() - 1))
                        .as("the revived session signed in as the configured user")
                        .contains("handrun")
                        .doesNotContain("configured");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    private static Response handleOldLeader(
            Request request, int oldLeaderPort, int newLeaderPort, AtomicInteger denials) {
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            boolean demoted = denials.get() > 0;
            return Response.success(
                    OPERATION_NON_REPLICATED,
                    clusterMetadata(oldLeaderPort, newLeaderPort, demoted ? newLeaderPort : oldLeaderPort));
        }
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(1));
        }
        if (request.operation() == OPERATION_CREATE_STREAM) {
            denials.incrementAndGet();
            return Response.error(OPERATION_CREATE_STREAM, TRANSIENT_NOT_ACCEPTED);
        }
        throw new IllegalStateException("Unexpected request to old leader: " + request);
    }

    private static Response handleNewLeader(
            Request request, int oldLeaderPort, int newLeaderPort, AtomicInteger retriedMutations) {
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(
                    OPERATION_NON_REPLICATED, clusterMetadata(oldLeaderPort, newLeaderPort, newLeaderPort));
        }
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(2));
        }
        if (request.operation() == OPERATION_CREATE_STREAM) {
            retriedMutations.incrementAndGet();
            ByteBuf body = Unpooled.buffer(Integer.BYTES);
            body.writeIntLE(0);
            return Response.success(OPERATION_CREATE_STREAM, body);
        }
        throw new IllegalStateException("Unexpected request to new leader: " + request);
    }

    private static CompletableFuture<Void> serve(ServerSocket server, RequestHandler handler) {
        return serve(server, 1, handler);
    }

    private static CompletableFuture<Void> serve(ServerSocket server, int connectionCount, RequestHandler handler) {
        return CompletableFuture.runAsync(() -> {
            try {
                for (int connection = 0; connection < connectionCount; connection++) {
                    try (Socket socket = server.accept()) {
                        InputStream input = socket.getInputStream();
                        OutputStream output = socket.getOutputStream();
                        Request request;
                        while ((request = readRequest(input)) != null) {
                            writeResponse(output, request, handler.handle(request));
                        }
                    }
                }
            } catch (IOException error) {
                throw new IllegalStateException("Mock VSR server failed", error);
            }
        });
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
                fields.getLong(REQUEST_ID_OFFSET),
                body);
    }

    private static void writeResponse(OutputStream output, Request request, Response response) throws IOException {
        byte[] body = new byte[response.body().readableBytes()];
        response.body().readBytes(body);
        response.body().release();
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE + body.length);
        header[COMMAND_OFFSET] = (byte) response.command();
        if (response.command() == COMMAND_EVICTION) {
            header[EVICTION_REASON_OFFSET] = (byte) response.evictionReason();
        } else {
            fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
            header[REPLY_OPERATION_OFFSET] = (byte) response.operation();
            fields.putInt(REPLY_STATUS_OFFSET, response.status());
        }
        output.write(header);
        output.write(body);
        output.flush();
    }

    private static ByteBuf registerBody(long session) {
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(1);
        body.writeLongLE(session);
        body.writeIntLE(11 << 10);
        body.writeByte(0);
        return body;
    }

    private static ByteBuf transientResult(int errorCode) {
        ByteBuf body = Unpooled.buffer(3 * Integer.BYTES);
        body.writeIntLE(1);
        body.writeIntLE(0);
        body.writeIntLE(errorCode);
        return body;
    }

    private static ByteBuf singleNodeMetadata(int port) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(1);
        writeNode(body, "node", port, true);
        return body;
    }

    private static ByteBuf clusterMetadata(int oldLeaderPort, int newLeaderPort, int leaderPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(2);
        writeNode(body, "old-node", oldLeaderPort, oldLeaderPort == leaderPort);
        writeNode(body, "new-node", newLeaderPort, newLeaderPort == leaderPort);
        return body;
    }

    private static void writeNode(ByteBuf body, String name, int port, boolean leader) {
        writeString(body, name);
        writeString(body, InetAddress.getLoopbackAddress().getHostAddress());
        body.writeShortLE(port);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeByte(leader ? 0 : 1);
        body.writeByte(0);
    }

    private static void writeString(ByteBuf body, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        body.writeIntLE(bytes.length);
        body.writeBytes(bytes);
    }

    private record Request(int operation, int commandCode, long requestId, byte[] body) {
        boolean is(int expectedCode, int expectedOperation) {
            return commandCode == expectedCode && operation == expectedOperation;
        }

        /** The request body as text, for asserting which user a login names. */
        String bodyAsText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private record Response(int command, int operation, int status, int evictionReason, ByteBuf body) {
        static Response success(int operation, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, body);
        }

        static Response error(int operation, int status) {
            return new Response(COMMAND_REPLY, operation, status, 0, Unpooled.EMPTY_BUFFER);
        }

        static Response eviction(int reason) {
            return new Response(COMMAND_EVICTION, 0, 0, reason, Unpooled.EMPTY_BUFFER);
        }
    }

    @FunctionalInterface
    private interface RequestHandler {
        Response handle(Request request);
    }
}
