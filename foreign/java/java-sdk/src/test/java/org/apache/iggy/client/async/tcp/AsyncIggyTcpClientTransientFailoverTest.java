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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientTransientFailoverTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 204;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 224;
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
            CompletableFuture<Void> server = serve(serverSocket, request -> {
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
        return CompletableFuture.runAsync(() -> {
            try (Socket socket = server.accept()) {
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream();
                Request request;
                while ((request = readRequest(input)) != null) {
                    writeResponse(output, handler.handle(request));
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
        return new Request(Byte.toUnsignedInt(header[REQUEST_OPERATION_OFFSET]), fields.getInt(REQUEST_CODE_OFFSET));
    }

    private static void writeResponse(OutputStream output, Response response) throws IOException {
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

    private record Request(int operation, int commandCode) {
        boolean is(int expectedCode, int expectedOperation) {
            return commandCode == expectedCode && operation == expectedOperation;
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
