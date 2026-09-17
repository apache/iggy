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
import java.util.concurrent.CompletableFuture;

/**
 * A loopback server speaking the VSR frame format, for tests that need to steer
 * a real client through routing, attachment and poll exchanges.
 *
 * <p>A handler answers each decoded {@link Request} with a {@link Response}. It
 * may block to hold a reply, which is how a test keeps an exchange open.
 */
final class VsrLoopbackPeer {

    static final int HEADER_SIZE = 256;
    static final int COMMAND_REPLY = 8;
    static final int COMMAND_EVICTION = 13;

    /** Tells {@link #serve} to drop the connection instead of replying. */
    static final int COMMAND_DISCONNECT = -1;

    /** Tells {@link #serve} to keep the request open with no reply at all. */
    static final int COMMAND_SILENCE = -2;

    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_CLIENT_OFFSET = 128;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REPLY_COMMIT_OFFSET = 184;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;
    private static final int EVICTION_REASON_OFFSET = 255;

    private VsrLoopbackPeer() {}

    static CompletableFuture<Void> serve(ServerSocket server, RequestHandler handler) {
        return serve(server, 1, handler);
    }

    /**
     * Runs blocking socket I/O on one dedicated daemon thread per mock node.
     * The common fork-join pool has only cores minus one workers on small CI
     * runners, so three blocking nodes can starve the client continuations the
     * test is waiting for when the full suite runs concurrently.
     *
     * @param server          the listening socket
     * @param connectionCount how many connections to accept in sequence
     * @param handler         answers each decoded request
     * @return a future completing once every accepted connection has ended
     */
    static CompletableFuture<Void> serve(ServerSocket server, int connectionCount, RequestHandler handler) {
        CompletableFuture<Void> serving = new CompletableFuture<>();
        Thread serverThread = new Thread(
                () -> {
                    try {
                        for (int connection = 0; connection < connectionCount; connection++) {
                            serveConnection(server, handler);
                        }
                        serving.complete(null);
                    } catch (IOException error) {
                        serving.completeExceptionally(new IllegalStateException("Mock VSR server failed", error));
                    } catch (RuntimeException error) {
                        serving.completeExceptionally(error);
                    }
                },
                "vsr-loopback-peer-" + server.getLocalPort());
        serverThread.setDaemon(true);
        serverThread.start();
        return serving;
    }

    /**
     * Accepts connections until the socket closes, each on its own thread, for
     * tests that keep several client connections open at the same time.
     *
     * @param server  the listening socket
     * @param handler answers each decoded request
     */
    static void serveConcurrently(ServerSocket server, RequestHandler handler) {
        Thread acceptor = new Thread(
                () -> {
                    while (!server.isClosed()) {
                        Socket socket;
                        try {
                            socket = server.accept();
                        } catch (IOException closed) {
                            return;
                        }
                        Thread worker = new Thread(() -> serveSocket(socket, handler), "vsr-loopback-connection");
                        worker.setDaemon(true);
                        worker.start();
                    }
                },
                "vsr-loopback-acceptor-" + server.getLocalPort());
        acceptor.setDaemon(true);
        acceptor.start();
    }

    static ByteBuf registerBody(long session) {
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(1);
        body.writeLongLE(session);
        body.writeIntLE(11 << 10);
        body.writeByte(0);
        return body;
    }

    static ByteBuf transientResult(int errorCode) {
        ByteBuf body = Unpooled.buffer(3 * Integer.BYTES);
        body.writeIntLE(1);
        body.writeIntLE(0);
        body.writeIntLE(errorCode);
        return body;
    }

    static ByteBuf singleNodeMetadata(int port) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(1);
        writeNode(body, "node", port, true);
        return body;
    }

    static ByteBuf clusterMetadata(int oldLeaderPort, int newLeaderPort, int leaderPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(2);
        writeNode(body, "old-node", oldLeaderPort, oldLeaderPort == leaderPort);
        writeNode(body, "new-node", newLeaderPort, newLeaderPort == leaderPort);
        return body;
    }

    static ByteBuf threeNodeMetadata(int firstPort, int secondPort, int thirdPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(3);
        writeNode(body, "metadata-leader", firstPort, true);
        writeNode(body, "follower", secondPort, false);
        writeNode(body, "partition-primary", thirdPort, false);
        return body;
    }

    static void writeNode(ByteBuf body, String name, int port, boolean leader) {
        writeString(body, name);
        writeString(body, InetAddress.getLoopbackAddress().getHostAddress());
        body.writeShortLE(port);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeByte(leader ? 0 : 1);
        body.writeByte(0);
    }

    private static void serveConnection(ServerSocket server, RequestHandler handler) throws IOException {
        try (Socket socket = server.accept()) {
            pump(socket, handler);
        }
    }

    private static void serveSocket(Socket socket, RequestHandler handler) {
        try (socket) {
            pump(socket, handler);
        } catch (IOException | RuntimeException ignored) {
            // A client that closes mid-exchange is the behaviour under test.
        }
    }

    private static void pump(Socket socket, RequestHandler handler) throws IOException {
        InputStream input = socket.getInputStream();
        OutputStream output = socket.getOutputStream();
        Request request;
        while ((request = readRequest(input)) != null) {
            Response response = handler.handle(request);
            if (response.command() == COMMAND_DISCONNECT) {
                return;
            }
            writeResponse(output, request, response);
            if (response.closeAfterReply()) {
                return;
            }
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
                fields.getLong(REQUEST_ID_OFFSET),
                fields.getLong(REQUEST_CLIENT_OFFSET),
                fields.getLong(REQUEST_CLIENT_OFFSET + Long.BYTES),
                body);
    }

    private static void writeResponse(OutputStream output, Request request, Response response) throws IOException {
        if (response.command() == COMMAND_SILENCE) {
            return;
        }
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
            fields.putLong(REPLY_COMMIT_OFFSET, response.commit());
        }
        output.write(header);
        output.write(body);
        output.flush();
    }

    private static void writeString(ByteBuf body, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        body.writeIntLE(bytes.length);
        body.writeBytes(bytes);
    }

    @FunctionalInterface
    interface RequestHandler {
        Response handle(Request request);
    }

    record Request(int operation, int commandCode, long requestId, long clientLow, long clientHigh, byte[] body) {
        boolean is(int expectedCode, int expectedOperation) {
            return commandCode == expectedCode && operation == expectedOperation;
        }

        /** The request body as text, for asserting which user a login names. */
        String bodyAsText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    record Response(
            int command,
            int operation,
            int status,
            int evictionReason,
            long commit,
            ByteBuf body,
            boolean closeAfterReply) {

        static Response success(int operation, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, 0, body, false);
        }

        static Response error(int operation, int status) {
            return new Response(COMMAND_REPLY, operation, status, 0, 0, Unpooled.EMPTY_BUFFER, false);
        }

        static Response eviction(int reason) {
            return new Response(COMMAND_EVICTION, 0, 0, reason, 0, Unpooled.EMPTY_BUFFER, false);
        }

        static Response committed(int operation, long commit, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, commit, body, false);
        }

        static Response successAndDisconnect(int operation, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, 0, body, true);
        }

        static Response disconnect() {
            return new Response(COMMAND_DISCONNECT, 0, 0, 0, 0, Unpooled.EMPTY_BUFFER, false);
        }

        static Response noReply() {
            return new Response(COMMAND_SILENCE, 0, 0, 0, 0, Unpooled.EMPTY_BUFFER, false);
        }
    }
}
