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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTcpConnectionHeartbeatTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;

    private static final int COMMAND_REPLY = 8;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int PING_CODE = 1;

    @Test
    void shouldSendHeartbeatsUntilConnectionCloses() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            AtomicInteger requests = new AtomicInteger();
            CompletableFuture<Void> receivedTwoHeartbeats = new CompletableFuture<>();
            CompletableFuture<Void> server = CompletableFuture.runAsync(() -> {
                try (Socket socket = serverSocket.accept()) {
                    socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(2));
                    InputStream input = socket.getInputStream();
                    OutputStream output = socket.getOutputStream();
                    while (true) {
                        Request request = readRequest(input);
                        if (request == null) {
                            return;
                        }
                        assertThat(request.operation()).isEqualTo(OPERATION_NON_REPLICATED);
                        assertThat(request.commandCode()).isEqualTo(PING_CODE);
                        writeResponse(output, request);
                        if (requests.incrementAndGet() == 2) {
                            receivedTwoHeartbeats.complete(null);
                        }
                    }
                } catch (IOException error) {
                    receivedTwoHeartbeats.completeExceptionally(error);
                    throw new IllegalStateException("Mock VSR server failed", error);
                }
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .heartbeatInterval(Duration.ofMillis(25))
                    .requestTimeout(Duration.ofSeconds(1))
                    .build();
            client.connect().get(5, TimeUnit.SECONDS);
            receivedTwoHeartbeats.get(5, TimeUnit.SECONDS);
            client.close().get(5, TimeUnit.SECONDS);
            int requestsAtClose = requests.get();

            server.get(5, TimeUnit.SECONDS);
            Thread.sleep(100);
            assertThat(requests).hasValue(requestsAtClose);
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

    private static void writeResponse(OutputStream output, Request request) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE);
        fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
        header[COMMAND_OFFSET] = COMMAND_REPLY;
        header[REPLY_OPERATION_OFFSET] = OPERATION_NON_REPLICATED;
        output.write(header);
        output.flush();
    }

    private record Request(int operation, int commandCode, long requestId) {}
}
