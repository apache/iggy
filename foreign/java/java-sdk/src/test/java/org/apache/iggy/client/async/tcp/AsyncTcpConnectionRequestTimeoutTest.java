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

import org.apache.iggy.exception.IggyTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncTcpConnectionRequestTimeoutTest {
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(100);

    private AsyncIggyTcpClient client;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReplaceChannelAfterResponseTimeout() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            client = AsyncIggyTcpClient.builder()
                    .host(server.getInetAddress().getHostAddress())
                    .port(server.getLocalPort())
                    .requestTimeout(REQUEST_TIMEOUT)
                    .build();

            CompletableFuture<Socket> firstAccepted = accept(server);
            client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            try (Socket firstSocket = firstAccepted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                verifyRequestTimesOutAndClosesSocket(client.system().ping(), firstSocket);
            }

            CompletableFuture<Socket> secondAccepted = accept(server);
            CompletableFuture<String> secondResponse = client.system().ping();
            try (Socket secondSocket = secondAccepted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                verifyRequestTimesOutAndClosesSocket(secondResponse, secondSocket);
            }
        }
    }

    @Test
    void shouldFailMultiplexedRequestsAndUseReplacementChannelAfterTimeout() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            client = AsyncIggyTcpClient.builder()
                    .host(server.getInetAddress().getHostAddress())
                    .port(server.getLocalPort())
                    .requestTimeout(REQUEST_TIMEOUT)
                    .build();

            CompletableFuture<Socket> firstAccepted = accept(server);
            client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            try (Socket firstSocket = firstAccepted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                CompletableFuture<String> first = client.system().ping();
                CompletableFuture<String> second = client.system().ping();

                assertThat(verifyRequestTimesOutAndClosesSocket(first, firstSocket))
                        .hasSize(2 * 256);
                assertThatThrownBy(() -> second.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IggyTimeoutException.class);
            }

            CompletableFuture<Socket> secondAccepted = accept(server);
            CompletableFuture<String> third = client.system().ping();
            try (Socket secondSocket = secondAccepted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                assertThat(verifyRequestTimesOutAndClosesSocket(third, secondSocket))
                        .hasSize(256);
            }
        }
    }

    private static byte[] verifyRequestTimesOutAndClosesSocket(CompletableFuture<?> response, Socket socket)
            throws Exception {
        assertThatThrownBy(() -> response.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IggyTimeoutException.class);
        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TEST_TIMEOUT_SECONDS));
        byte[] request = socket.getInputStream().readAllBytes();
        assertThat(request).isNotEmpty();
        return request;
    }

    private static CompletableFuture<Socket> accept(ServerSocket server) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return server.accept();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to accept test connection", e);
            }
        });
    }
}
