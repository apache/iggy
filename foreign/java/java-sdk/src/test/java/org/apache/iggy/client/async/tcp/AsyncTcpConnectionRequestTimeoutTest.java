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
    void shouldIgnoreLateReplyAfterTimeoutAndReuseTheSameChannel() throws Exception {
        verifyIsolatedCompletion(false);
    }

    @Test
    void shouldCancelOnlyTheRequestedCallAndKeepItsConcurrentPeer() throws Exception {
        verifyIsolatedCompletion(true);
    }

    private void verifyIsolatedCompletion(boolean cancel) throws Exception {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            client = AsyncIggyTcpClient.builder()
                    .host(server.getInetAddress().getHostAddress())
                    .port(server.getLocalPort())
                    .requestTimeout(REQUEST_TIMEOUT)
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            CompletableFuture<Socket> accepted = accept(server);
            client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            try (Socket socket = accepted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(TEST_TIMEOUT_SECONDS));
                var first = client.system().ping();
                var firstRequest = AsyncIggyTcpClientTransientFailoverTest.readRequest(socket.getInputStream(), 0);
                CompletableFuture<String> second;
                if (cancel) {
                    second = client.system().ping();
                    assertThat(first.cancel(false)).isTrue();
                } else {
                    assertThatThrownBy(() -> first.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(IggyTimeoutException.class);
                    second = client.system().ping();
                }
                var secondRequest = AsyncIggyTcpClientTransientFailoverTest.readRequest(socket.getInputStream(), 0);
                assertThat(secondRequest)
                        .as("an explicit request timeout or cancellation keeps the shared channel open")
                        .isNotNull();
                assertThat(secondRequest.requestId()).isNotEqualTo(firstRequest.requestId());
                AsyncIggyTcpClientTransientFailoverTest.writeResponse(
                        socket.getOutputStream(),
                        firstRequest,
                        AsyncIggyTcpClientTransientFailoverTest.Response.success(
                                firstRequest.operation(), io.netty.buffer.Unpooled.EMPTY_BUFFER));
                AsyncIggyTcpClientTransientFailoverTest.writeResponse(
                        socket.getOutputStream(),
                        secondRequest,
                        AsyncIggyTcpClientTransientFailoverTest.Response.success(
                                secondRequest.operation(), io.netty.buffer.Unpooled.EMPTY_BUFFER));
                assertThat(second.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("pong");
                assertThat(first.isCompletedExceptionally()).isTrue();
            }
        }
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
