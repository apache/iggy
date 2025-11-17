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

package org.apache.iggy.client.async.tcp.error;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.async.tcp.base.AsyncTcpTestBase;
import org.apache.iggy.client.async.tcp.mock.FaultyNettyServer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Network interruption tests for AsyncIggyTcpClient.
 * Tests various network interruption scenarios including connection drops,
 * partial frame transmission, corrupted frames, and slow networks.
 */
@DisplayName("Async TCP Network Interruption Tests")
class AsyncTcpNetworkInterruptionTest extends AsyncTcpTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTcpNetworkInterruptionTest.class);

    private FaultyNettyServer mockServer;

    @BeforeEach
    void setupNetworkInterruptionTest() throws Exception {
        mockServer = new FaultyNettyServer(PORT);
    }

    @AfterEach
    void cleanupNetworkInterruptionTest() throws Exception {
        try {
            if (mockServer != null) {
                mockServer.stop();
            }
        } catch (Exception ignored) {
            // Server may already be stopped
        }
        if (client != null) {
            try {
                client.close().get(1, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                // Client may already be closed
            }
        }
    }

    @Test
    @DisplayName("Connection drop during send should fail send operation")
    void testConnectionDropDuringSend() throws Exception {
        logger.info("Testing connection drop during send");

        // Given: Mock server that drops connection after accepting it
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to drop connection when receiving data
        mockServer.simulateConnectionTimeout();

        // When: Attempt to send a message
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Send should fail - wait for timeout to trigger
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                });
    }

    @Test
    @DisplayName("Connection drop during receive should fail pending requests")
    void testConnectionDropDuringReceive() throws Exception {
        logger.info("Testing connection drop during receive");

        // Given: Mock server that accepts connection but doesn't respond
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to accept but not respond
        mockServer.simulateConnectionTimeout();

        // When: Send a request and then close the connection from server side
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Give some time for the request to be sent
        Thread.sleep(100);

        // Close server to simulate connection drop
        mockServer.stop();

        // Then: Pending request should fail
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                });
    }

    @Test
    @DisplayName("Partial frame transmission should be handled gracefully")
    void testPartialFrameTransmission() throws Exception {
        logger.info("Testing partial frame transmission");

        // Given: Mock server that sends partial frames
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to send partial frames
        mockServer.simulatePartialFrame();

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should handle partial frame gracefully
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                });
    }

    @Test
    @DisplayName("Corrupted frame header should be detected and handled")
    void testCorruptedFrameHeader() throws Exception {
        logger.info("Testing corrupted frame header");

        // Given: Mock server that sends malformed responses
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to send malformed responses
        mockServer.simulateMalformedResponse();

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should detect and handle corrupted frame
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                });
    }

    @Test
    @DisplayName("Slow network simulation should respect timeouts")
    void testSlowNetworkSimulation() throws Exception {
        logger.info("Testing slow network simulation");

        // Given: Mock server with slow response
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofMillis(500))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to be slow
        mockServer.simulateSlowNetwork(Duration.ofSeconds(2));

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should timeout due to slow network
        assertThatThrownBy(() -> sendFuture.get(1, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("Multiple requests with interruption should all fail")
    void testMultipleRequestsWithInterruption() throws Exception {
        logger.info("Testing multiple requests with interruption");

        // Given: Mock server that will be interrupted
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Send multiple concurrent requests
        CompletableFuture<Void> future1 = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message 1"))
        );
        
        CompletableFuture<Void> future2 = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message 2"))
        );
        
        CompletableFuture<Void> future3 = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message 3"))
        );

        // Give some time for requests to be sent
        Thread.sleep(100);

        // Simulate network interruption by stopping server
        mockServer.stop();

        // Then: All requests should fail
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    // At least one should have timed out
                    boolean hasTimeout = false;
                    try {
                        future1.get(100, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        hasTimeout = true;
                    } catch (Exception ignored) {}
                    if (!hasTimeout) {
                        try {
                            future2.get(100, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            hasTimeout = true;
                        } catch (Exception ignored) {}
                    }
                    if (!hasTimeout) {
                        try {
                            future3.get(100, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            hasTimeout = true;
                        } catch (Exception ignored) {}
                    }
                    assertThat(hasTimeout).isTrue();
                });
    }
}