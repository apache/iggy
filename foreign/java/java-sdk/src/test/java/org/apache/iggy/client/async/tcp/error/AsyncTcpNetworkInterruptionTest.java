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

/**
 * Network interruption tests for AsyncIggyTcpClient.
 * Tests various network interruption scenarios including connection drops,
 * partial frame transmission, corrupted frames, and slow networks.
 * 
 * Note: These tests use a mock server and may not perfectly replicate real network conditions.
 * They are designed to test the client's resilience to various error scenarios.
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
        if (mockServer != null) {
            try {
                mockServer.stop();
            } catch (Exception e) {
                logger.warn("Error stopping mock server: {}", e.getMessage());
            }
        }
        if (client != null) {
            try {
                client.close().get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.warn("Error closing client: {}", e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("Server accepting but not responding should timeout")
    void testConnectionTimeout() throws Exception {
        logger.info("Testing server not responding");

        // Given: Mock server that accepts connections but doesn't respond
        mockServer.simulateConnectionTimeout();
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        // Client connects successfully (TCP handshake completes)
        client.connect().get(5, TimeUnit.SECONDS);

        // When: Try to send a message (requires server response)
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should timeout because server doesn't respond
        assertThatThrownBy(() -> sendFuture.get(3, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("Malformed response should cause error")
    void testMalformedResponse() throws Exception {
        logger.info("Testing malformed response");

        // Given: Mock server that sends malformed responses
        mockServer.simulateMalformedResponse();
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should fail due to malformed response
        // This may timeout or fail with an exception depending on how the decoder handles it
        assertThatThrownBy(() -> sendFuture.get(3, TimeUnit.SECONDS))
                .satisfiesAnyOf(
                    ex -> assertThat(ex).isInstanceOf(ExecutionException.class),
                    ex -> assertThat(ex).isInstanceOf(TimeoutException.class)
                );
    }

    @Test
    @DisplayName("Partial frame transmission should be handled")
    void testPartialFrameTransmission() throws Exception {
        logger.info("Testing partial frame transmission");

        // Given: Mock server that sends partial frames
        mockServer.simulatePartialFrame();
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should fail due to incomplete frame
        assertThatThrownBy(() -> sendFuture.get(3, TimeUnit.SECONDS))
                .satisfiesAnyOf(
                    ex -> assertThat(ex).isInstanceOf(ExecutionException.class),
                    ex -> assertThat(ex).isInstanceOf(TimeoutException.class)
                );
    }

    @Test
    @DisplayName("Slow network should respect timeouts")
    void testSlowNetworkSimulation() throws Exception {
        logger.info("Testing slow network simulation");

        // Given: Mock server with slow response (2 seconds delay)
        mockServer.simulateSlowNetwork(Duration.ofSeconds(2));
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Send a request with short timeout
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should timeout before server responds
        assertThatThrownBy(() -> sendFuture.get(1, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    @DisplayName("Server crash during request should fail pending requests")
    void testServerCrashDuringRequest() throws Exception {
        logger.info("Testing server crash during request");

        // Given: Mock server
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to not respond
        mockServer.simulateConnectionTimeout();

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Give some time for the request to be sent
        Thread.sleep(100);

        // Crash the server
        mockServer.stop();

        // Then: Pending request should fail
        assertThatThrownBy(() -> sendFuture.get(3, TimeUnit.SECONDS))
                .satisfiesAnyOf(
                    ex -> assertThat(ex).isInstanceOf(ExecutionException.class),
                    ex -> assertThat(ex).isInstanceOf(TimeoutException.class)
                );
    }
}
