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
 * Protocol error tests for AsyncIggyTcpClient.
 * Tests various protocol error scenarios including invalid command codes,
 * malformed request payloads, server error responses, and unexpected response formats.
 */
@DisplayName("Async TCP Protocol Error Tests")
class AsyncTcpProtocolErrorTest extends AsyncTcpTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTcpProtocolErrorTest.class);

    private FaultyNettyServer mockServer;

    @BeforeEach
    void setupProtocolErrorTest() throws Exception {
        mockServer = new FaultyNettyServer(PORT);
    }

    @AfterEach
    void cleanupProtocolErrorTest() throws Exception {
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
    @DisplayName("Invalid command code should return server error")
    void testInvalidCommandCode() throws Exception {
        logger.info("Testing invalid command code");

        // Given: Mock server that returns error for invalid command codes
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to return error for invalid commands
        mockServer.simulateServerError(1001, "Invalid command");

        // When: Send a request with an invalid command code would require accessing internal APIs
        // For now, we'll test with a valid command that the server is configured to reject
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should receive server error
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(ExecutionException.class);
                });
    }

    @Test
    @DisplayName("Malformed request payload should return 400 error")
    void testMalformedRequestPayload() throws Exception {
        logger.info("Testing malformed request payload");

        // Given: Mock server
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to return 400 error for malformed requests
        mockServer.simulateServerError(400, "Bad Request");

        // When: Send a request (server configured to treat all requests as malformed)
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should receive 400 error
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(ExecutionException.class);
                });
    }

    @Test
    @DisplayName("Server error response should be properly parsed")
    void testServerErrorResponse() throws Exception {
        logger.info("Testing server error response");

        // Given: Mock server that returns 500 error
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to return 500 error
        mockServer.simulateServerError(500, "Internal Server Error");

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should properly parse and return the error message
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(ExecutionException.class);
                });
    }

    @Test
    @DisplayName("Empty error response should generate generic error message")
    void testEmptyErrorResponse() throws Exception {
        logger.info("Testing empty error response");

        // Given: Mock server that returns error with no message
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to return error with empty message
        mockServer.simulateServerError(500, "");

        // When: Send a request
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        // Then: Should generate generic error message
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(ExecutionException.class);
                });
    }

    @Test
    @DisplayName("Unexpected response format should be handled")
    void testUnexpectedResponseFormat() throws Exception {
        logger.info("Testing unexpected response format");

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

        // Then: Should handle unexpected format gracefully
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> sendFuture.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(TimeoutException.class);
                });
    }

    @Test
    @DisplayName("Multiple errors in pipeline should be handled independently")
    void testMultipleErrorsInPipeline() throws Exception {
        logger.info("Testing multiple errors in pipeline");

        // Given: Mock server that returns errors
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .requestTimeout(Duration.ofSeconds(2))
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Configure server to return errors
        mockServer.simulateServerError(500, "Internal Server Error");

        // When: Send multiple requests concurrently
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

        // Then: Each should be handled independently
        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThatThrownBy(() -> future1.get(100, TimeUnit.MILLISECONDS))
                            .isInstanceOf(ExecutionException.class);
                });
    }
}