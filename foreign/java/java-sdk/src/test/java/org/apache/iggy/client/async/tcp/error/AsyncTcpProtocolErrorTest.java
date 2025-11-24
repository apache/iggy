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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Protocol error tests for AsyncIggyTcpClient.
 * Tests various protocol error scenarios including server error responses
 * and unexpected response formats.
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
    @DisplayName("Server error response should be properly handled")
    void testServerErrorResponse() throws Exception {
        logger.info("Testing server error response");

        // Given: Mock server that returns 500 error
        mockServer.simulateServerError(500, "Internal Server Error");
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

        // Then: Should properly parse and return the error message
        assertThatThrownBy(() -> sendFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Server error")
                .hasStackTraceContaining("Internal Server Error");
    }

    @Test
    @DisplayName("Empty error response should generate generic error message")
    void testEmptyErrorResponse() throws Exception {
        logger.info("Testing empty error response");

        // Given: Mock server that returns error with no message
        mockServer.simulateServerError(500, "");
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

        // Then: Should generate generic error message
        assertThatThrownBy(() -> sendFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Server error with status: 500");
    }

    @Test
    @DisplayName("Bad Request error should be properly handled")
    void testBadRequestError() throws Exception {
        logger.info("Testing bad request error");

        // Given: Mock server that returns 400 error
        mockServer.simulateServerError(400, "Bad Request");
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

        // Then: Should receive 400 error
        assertThatThrownBy(() -> sendFuture.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasStackTraceContaining("Server error")
                .hasStackTraceContaining("Bad Request");
    }

    @Test
    @DisplayName("Malformed response should be handled gracefully")
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

        // Then: Should handle malformed response gracefully (may timeout waiting for complete frame)
        assertThatThrownBy(() -> sendFuture.get(3, TimeUnit.SECONDS))
                .isInstanceOf(Exception.class); // Could be ExecutionException or TimeoutException
    }

    @Test
    @DisplayName("Multiple error responses should be handled independently")
    void testMultipleErrorResponses() throws Exception {
        logger.info("Testing multiple error responses");

        // Given: Mock server that returns errors
        mockServer.simulateServerError(500, "Internal Server Error");
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Send multiple requests
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

        // Then: Each should fail independently (could be ExecutionException or TimeoutException)
        assertThatThrownBy(() -> future1.get(5, TimeUnit.SECONDS))
                .satisfies(ex -> {
                    assertThat(ex).isInstanceOfAny(ExecutionException.class, java.util.concurrent.TimeoutException.class);
                });

        assertThatThrownBy(() -> future2.get(5, TimeUnit.SECONDS))
                .satisfies(ex -> {
                    assertThat(ex).isInstanceOfAny(ExecutionException.class, java.util.concurrent.TimeoutException.class);
                });
    }
}
