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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Resource cleanup tests for AsyncIggyTcpClient.
 * Tests proper cleanup of resources including channels, event loop groups,
 * pending requests, and memory buffers.
 */
@DisplayName("Async TCP Resource Cleanup Tests")
class AsyncTcpResourceCleanupTest extends AsyncTcpTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTcpResourceCleanupTest.class);

    private FaultyNettyServer mockServer;

    @BeforeEach
    void setupResourceCleanupTest() throws Exception {
        mockServer = new FaultyNettyServer(PORT);
    }

    @AfterEach
    void cleanupResourceCleanupTest() throws Exception {
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
    @DisplayName("Graceful channel close should complete successfully")
    void testGracefulChannelClose() throws Exception {
        logger.info("Testing graceful channel close");

        // Given: Connected client
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Close the connection
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        // Then: Should complete successfully
        assertThat(closeFuture.isDone()).isTrue();
        assertThat(closeFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    @DisplayName("Multiple close invocations should be idempotent")
    void testMultipleCloseInvocations() throws Exception {
        logger.info("Testing multiple close invocations");

        // Given: Connected client
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Close multiple times
        CompletableFuture<Void> closeFuture1 = client.close();
        closeFuture1.get(5, TimeUnit.SECONDS);
        
        CompletableFuture<Void> closeFuture2 = client.close();
        closeFuture2.get(5, TimeUnit.SECONDS);
        
        CompletableFuture<Void> closeFuture3 = client.close();
        closeFuture3.get(5, TimeUnit.SECONDS);

        // Then: All should complete successfully
        assertThat(closeFuture1.isDone()).isTrue();
        assertThat(closeFuture2.isDone()).isTrue();
        assertThat(closeFuture3.isDone()).isTrue();
        
        assertThat(closeFuture1.isCompletedExceptionally()).isFalse();
        assertThat(closeFuture2.isCompletedExceptionally()).isFalse();
        assertThat(closeFuture3.isCompletedExceptionally()).isFalse();
    }

    @Test
    @DisplayName("Close before connect should work")
    void testCloseBeforeConnect() throws Exception {
        logger.info("Testing close before connect");

        // Given: Client not yet connected
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        // When: Close without connecting
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        // Then: Should complete successfully
        assertThat(closeFuture.isDone()).isTrue();
        assertThat(closeFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    @DisplayName("Operations after close should fail")
    void testOperationsAfterClose() throws Exception {
        logger.info("Testing operations after close");

        // Given: Connected then closed client
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);
        client.close().get(5, TimeUnit.SECONDS);

        // When: Try to send a message after close
        // Then: Should throw IllegalStateException because clients are not initialized
        assertThatThrownBy(() -> client.messages())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Client not connected");
    }

    @Test
    @DisplayName("Client after error should handle close gracefully")
    void testCloseAfterError() throws Exception {
        logger.info("Testing close after error");

        // Given: Client that encounters an error
        mockServer.simulateServerError(500, "Internal Server Error");
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // Send a request that fails
        CompletableFuture<Void> sendFuture = client.messages().sendMessagesAsync(
                StreamId.of(1L),
                TopicId.of(1L),
                Partitioning.partitionId(1L),
                Collections.singletonList(Message.of("test message"))
        );

        try {
            sendFuture.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Expected to fail
        }

        // When: Close after error
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        // Then: Should close gracefully
        assertThat(closeFuture.isDone()).isTrue();
        assertThat(closeFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    @DisplayName("Concurrent close and send should be thread-safe")
    void testConcurrentCloseAndSend() throws Exception {
        logger.info("Testing concurrent close and send");

        // Given: Connected client
        mockServer.start().get(5, TimeUnit.SECONDS);
        
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .build();

        client.connect().get(5, TimeUnit.SECONDS);

        // When: Concurrently send and close
        CompletableFuture<Void> sendFuture = CompletableFuture.runAsync(() -> {
            try {
                client.messages().sendMessagesAsync(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Partitioning.partitionId(1L),
                        Collections.singletonList(Message.of("test message"))
                ).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                // May fail if close happens first, which is acceptable
                logger.debug("Send failed (expected if close happened first): {}", e.getMessage());
            }
        });
        
        CompletableFuture<Void> closeFuture = client.close();

        // Then: Close should complete successfully
        closeFuture.get(5, TimeUnit.SECONDS);
        assertThat(closeFuture.isDone()).isTrue();

        // Wait for send to complete (may have succeeded or failed)
        try {
            sendFuture.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Acceptable
        }
    }
}
