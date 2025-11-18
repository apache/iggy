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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for AsyncIggyTcpClient builder pattern.
 * Tests the builder functionality against a running Iggy server.
 */
class AsyncIggyTcpClientBuilderTest {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8090;

    private AsyncIggyTcpClient client;

    @AfterEach
    void cleanup() throws Exception {
        if (client != null) {
            client.close().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldCreateClientWithBuilder() throws Exception {
        // Given: Builder with basic configuration
        client = AsyncIggyTcpClient.builder().host(HOST).port(PORT).build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Client should be connected and functional
        assertNotNull(client.users());
        assertNotNull(client.messages());
        assertNotNull(client.streams());
        assertNotNull(client.topics());
        assertNotNull(client.consumerGroups());
    }

    @Test
    void shouldCreateClientWithCredentials() throws Exception {
        // Given: Builder with credentials configured
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .credentials("iggy", "iggy")
                .build();

        // When: Connect (auto-login should happen)
        CompletableFuture<Void> connectFuture = client.connect();
        connectFuture.get(5, TimeUnit.SECONDS);

        // Then: Should be connected and logged in
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithTimeoutConfiguration() throws Exception {
        // Given: Builder with timeout configuration
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .credentials("iggy", "iggy")
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithConnectionPoolSize() throws Exception {
        // Given: Builder with connection pool size
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .connectionPoolSize(10)
                .credentials("iggy", "iggy")
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithRetryPolicy() throws Exception {
        // Given: Builder with exponential backoff retry policy
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .retryPolicy(AsyncIggyTcpClient.RetryPolicy.exponentialBackoff())
                .credentials("iggy", "iggy")
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithCustomRetryPolicy() throws Exception {
        // Given: Builder with custom retry policy
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .retryPolicy(AsyncIggyTcpClient.RetryPolicy.fixedDelay(5, Duration.ofMillis(500)))
                .credentials("iggy", "iggy")
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithNoRetryPolicy() throws Exception {
        // Given: Builder with no retry policy
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .retryPolicy(AsyncIggyTcpClient.RetryPolicy.noRetry())
                .credentials("iggy", "iggy")
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldCreateClientWithAllOptions() throws Exception {
        // Given: Builder with all configuration options
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .credentials("iggy", "iggy")
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .connectionPoolSize(10)
                .retryPolicy(AsyncIggyTcpClient.RetryPolicy.exponentialBackoff(
                        3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0))
                .build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldUseDefaultValues() throws Exception {
        // Given: Builder with only credentials (should use defaults)
        client = AsyncIggyTcpClient.builder()
                .credentials("iggy", "iggy")
                .build(); // Uses default host=localhost, port=8090

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        AsyncIggyTcpClient.Builder builder =
                AsyncIggyTcpClient.builder().host("").port(PORT);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        AsyncIggyTcpClient.Builder builder =
                AsyncIggyTcpClient.builder().host(null).port(PORT);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        AsyncIggyTcpClient.Builder builder =
                AsyncIggyTcpClient.builder().host(HOST).port(-1);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        AsyncIggyTcpClient.Builder builder =
                AsyncIggyTcpClient.builder().host(HOST).port(0);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldMaintainBackwardCompatibilityWithOldConstructor() throws Exception {
        // Given: Old constructor approach
        client = new AsyncIggyTcpClient(HOST, PORT);

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should work as before
        assertNotNull(client.users());
    }

    @Test
    void shouldConnectAndPerformOperations() throws Exception {
        // Given: Client with credentials
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .credentials("iggy", "iggy")
                .build();

        // When: Connect
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should be able to access all clients
        assertNotNull(client.users(), "Users client should not be null");
        assertNotNull(client.messages(), "Messages client should not be null");
        assertNotNull(client.streams(), "Streams client should not be null");
        assertNotNull(client.topics(), "Topics client should not be null");
        assertNotNull(client.consumerGroups(), "Consumer groups client should not be null");
    }

    @Test
    void shouldCloseConnectionGracefully() throws Exception {
        // Given: Connected client
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(PORT)
                .credentials("iggy", "iggy")
                .build();
        client.connect().get(5, TimeUnit.SECONDS);

        // When: Close connection
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        // Then: Should complete without exception
        assertTrue(closeFuture.isDone());
        assertFalse(closeFuture.isCompletedExceptionally());
    }
}
