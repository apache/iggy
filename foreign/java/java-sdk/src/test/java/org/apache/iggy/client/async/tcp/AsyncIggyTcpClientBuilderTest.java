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

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.blocking.IntegrationTest.LOCALHOST_IP;
import static org.apache.iggy.client.blocking.IntegrationTest.TCP_PORT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for AsyncIggyTcpClient builder pattern.
 * Tests the builder functionality against a running Iggy server.
 */
class AsyncIggyTcpClientBuilderTest {

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
        client = AsyncIggyTcpClient.builder().host(LOCALHOST_IP).port(TCP_PORT).build();

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
    void shouldUseDefaultValues() throws Exception {
        // Given: Builder with defaults (should use host=localhost, port=8090)
        client = AsyncIggyTcpClient.builder().build();

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host("").port(TCP_PORT);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(null).port(TCP_PORT);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(LOCALHOST_IP).port(-1);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(LOCALHOST_IP).port(0);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldMaintainBackwardCompatibilityWithOldConstructor() throws Exception {
        // Given: Old constructor approach
        client = new AsyncIggyTcpClient(LOCALHOST_IP, TCP_PORT);

        // When: Connect to server
        client.connect().get(5, TimeUnit.SECONDS);

        // Then: Should work as before
        assertNotNull(client.users());
    }

    @Test
    void shouldConnectAndPerformOperations() throws Exception {
        // Given: Client
        client = AsyncIggyTcpClient.builder().host(LOCALHOST_IP).port(TCP_PORT).build();

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
        client = AsyncIggyTcpClient.builder().host(LOCALHOST_IP).port(TCP_PORT).build();
        client.connect().get(5, TimeUnit.SECONDS);

        // When: Close connection
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(5, TimeUnit.SECONDS);

        // Then: Should complete without exception
        assertTrue(closeFuture.isDone());
        assertFalse(closeFuture.isCompletedExceptionally());
    }
}
