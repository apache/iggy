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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Connection failure tests for AsyncIggyTcpClient.
 * Tests various connection failure scenarios including unreachable servers,
 * invalid hosts, invalid ports, timeouts, and connection refused errors.
 */
@DisplayName("Async TCP Connection Failure Tests")
class AsyncTcpConnectionFailureTest extends AsyncTcpTestBase {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTcpConnectionFailureTest.class);

    @Test
    @DisplayName("Connect to unreachable server should fail with ConnectException")
    void testConnectionToUnreachableServer() {
        logger.info("Testing connection to unreachable server");

        // Given: Client configured to connect to an unreachable server
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(12345) // Assuming this port is not listening
                .connectionTimeout(Duration.ofSeconds(2))
                .build();

        // When/Then: Connection should fail with ConnectException
        assertThatThrownBy(() -> client.connect().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectException.class);
    }

    @Test
    @DisplayName("Connect with invalid host should fail with UnknownHostException")
    void testConnectionWithInvalidHost() {
        logger.info("Testing connection with invalid host");

        // Given: Client configured with an invalid host
        client = AsyncIggyTcpClient.builder()
                .host(INVALID_HOST)
                .port(PORT)
                .build();

        // When/Then: Connection should fail with UnknownHostException
        assertThatThrownBy(() -> client.connect().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(UnknownHostException.class);
    }

    @Test
    @DisplayName("Build with invalid port should throw IllegalArgumentException")
    void testConnectionWithInvalidPort() {
        logger.info("Testing builder with invalid port");

        // When/Then: Building with invalid ports should throw IllegalArgumentException
        assertThatThrownBy(() -> AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(65536)
                .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Connection timeout should fail within configured timeout")
    void testConnectionTimeout() {
        logger.info("Testing connection timeout");

        // Given: Client configured with a short connection timeout
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(12345) // Assuming this port is not listening
                .connectionTimeout(Duration.ofMillis(500))
                .build();

        // When/Then: Connection should timeout
        assertThatThrownBy(() -> client.connect().get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectException.class);
    }

    @Test
    @DisplayName("Connection refused should fail quickly")
    void testConnectionRefused() {
        logger.info("Testing connection refused");

        // Given: Client configured to connect to a port that is not listening
        client = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(12345) // Assuming this port is not listening
                .build();

        // When/Then: Connection should be refused
        assertThatThrownBy(() -> client.connect().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Connection refused");
    }

    @Test
    @DisplayName("Concurrent connection failures should be independent")
    void testConcurrentConnectionFailures() {
        logger.info("Testing concurrent connection failures");

        // Given: Multiple clients configured to connect to unreachable servers
        AsyncIggyTcpClient client1 = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(12345)
                .build();

        AsyncIggyTcpClient client2 = AsyncIggyTcpClient.builder()
                .host(HOST)
                .port(12346)
                .build();

        // When: Attempt to connect concurrently
        var future1 = client1.connect();
        var future2 = client2.connect();

        // Then: Both should fail independently
        assertThatThrownBy(() -> future1.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectException.class);

        assertThatThrownBy(() -> future2.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(ConnectException.class);

        // Cleanup
        try {
            client1.close().get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Ignore
        }
        try {
            client2.close().get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Ignore
        }
    }

    // Retry policy tests would go here when we implement them
}