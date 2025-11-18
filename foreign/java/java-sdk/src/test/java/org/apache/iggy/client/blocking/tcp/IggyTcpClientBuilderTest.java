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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.IntegrationTest;
import org.apache.iggy.system.ClientInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for IggyTcpClient builder pattern.
 * Tests the builder functionality against a running Iggy server.
 */
class IggyTcpClientBuilderTest extends IntegrationTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

    @Test
    void shouldCreateClientWithBuilder() {
        // Given: Builder with basic configuration
        IggyTcpClient client =
                IggyTcpClient.builder().host("127.0.0.1").port(TCP_PORT).build();

        // When: Login to verify connection
        client.users().login("iggy", "iggy");

        // Then: Client should be able to fetch system info
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithCredentials() {
        // Given: Builder with credentials configured
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .credentials("iggy", "iggy")
                .build();

        // When: Try to access system info (auto-login should have happened)
        // Then: Should succeed without explicit login
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithTimeoutConfiguration() {
        // Given: Builder with timeout configuration
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .credentials("iggy", "iggy")
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithConnectionPoolSize() {
        // Given: Builder with connection pool size
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .connectionPoolSize(10)
                .credentials("iggy", "iggy")
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithRetryPolicy() {
        // Given: Builder with exponential backoff retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .retryPolicy(IggyTcpClient.RetryPolicy.exponentialBackoff())
                .credentials("iggy", "iggy")
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithCustomRetryPolicy() {
        // Given: Builder with custom retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .retryPolicy(IggyTcpClient.RetryPolicy.fixedDelay(5, Duration.ofMillis(500)))
                .credentials("iggy", "iggy")
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithNoRetryPolicy() {
        // Given: Builder with no retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .retryPolicy(IggyTcpClient.RetryPolicy.noRetry())
                .credentials("iggy", "iggy")
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithAllOptions() {
        // Given: Builder with all configuration options
        IggyTcpClient client = IggyTcpClient.builder()
                .host("127.0.0.1")
                .port(TCP_PORT)
                .credentials("iggy", "iggy")
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .connectionPoolSize(10)
                .retryPolicy(IggyTcpClient.RetryPolicy.exponentialBackoff(
                        3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0))
                .build();

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldUseDefaultValues() {
        // Given: Builder with only required fields (should use defaults)
        IggyTcpClient client =
                IggyTcpClient.builder().credentials("iggy", "iggy").build(); // Uses default host=localhost, port=8090

        // When: Perform an operation
        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        IggyTcpClient.Builder builder = IggyTcpClient.builder().host("").port(TCP_PORT);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        IggyTcpClient.Builder builder = IggyTcpClient.builder().host(null).port(TCP_PORT);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        IggyTcpClient.Builder builder =
                IggyTcpClient.builder().host("127.0.0.1").port(-1);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        IggyTcpClient.Builder builder =
                IggyTcpClient.builder().host("127.0.0.1").port(0);

        // When/Then: Building should throw IllegalArgumentException
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void shouldMaintainBackwardCompatibilityWithOldConstructor() {
        // Given: Old constructor approach
        IggyTcpClient client = new IggyTcpClient("127.0.0.1", TCP_PORT);

        // When: Login and perform operation
        client.users().login("iggy", "iggy");
        List<ClientInfo> clients = client.system().getClients();

        // Then: Should work as before
        assertNotNull(clients);
    }
}
