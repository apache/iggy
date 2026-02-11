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
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.system.ClientInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
        // Given: Builder with basic configuration and credentials
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Client should be able to fetch system info
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithCredentials() {
        // Given: Builder with credentials configured
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // When: Try to access system info (auto-login should have happened)
        // Then: Should succeed without explicit login
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithTimeoutConfiguration() {
        // Given: Builder with timeout configuration
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithConnectionPoolSize() {
        // Given: Builder with connection pool size
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .connectionPoolSize(10)
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithRetryPolicy() {
        // Given: Builder with exponential backoff retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .retryPolicy(RetryPolicy.exponentialBackoff())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithCustomRetryPolicy() {
        // Given: Builder with custom retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .retryPolicy(RetryPolicy.fixedDelay(5, Duration.ofMillis(500)))
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithNoRetryPolicy() {
        // Given: Builder with no retry policy
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .retryPolicy(RetryPolicy.noRetry())
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldCreateClientWithAllOptions() {
        // Given: Builder with all configuration options
        IggyTcpClient client = IggyTcpClient.builder()
                .host(LOCALHOST_IP)
                .port(tcpPort())
                .connectionTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(10))
                .connectionPoolSize(10)
                .retryPolicy(RetryPolicy.exponentialBackoff(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0))
                .credentials("iggy", "iggy")
                .buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldUseDefaultValues() {
        // This only applies to external-server mode where endpoint is fixed at localhost:8090.
        assumeTrue(
                System.getenv("USE_EXTERNAL_SERVER") != null,
                "Default host/port test requires external server mode at 127.0.0.1:8090");

        // Given: Builder with only credentials (should use default host=localhost, port=8090)
        IggyTcpClient client =
                IggyTcpClient.builder().credentials("iggy", "iggy").buildAndLogin();

        // Then: Should succeed
        List<ClientInfo> clients = client.system().getClients();
        assertNotNull(clients);
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        IggyTcpClientBuilder builder = IggyTcpClient.builder().host("").port(TCP_PORT);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        IggyTcpClientBuilder builder = IggyTcpClient.builder().host(null).port(TCP_PORT);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        IggyTcpClientBuilder builder =
                IggyTcpClient.builder().host(LOCALHOST_IP).port(-1);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        IggyTcpClientBuilder builder =
                IggyTcpClient.builder().host(LOCALHOST_IP).port(0);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldWorkWithConstructorAndExplicitConnect() {
        // Given: Constructor approach with explicit connect
        IggyTcpClient client = new IggyTcpClient(LOCALHOST_IP, tcpPort());

        // When: Connect, login and perform operation
        client.connect();
        client.users().login("iggy", "iggy");
        List<ClientInfo> clients = client.system().getClients();

        // Then: Should work
        assertNotNull(clients);
    }
}
