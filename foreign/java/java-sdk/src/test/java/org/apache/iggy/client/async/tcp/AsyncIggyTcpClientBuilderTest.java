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

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for AsyncIggyTcpClient builder pattern.
 * Tests the builder functionality against a running Iggy server.
 */
class AsyncIggyTcpClientBuilderTest extends BaseIntegrationTest {

    private static final String TEST_USERNAME = "iggy";
    private static final String TEST_PASSWORD = "iggy";
    private static final int TEST_TIMEOUT_SECONDS = 5;

    private AsyncIggyTcpClient client;

    @AfterEach
    void cleanup() throws Exception {
        if (client != null) {
            client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldCreateClientWithBuilder() throws Exception {
        // Given: Builder with basic configuration
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        // When: Connect to server
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Client should be connected and functional
        assertNotNull(client.users());
        assertNotNull(client.messages());
        assertNotNull(client.streams());
        assertNotNull(client.topics());
        assertNotNull(client.consumerGroups());
        assertNotNull(client.system());
        assertNotNull(client.personalAccessTokens());
        assertNotNull(client.partitions());
        assertNotNull(client.consumerOffsets());
    }

    @Test
    void shouldUseDefaultValues() throws Exception {
        // This only applies to external-server mode where endpoint is fixed at localhost:8090.
        assumeTrue(
                System.getenv("USE_EXTERNAL_SERVER") != null,
                "Default host/port test requires external server mode at 127.0.0.1:8090");

        // Given: Builder with defaults (should use host=localhost, port=8090)
        client = AsyncIggyTcpClient.builder().build();

        // When: Connect to server
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Should succeed
        assertNotNull(client.users());
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host("").port(serverTcpPort());

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(null).port(serverTcpPort());

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(-1);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(0);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThrows(IggyInvalidArgumentException.class, builder::build);
    }

    @Test
    void shouldMaintainBackwardCompatibilityWithOldConstructor() throws Exception {
        // Given: Old constructor approach
        client = new AsyncIggyTcpClient(serverHost(), serverTcpPort());

        // When: Connect to server
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Should work as before
        assertNotNull(client.users());
    }

    @Test
    void shouldConnectAndPerformOperations() throws Exception {
        // Given: Client
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        // When: Connect
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Should be able to access all clients
        assertNotNull(client.users(), "Users client should not be null");
        assertNotNull(client.messages(), "Messages client should not be null");
        assertNotNull(client.streams(), "Streams client should not be null");
        assertNotNull(client.topics(), "Topics client should not be null");
        assertNotNull(client.consumerGroups(), "Consumer groups client should not be null");
        assertNotNull(client.system(), "System client should not be null");
        assertNotNull(client.personalAccessTokens(), "Personal access tokens client should not be null");
        assertNotNull(client.partitions(), "Partitions client should not be null");
        assertNotNull(client.consumerOffsets(), "Consumer offsets client should not be null");
    }

    @Test
    void shouldCloseConnectionGracefully() throws Exception {
        // Given: Connected client
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        client.connect().get(5, TimeUnit.SECONDS);

        // When: Close connection
        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Should complete without exception
        assertTrue(closeFuture.isDone());
        assertFalse(closeFuture.isCompletedExceptionally());
    }

    @Test
    void testThrowExceptionWhenLoginBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.login());
    }

    @Test
    void testThrowExceptionWhenAccessingUsersBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.users());
    }

    @Test
    void testThrowExceptionWhenAccessingMessagesBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.messages());
    }

    @Test
    void testThrowExceptionWhenAccessingStreamsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.streams());
    }

    @Test
    void testThrowExceptionWhenAccessingTopicsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.topics());
    }

    @Test
    void testThrowExceptionWhenAccessingConsumerGroupsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.consumerGroups());
    }

    @Test
    void testThrowExceptionWhenAccessingSystemBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.system());
    }

    @Test
    void testThrowExceptionWhenAccessingPersonalAccessTokensBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.personalAccessTokens());
    }

    @Test
    void testThrowExceptionWhenAccessingPartitionsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.partitions());
    }

    @Test
    void testThrowExceptionWhenAccessingConsumerOffsetsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThrows(IggyNotConnectedException.class, () -> client.consumerOffsets());
    }

    @Test
    void testThrowExceptionWhenLoginWithoutCredentials() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        client.connect().get(5, TimeUnit.SECONDS);

        assertThrows(IggyMissingCredentialsException.class, () -> client.login());
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithoutCredentials() {
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(serverTcpPort());

        assertThrows(IggyMissingCredentialsException.class, builder::buildAndLogin);
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithNullUsername() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(null, TEST_PASSWORD);

        assertThrows(IggyMissingCredentialsException.class, builder::buildAndLogin);
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithNullPassword() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, null);

        assertThrows(IggyMissingCredentialsException.class, builder::buildAndLogin);
    }

    @Test
    void testBuildClientWithCredentials() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithConnectionTimeout() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .connectionTimeout(Duration.ofSeconds(10))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithRequestTimeout() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .requestTimeout(Duration.ofSeconds(30))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithConnectionPoolSize() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .connectionPoolSize(5)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithExponentialBackoffRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.exponentialBackoff())
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithFixedDelayRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.fixedDelay(3, Duration.ofMillis(100)))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithNoRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.noRetry())
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithTlsBoolean() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .tls(false)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildClientWithEnableTls() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .enableTls()
                .build();

        assertNotNull(client);
    }

    @Test
    void testBuildClientWithAllConfigurationOptions() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .connectionTimeout(Duration.ofSeconds(10))
                .requestTimeout(Duration.ofSeconds(30))
                .connectionPoolSize(5)
                .retryPolicy(RetryPolicy.exponentialBackoff())
                .tls(false)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
        assertNotNull(client.messages());
        assertNotNull(client.streams());
        assertNotNull(client.topics());
        assertNotNull(client.consumerGroups());
        assertNotNull(client.system());
        assertNotNull(client.personalAccessTokens());
        assertNotNull(client.partitions());
        assertNotNull(client.consumerOffsets());
    }

    @Test
    void testBuildAndLoginSuccessfully() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .buildAndLogin()
                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
        assertNotNull(client.messages());
        assertNotNull(client.streams());
        assertNotNull(client.topics());
        assertNotNull(client.consumerGroups());
        assertNotNull(client.system());
        assertNotNull(client.personalAccessTokens());
        assertNotNull(client.partitions());
        assertNotNull(client.consumerOffsets());
    }

    @Test
    void testHandleCloseOnUnconnectedClient() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue(closeFuture.isDone());
        assertFalse(closeFuture.isCompletedExceptionally());
    }

    @Test
    void testHandleMultipleCloseCalls() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        client.connect().get(5, TimeUnit.SECONDS);

        CompletableFuture<Void> firstClose = client.close();
        firstClose.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        CompletableFuture<Void> secondClose = client.close();
        secondClose.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue(firstClose.isDone());
        assertFalse(firstClose.isCompletedExceptionally());
        assertTrue(secondClose.isDone());
        assertFalse(secondClose.isCompletedExceptionally());
    }

    @Test
    void testHandleNullTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate((String) null);

        assertNotNull(builder);
    }

    @Test
    void testHandleEmptyTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate("");

        assertNotNull(builder);
    }

    @Test
    void testHandleBlankTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate("   ");

        assertNotNull(builder);
    }

    @Test
    void testBuildConnectAndLoginManually() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .build();

        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }

    @Test
    void testBuildAndConnectWithoutCredentialsThenLoginExplicitly() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        client.connect().get(5, TimeUnit.SECONDS);
        client.users().login(TEST_USERNAME, TEST_PASSWORD).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertNotNull(client.users());
    }
}
