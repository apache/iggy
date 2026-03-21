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
import org.apache.iggy.exception.IggyAuthenticationException;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
        assertThat(client.users()).isNotNull();
        assertThat(client.messages()).isNotNull();
        assertThat(client.streams()).isNotNull();
        assertThat(client.topics()).isNotNull();
        assertThat(client.consumerGroups()).isNotNull();
        assertThat(client.system()).isNotNull();
        assertThat(client.personalAccessTokens()).isNotNull();
        assertThat(client.partitions()).isNotNull();
        assertThat(client.consumerOffsets()).isNotNull();
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
        assertThat(client.users()).isNotNull();
    }

    @Test
    void shouldThrowExceptionForEmptyHost() {
        // Given: Builder with empty host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host("").port(serverTcpPort());

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThatThrownBy(builder::build).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldThrowExceptionForNullHost() {
        // Given: Builder with null host
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(null).port(serverTcpPort());

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThatThrownBy(builder::build).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldThrowExceptionForInvalidPort() {
        // Given: Builder with invalid port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(-1);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThatThrownBy(builder::build).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldThrowExceptionForZeroPort() {
        // Given: Builder with zero port
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(0);

        // When/Then: Building should throw IggyInvalidArgumentException
        assertThatThrownBy(builder::build).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldMaintainBackwardCompatibilityWithOldConstructor() throws Exception {
        // Given: Old constructor approach
        client = new AsyncIggyTcpClient(serverHost(), serverTcpPort());

        // When: Connect to server
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Then: Should work as before
        assertThat(client.users()).isNotNull();
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
        assertThat(client.users()).as("Users client should not be null").isNotNull();
        assertThat(client.messages()).as("Messages client should not be null").isNotNull();
        assertThat(client.streams()).as("Streams client should not be null").isNotNull();
        assertThat(client.topics()).as("Topics client should not be null").isNotNull();
        assertThat(client.consumerGroups())
                .as("Consumer groups client should not be null")
                .isNotNull();
        assertThat(client.system()).as("System client should not be null").isNotNull();
        assertThat(client.personalAccessTokens())
                .as("Personal access tokens client should not be null")
                .isNotNull();
        assertThat(client.partitions())
                .as("Partitions client should not be null")
                .isNotNull();
        assertThat(client.consumerOffsets())
                .as("Consumer offsets client should not be null")
                .isNotNull();
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
        assertThat(closeFuture.isDone()).isTrue();
        assertThat(closeFuture.isCompletedExceptionally()).isFalse();
    }

    @Test
    void testThrowExceptionWhenLoginBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .build();

        assertThatThrownBy(() -> client.login()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingUsersBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.users()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingMessagesBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.messages()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingStreamsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.streams()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingTopicsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.topics()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingConsumerGroupsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.consumerGroups()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingSystemBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.system()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingPersonalAccessTokensBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.personalAccessTokens()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingPartitionsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.partitions()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenAccessingConsumerOffsetsBeforeConnect() {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        assertThatThrownBy(() -> client.consumerOffsets()).isInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    void testThrowExceptionWhenLoginWithoutCredentials() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        client.connect().get(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> client.login()).isInstanceOf(IggyMissingCredentialsException.class);
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithoutCredentials() {
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().host(serverHost()).port(serverTcpPort());

        assertThatThrownBy(builder::buildAndLogin).isInstanceOf(IggyMissingCredentialsException.class);
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithNullUsername() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(null, TEST_PASSWORD);

        assertThatThrownBy(builder::buildAndLogin).isInstanceOf(IggyMissingCredentialsException.class);
    }

    @Test
    void testThrowExceptionWhenBuildAndLoginWithNullPassword() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, null);

        assertThatThrownBy(builder::buildAndLogin).isInstanceOf(IggyMissingCredentialsException.class);
    }

    @Test
    void testBuildClientWithCredentials() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithConnectionTimeout() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .connectionTimeout(Duration.ofSeconds(10))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithRequestTimeout() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .requestTimeout(Duration.ofSeconds(30))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithConnectionPoolSize() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .connectionPoolSize(5)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithExponentialBackoffRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.exponentialBackoff())
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithFixedDelayRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.fixedDelay(3, Duration.ofMillis(100)))
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithNoRetryPolicy() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .retryPolicy(RetryPolicy.noRetry())
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithTlsBoolean() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .tls(false)
                .build();
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }

    @Test
    void testBuildClientWithEnableTls() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .enableTls()
                .build();

        assertThat(client).isNotNull();
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

        assertThat(client.users()).isNotNull();
        assertThat(client.messages()).isNotNull();
        assertThat(client.streams()).isNotNull();
        assertThat(client.topics()).isNotNull();
        assertThat(client.consumerGroups()).isNotNull();
        assertThat(client.system()).isNotNull();
        assertThat(client.personalAccessTokens()).isNotNull();
        assertThat(client.partitions()).isNotNull();
        assertThat(client.consumerOffsets()).isNotNull();
    }

    @Test
    void testBuildAndLoginSuccessfully() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .buildAndLogin()
                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
        assertThat(client.messages()).isNotNull();
        assertThat(client.streams()).isNotNull();
        assertThat(client.topics()).isNotNull();
        assertThat(client.consumerGroups()).isNotNull();
        assertThat(client.system()).isNotNull();
        assertThat(client.personalAccessTokens()).isNotNull();
        assertThat(client.partitions()).isNotNull();
        assertThat(client.consumerOffsets()).isNotNull();
    }

    @Test
    void testHandleCloseOnUnconnectedClient() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        CompletableFuture<Void> closeFuture = client.close();
        closeFuture.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(closeFuture.isDone()).isTrue();
        assertThat(closeFuture.isCompletedExceptionally()).isFalse();
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

        assertThat(firstClose.isDone()).isTrue();
        assertThat(firstClose.isCompletedExceptionally()).isFalse();
        assertThat(secondClose.isDone()).isTrue();
        assertThat(secondClose.isCompletedExceptionally()).isFalse();
    }

    @Test
    void testHandleNullTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate((String) null);

        assertThat(builder).isNotNull();
    }

    @Test
    void testHandleEmptyTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate("");

        assertThat(builder).isNotNull();
    }

    @Test
    void testHandleBlankTlsCertificateString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder().tlsCertificate("   ");

        assertThat(builder).isNotNull();
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

        assertThat(client.users()).isNotNull();
    }

    @Test
    void shouldCompleteExceptionallyWhenBuildAndLoginWithWrongCredentials() {
        // given
        CompletableFuture<AsyncIggyTcpClient> future = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, "wrong_password")
                .buildAndLogin();

        // when/then
        assertThatThrownBy(() -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IggyAuthenticationException.class)
                .extracting(e -> e.getCause().getSuppressed())
                .satisfies(suppressed -> assertThat(suppressed).isEmpty());
    }

    @Test
    void shouldCleanUpResourcesWhenBuildAndLoginFailsWithWrongCredentials() throws Exception {
        // given: a buildAndLogin that will fail due to wrong credentials
        CompletableFuture<AsyncIggyTcpClient> future = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, "wrong_password")
                .buildAndLogin();

        // when: wait for the failure to complete (including close cleanup)
        try {
            future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException ignored) {
            // expected
        }

        // then: resources should be cleaned up — a subsequent buildAndLogin should succeed
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, TEST_PASSWORD)
                .buildAndLogin()
                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(client.users()).isNotNull();
    }

    @Test
    void shouldWaitForCloseBeforeCompletingExceptionally() {
        // given
        CompletableFuture<AsyncIggyTcpClient> future = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, "wrong_password")
                .buildAndLogin();

        // when: the future completes exceptionally
        assertThatThrownBy(() -> future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);

        // then: after get() returns, the future is done — close() has already completed
        assertThat(future).isCompletedExceptionally();
    }

    @Test
    void shouldPreserveOriginalExceptionTypeFromBuildAndLogin() {
        // given
        CompletableFuture<AsyncIggyTcpClient> future = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(TEST_USERNAME, "wrong_password")
                .buildAndLogin();

        // when/then: the cause chain should contain the authentication exception directly
        assertThatThrownBy(() -> future.join())
                .isInstanceOf(CompletionException.class)
                .cause()
                .isInstanceOf(IggyAuthenticationException.class);
    }

    @Test
    void testBuildAndConnectWithoutCredentialsThenLoginExplicitly() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();

        client.connect().get(5, TimeUnit.SECONDS);
        client.users().login(TEST_USERNAME, TEST_PASSWORD).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(client.users()).isNotNull();
    }
}
