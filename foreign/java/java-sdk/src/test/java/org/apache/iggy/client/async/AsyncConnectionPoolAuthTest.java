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

package org.apache.iggy.client.async;

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for connection pool authentication lifecycle.
 * Verifies that lazy per-channel authentication works correctly across
 * login, logout, and re-login cycles with a pooled connection.
 */
@DisplayName("Connection Pool Authentication")
class AsyncConnectionPoolAuthTest extends BaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AsyncConnectionPoolAuthTest.class);

    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final int POOL_SIZE = 3;

    private AsyncIggyTcpClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .connectionPoolSize(POOL_SIZE)
                .build();
        client.connect().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("should reject commands before login")
    void shouldRejectCommandsBeforeLogin() {
        // when/then
        assertThatThrownBy(() -> client.streams().getStreams().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IggyNotConnectedException.class);
    }

    @Test
    @DisplayName("should execute commands after login")
    void shouldExecuteCommandsAfterLogin() throws Exception {
        // given
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);

        // when
        var streams = client.streams().getStreams().get(5, TimeUnit.SECONDS);

        // then
        assertThat(streams).isNotNull();
    }

    @Test
    @DisplayName("should login, logout, and re-login successfully")
    void shouldLoginLogoutAndReLogin() throws Exception {
        // given
        String streamName = "auth-test-" + UUID.randomUUID();

        // when - first login
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);
        var streamsAfterLogin = client.streams().getStreams().get(5, TimeUnit.SECONDS);

        // then
        assertThat(streamsAfterLogin).isNotNull();
        log.info("First login successful, got {} streams", streamsAfterLogin.size());

        // when - create a stream to verify full functionality
        var stream = client.streams().createStream(streamName).get(5, TimeUnit.SECONDS);
        assertThat(stream).isNotNull();
        assertThat(stream.name()).isEqualTo(streamName);

        // when - logout
        client.users().logout().get(5, TimeUnit.SECONDS);
        log.info("Logout successful");

        // then - commands should fail after logout
        assertThatThrownBy(() -> client.streams().getStreams().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IggyNotConnectedException.class);
        log.info("Commands correctly rejected after logout");

        // when - re-login
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);
        log.info("Re-login successful");

        // then - commands should work again
        var streamsAfterReLogin = client.streams().getStreams().get(5, TimeUnit.SECONDS);
        assertThat(streamsAfterReLogin).isNotNull();

        // cleanup
        client.streams().deleteStream(StreamId.of(streamName)).get(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("should authenticate all pool channels lazily via concurrent requests")
    void shouldAuthenticatePoolChannelsLazily() throws Exception {
        // given
        String streamName = "pool-auth-test-" + UUID.randomUUID();
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);

        client.streams().createStream(streamName).get(5, TimeUnit.SECONDS);
        client.topics()
                .createTopic(
                        StreamId.of(streamName),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        "test-topic")
                .get(5, TimeUnit.SECONDS);

        // when - fire more concurrent requests than the pool size to force
        // multiple channels to be created and lazily authenticated
        int concurrentRequests = POOL_SIZE * 3;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < concurrentRequests; i++) {
            var future = client.messages()
                    .sendMessages(
                            StreamId.of(streamName),
                            org.apache.iggy.identifier.TopicId.of("test-topic"),
                            Partitioning.partitionId(0L),
                            List.of(Message.of("msg-" + i)));
            futures.add(future);
        }

        // then - all requests should complete successfully
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);
        log.info("All {} concurrent requests completed successfully", concurrentRequests);

        // cleanup
        client.streams().deleteStream(StreamId.of(streamName)).get(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("should re-authenticate stale channels after logout and re-login")
    void shouldReAuthenticateStaleChannelsAfterReLogin() throws Exception {
        // given - login and warm up multiple pool channels
        String streamName = "reauth-test-" + UUID.randomUUID();
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);

        client.streams().createStream(streamName).get(5, TimeUnit.SECONDS);
        client.topics()
                .createTopic(
                        StreamId.of(streamName),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        "test-topic")
                .get(5, TimeUnit.SECONDS);

        List<CompletableFuture<Void>> warmupFutures = new ArrayList<>();
        for (int i = 0; i < POOL_SIZE * 2; i++) {
            warmupFutures.add(client.messages()
                    .sendMessages(
                            StreamId.of(streamName),
                            org.apache.iggy.identifier.TopicId.of("test-topic"),
                            Partitioning.partitionId(0L),
                            List.of(Message.of("warmup-" + i))));
        }
        CompletableFuture.allOf(warmupFutures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
        log.info("Pool warmed up with {} requests", warmupFutures.size());

        // when - logout and re-login (invalidates all channel auth generations)
        client.users().logout().get(5, TimeUnit.SECONDS);
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);
        log.info("Logout + re-login complete");

        // then - all channels should re-authenticate transparently
        List<CompletableFuture<Void>> postReLoginFutures = new ArrayList<>();
        for (int i = 0; i < POOL_SIZE * 2; i++) {
            postReLoginFutures.add(client.messages()
                    .sendMessages(
                            StreamId.of(streamName),
                            org.apache.iggy.identifier.TopicId.of("test-topic"),
                            Partitioning.partitionId(0L),
                            List.of(Message.of("after-relogin-" + i))));
        }
        CompletableFuture.allOf(postReLoginFutures.toArray(new CompletableFuture[0]))
                .get(15, TimeUnit.SECONDS);
        log.info("All {} post-re-login requests succeeded", postReLoginFutures.size());

        // cleanup
        client.streams().deleteStream(StreamId.of(streamName)).get(5, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("should handle multiple sequential login-logout cycles")
    void shouldHandleMultipleLoginLogoutCycles() throws Exception {
        // given
        int cycles = 3;

        for (int i = 0; i < cycles; i++) {
            // when - login
            client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);

            // then - verify commands work
            var streams = client.streams().getStreams().get(5, TimeUnit.SECONDS);
            assertThat(streams).isNotNull();
            log.info("Cycle {}: login and command succeeded", i + 1);

            // when - logout
            client.users().logout().get(5, TimeUnit.SECONDS);

            // then - verify commands are rejected
            assertThatThrownBy(() -> client.streams().getStreams().get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IggyNotConnectedException.class);
            log.info("Cycle {}: logout and rejection verified", i + 1);
        }
    }
}
