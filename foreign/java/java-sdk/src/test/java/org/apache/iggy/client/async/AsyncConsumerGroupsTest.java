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
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
 * Async-specific tests for {@link ConsumerGroupsClient}: error scenarios,
 * multi-client membership, and CompletableFuture patterns (chaining, concurrency).
 *
 * <p>Basic CRUD and join/leave operations are covered by the blocking
 * {@code ConsumerGroupsClientBaseTest} and {@code ConsumerGroupsTcpClientTest}.
 */
public class AsyncConsumerGroupsTest extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncConsumerGroupsTest.class);

    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final String TEST_STREAM = "async-cg-test-stream-" + UUID.randomUUID();
    private static final String TEST_TOPIC = "async-cg-test-topic";
    private static final int TIMEOUT_SECONDS = 5;

    private static final StreamId STREAM_ID = StreamId.of(TEST_STREAM);
    private static final TopicId TOPIC_ID = TopicId.of(TEST_TOPIC);

    private static AsyncIggyTcpClient client;

    @BeforeAll
    public static void setup() throws Exception {
        log.info("Setting up async consumer groups test");
        client = new AsyncIggyTcpClient(serverHost(), serverTcpPort());

        client.connect()
                .thenCompose(v -> client.users().login(USERNAME, PASSWORD))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        client.streams().createStream(TEST_STREAM).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.topics()
                .createTopic(
                        STREAM_ID,
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        TEST_TOPIC)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        try {
            client.streams().deleteStream(STREAM_ID).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (RuntimeException e) {
            log.debug("Stream cleanup failed (may not exist): {}", e.getMessage());
        }
        if (client != null) {
            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    // ===== Error scenario tests =====

    @Test
    void shouldReturnEmptyForNonExistentGroup() throws Exception {
        // when
        Optional<ConsumerGroupDetails> result = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldFailToDeleteNonExistentGroup() {
        // when
        var future = client.consumerGroups().deleteConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L));

        // then
        assertThatThrownBy(() -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IggyResourceNotFoundException.class);
    }

    @Test
    void shouldFailToJoinNonExistentGroup() {
        // when
        var future = client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L));

        // then
        assertThatThrownBy(() -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IggyResourceNotFoundException.class);
    }

    @Test
    void shouldFailToLeaveGroupNotJoined() throws Exception {
        // given
        String groupName = "leave-not-joined-test-" + UUID.randomUUID();
        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // when
        var future = client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()));

        // then
        assertThatThrownBy(() -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isInstanceOf(IggyResourceNotFoundException.class);
    }

    @Test
    void shouldHandleMultipleClientsJoiningGroup() throws Exception {
        // given
        String groupName = "multi-client-test-" + UUID.randomUUID();
        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

        AsyncIggyTcpClient secondClient = new AsyncIggyTcpClient(serverHost(), serverTcpPort());
        try {
            secondClient
                    .connect()
                    .thenCompose(v -> secondClient.users().login(USERNAME, PASSWORD))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when
            client.consumerGroups()
                    .joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondClient
                    .consumerGroups()
                    .joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // then
            ConsumerGroupDetails group = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();

            assertThat(group.membersCount()).isEqualTo(2);
            assertThat(group.members()).hasSize(2);

            // cleanup — leave before closing
            client.consumerGroups()
                    .leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondClient
                    .consumerGroups()
                    .leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            secondClient.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldChainCreateAndGetWithThenCompose() throws Exception {
        // given
        String groupName = "chain-test-" + UUID.randomUUID();

        // when
        Optional<ConsumerGroupDetails> result = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .thenCompose(created ->
                        client.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id())))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // then
        assertThat(result).isPresent();
        assertThat(result.get().name()).isEqualTo(groupName);
        assertThat(result.get().membersCount()).isEqualTo(0);
    }

    @Test
    void shouldChainJoinVerifyLeaveVerify() throws Exception {
        // given
        String groupName = "sequential-chain-test-" + UUID.randomUUID();
        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

        // when
        ConsumerGroupDetails afterLeave = client.consumerGroups()
                .joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                .thenCompose(v -> client.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                .thenCompose(groupOpt -> {
                    assertThat(groupOpt).isPresent();
                    assertThat(groupOpt.get().membersCount()).isEqualTo(1);
                    return client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId);
                })
                .thenCompose(v -> client.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                .thenApply(groupOpt -> {
                    assertThat(groupOpt).isPresent();
                    return groupOpt.get();
                })
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // then
        assertThat(afterLeave.membersCount()).isEqualTo(0);
    }

    @Test
    void shouldHandleConcurrentGroupCreations() throws Exception {
        // given
        String streamName = "concurrent-create-stream-" + UUID.randomUUID();
        String topicName = "concurrent-create-topic";
        StreamId streamId = StreamId.of(streamName);
        TopicId topicId = TopicId.of(topicName);

        try {
            client.streams().createStream(streamName).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.topics()
                    .createTopic(
                            streamId,
                            1L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            Optional.empty(),
                            topicName)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when
            List<CompletableFuture<ConsumerGroupDetails>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                futures.add(client.consumerGroups().createConsumerGroup(streamId, topicId, "concurrent-group-" + i));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

            // then
            for (CompletableFuture<ConsumerGroupDetails> f : futures) {
                assertThat(f.isDone()).isTrue();
                assertThat(f.isCompletedExceptionally()).isFalse();
            }

            List<ConsumerGroup> groups =
                    client.consumerGroups().getConsumerGroups(streamId, topicId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(groups).hasSize(5);
            assertThat(groups)
                    .map(ConsumerGroup::name)
                    .containsExactlyInAnyOrder(
                            "concurrent-group-0",
                            "concurrent-group-1",
                            "concurrent-group-2",
                            "concurrent-group-3",
                            "concurrent-group-4");
        } finally {
            client.streams().deleteStream(streamId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldHandleConcurrentJoinAndLeaveOperations() throws Exception {
        // given
        String groupName = "concurrent-join-leave-test-" + UUID.randomUUID();
        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

        AsyncIggyTcpClient secondClient = new AsyncIggyTcpClient(serverHost(), serverTcpPort());
        AsyncIggyTcpClient thirdClient = new AsyncIggyTcpClient(serverHost(), serverTcpPort());
        try {
            secondClient
                    .connect()
                    .thenCompose(v -> secondClient.users().login(USERNAME, PASSWORD))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            thirdClient
                    .connect()
                    .thenCompose(v -> thirdClient.users().login(USERNAME, PASSWORD))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when — join concurrently
            CompletableFuture.allOf(
                            client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            secondClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            thirdClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

            // then
            ConsumerGroupDetails afterJoin = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();
            assertThat(afterJoin.membersCount()).isEqualTo(3);

            // when — leave concurrently
            CompletableFuture.allOf(
                            client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            secondClient.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            thirdClient.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

            // then
            ConsumerGroupDetails afterLeave = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();
            assertThat(afterLeave.membersCount()).isEqualTo(0);
        } finally {
            secondClient.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            thirdClient.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
