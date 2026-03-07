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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Dedicated async-specific tests for {@link ConsumerGroupsClient} via
 * {@link org.apache.iggy.client.async.tcp.ConsumerGroupsTcpClient}.
 *
 * <p>Covers all CRUD operations, join/leave membership, error scenarios, and
 * CompletableFuture-specific patterns (chaining, concurrency, exception propagation).
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
                .thenCompose(v -> {
                    log.info("Connected to Iggy server");
                    return client.users().login(USERNAME, PASSWORD);
                })
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

        log.info("Created stream '{}' and topic '{}'", TEST_STREAM, TEST_TOPIC);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        log.info("Cleaning up async consumer groups test resources");
        try {
            client.streams().deleteStream(STREAM_ID).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Deleted test stream: {}", TEST_STREAM);
        } catch (RuntimeException e) {
            log.debug("Stream cleanup failed (may not exist): {}", e.getMessage());
        }
        if (client != null) {
            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info("Closed async client");
        }
    }

    // ===== Happy path tests =====

    @Test
    void shouldCreateConsumerGroupAsync() throws Exception {
        String groupName = "create-test-" + UUID.randomUUID();

        ConsumerGroupDetails group = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(group).isNotNull();
        assertThat(group.id()).isNotNull();
        assertThat(group.name()).isEqualTo(groupName);
        assertThat(group.membersCount()).isEqualTo(0);
        assertThat(group.members()).isEmpty();
    }

    @Test
    void shouldGetConsumerGroupByIdAsync() throws Exception {
        String groupName = "get-by-id-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Optional<ConsumerGroupDetails> retrieved = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().id()).isEqualTo(created.id());
        assertThat(retrieved.get().name()).isEqualTo(groupName);
    }

    @Test
    void shouldGetConsumerGroupByNameAsync() throws Exception {
        String groupName = "get-by-name-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Optional<ConsumerGroupDetails> byId = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Optional<ConsumerGroupDetails> byName = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(groupName))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(byName).isPresent();
        assertThat(byName.get().name()).isEqualTo(groupName);
        assertThat(byId).isEqualTo(byName);
    }

    @Test
    void shouldListAllConsumerGroupsAsync() throws Exception {
        String streamName = "list-all-stream-" + UUID.randomUUID();
        String topicName = "list-all-topic";
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

            client.consumerGroups()
                    .createConsumerGroup(streamId, topicId, "group-a")
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.consumerGroups()
                    .createConsumerGroup(streamId, topicId, "group-b")
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.consumerGroups()
                    .createConsumerGroup(streamId, topicId, "group-c")
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            List<ConsumerGroup> groups =
                    client.consumerGroups().getConsumerGroups(streamId, topicId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(groups).hasSize(3);
            assertThat(groups).map(ConsumerGroup::name).containsExactlyInAnyOrder("group-a", "group-b", "group-c");
        } finally {
            client.streams().deleteStream(streamId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldDeleteConsumerGroupAsync() throws Exception {
        String groupName = "delete-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        client.consumerGroups()
                .deleteConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Optional<ConsumerGroupDetails> deleted = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(deleted).isEmpty();
    }

    @Test
    void shouldDeleteConsumerGroupByNameAsync() throws Exception {
        String groupName = "delete-by-name-test-" + UUID.randomUUID();

        client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        client.consumerGroups()
                .deleteConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(groupName))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        Optional<ConsumerGroupDetails> deleted = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(groupName))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(deleted).isEmpty();
    }

    // ===== Join/Leave tests =====

    @Test
    void shouldJoinConsumerGroupAsync() throws Exception {
        String groupName = "join-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

        client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ConsumerGroupDetails group = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get();

        assertThat(group.membersCount()).isEqualTo(1);
        assertThat(group.members()).hasSize(1);
        assertThat(group.members().getFirst().partitionsCount()).isGreaterThan(0);

        client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void shouldLeaveConsumerGroupAsync() throws Exception {
        String groupName = "leave-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

        client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ConsumerGroupDetails group = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get();

        assertThat(group.membersCount()).isEqualTo(0);
        assertThat(group.members()).isEmpty();
    }

    @Test
    void shouldJoinAndLeaveConsumerGroupSequentiallyAsync() throws Exception {
        String groupName = "sequential-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ConsumerId groupId = ConsumerId.of(created.id());

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

        assertThat(afterLeave.membersCount()).isEqualTo(0);
    }

    @Test
    void shouldHandleMultipleClientsJoiningGroupAsync() throws Exception {
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

            client.consumerGroups()
                    .joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondClient
                    .consumerGroups()
                    .joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            ConsumerGroupDetails group = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();

            assertThat(group.membersCount()).isEqualTo(2);
            assertThat(group.members()).hasSize(2);

            client.consumerGroups()
                    .leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            secondClient
                    .consumerGroups()
                    .leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            ConsumerGroupDetails afterLeave = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();

            assertThat(afterLeave.membersCount()).isEqualTo(0);
        } finally {
            secondClient.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    // ===== Error scenario tests =====

    @Test
    void shouldReturnEmptyForNonExistentGroup() throws Exception {
        Optional<ConsumerGroupDetails> result = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldFailToDeleteNonExistentGroup() {
        var future = client.consumerGroups().deleteConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L));

        var exception = assertThrows(ExecutionException.class, () -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause()).isInstanceOf(IggyResourceNotFoundException.class);
    }

    @Test
    void shouldFailToJoinNonExistentGroup() {
        var future = client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(999_999L));

        var exception = assertThrows(ExecutionException.class, () -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause()).isInstanceOf(IggyResourceNotFoundException.class);
    }

    @Test
    void shouldFailToLeaveGroupNotJoined() throws Exception {
        String groupName = "leave-not-joined-test-" + UUID.randomUUID();

        ConsumerGroupDetails created = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        var future = client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id()));

        var exception = assertThrows(ExecutionException.class, () -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause()).isInstanceOf(IggyResourceNotFoundException.class);
    }

    // ===== CompletableFuture-specific tests =====

    @Test
    void shouldChainCreateAndGetWithThenCompose() throws Exception {
        String groupName = "chain-test-" + UUID.randomUUID();

        Optional<ConsumerGroupDetails> result = client.consumerGroups()
                .createConsumerGroup(STREAM_ID, TOPIC_ID, groupName)
                .thenCompose(created ->
                        client.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, ConsumerId.of(created.id())))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(result).isPresent();
        assertThat(result.get().name()).isEqualTo(groupName);
        assertThat(result.get().membersCount()).isEqualTo(0);
    }

    @Test
    void shouldHandleConcurrentGroupCreations() throws Exception {
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

            List<CompletableFuture<ConsumerGroupDetails>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                futures.add(client.consumerGroups().createConsumerGroup(streamId, topicId, "concurrent-group-" + i));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

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

            CompletableFuture.allOf(
                            client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            secondClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            thirdClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

            ConsumerGroupDetails afterJoin = client.consumerGroups()
                    .getConsumerGroup(STREAM_ID, TOPIC_ID, groupId)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get();

            assertThat(afterJoin.membersCount()).isEqualTo(3);

            CompletableFuture.allOf(
                            client.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            secondClient.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId),
                            thirdClient.consumerGroups().leaveConsumerGroup(STREAM_ID, TOPIC_ID, groupId))
                    .get(TIMEOUT_SECONDS * 2, TimeUnit.SECONDS);

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
