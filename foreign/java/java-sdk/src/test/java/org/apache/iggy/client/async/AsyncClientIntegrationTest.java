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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the complete async client flow.
 * Tests connection, authentication, stream/topic management, and message operations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AsyncClientIntegrationTest extends BaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AsyncClientIntegrationTest.class);

    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private static final String TEST_STREAM = "async-test-stream-" + UUID.randomUUID();
    private static final String TEST_TOPIC = "async-test-topic";
    private static final long PARTITION_ID = 1L;

    private static AsyncIggyTcpClient client;

    @BeforeAll
    public static void setup() throws Exception {
        log.info("Setting up async client for integration tests");
        client = new AsyncIggyTcpClient(serverHost(), serverTcpPort());

        // Connect and login
        client.connect()
                .thenCompose(v -> {
                    log.info("Connected to Iggy server");
                    return client.users().login(USERNAME, PASSWORD);
                })
                .get(5, TimeUnit.SECONDS);

        log.info("Successfully logged in as: {}", USERNAME);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        log.info("Cleaning up test resources");

        try {
            // Clean up test stream if it exists
            client.streams().deleteStream(StreamId.of(TEST_STREAM)).get(5, TimeUnit.SECONDS);
            log.info("Deleted test stream: {}", TEST_STREAM);
        } catch (RuntimeException e) {
            // Stream may not exist, which is fine
            log.debug("Stream cleanup failed (may not exist): {}", e.getMessage());
        }

        // Close the client
        if (client != null) {
            client.close().get(5, TimeUnit.SECONDS);
            log.info("Closed async client");
        }
    }

    @Test
    @Order(1)
    public void testCreateStream() throws Exception {
        log.info("Testing stream creation");

        var streamDetails = client.streams().createStream(TEST_STREAM).get(5, TimeUnit.SECONDS);

        assertThat(streamDetails).isNotNull();
        assertThat(streamDetails.name()).isEqualTo(TEST_STREAM);
        log.info("Successfully created stream: {}", streamDetails.name());
    }

    @Test
    @Order(2)
    public void testGetStream() throws Exception {
        log.info("Testing stream retrieval");

        var streamOpt = client.streams().getStream(StreamId.of(TEST_STREAM)).get(5, TimeUnit.SECONDS);

        assertThat(streamOpt).isPresent();
        assertThat(streamOpt.get().name()).isEqualTo(TEST_STREAM);
        log.info("Successfully retrieved stream: {}", streamOpt.get().name());
    }

    @Test
    @Order(3)
    public void testCreateTopic() throws Exception {
        log.info("Testing topic creation");

        var topicDetails = client.topics()
                .createTopic(
                        StreamId.of(TEST_STREAM),
                        2L, // 2 partitions
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        TEST_TOPIC)
                .get(5, TimeUnit.SECONDS);

        assertThat(topicDetails).isNotNull();
        assertThat(topicDetails.name()).isEqualTo(TEST_TOPIC);
        assertThat(topicDetails.partitionsCount()).isEqualTo(2);
        log.info(
                "Successfully created topic: {} with {} partitions",
                topicDetails.name(),
                topicDetails.partitionsCount());
    }

    @Test
    @Order(4)
    public void testGetTopic() throws Exception {
        log.info("Testing topic retrieval");

        var topicOpt = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);

        assertThat(topicOpt).isPresent();
        assertThat(topicOpt.get().name()).isEqualTo(TEST_TOPIC);
        log.info("Successfully retrieved topic: {}", topicOpt.get().name());
    }

    @Test
    @Order(5)
    public void testSendMessages() throws Exception {
        log.info("Testing message sending");

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String content = String.format("Test message %d - %s", i, UUID.randomUUID());
            messages.add(Message.of(content));
        }

        // Send messages to partition 1
        client.messages()
                .sendMessages(
                        StreamId.of(TEST_STREAM),
                        TopicId.of(TEST_TOPIC),
                        Partitioning.partitionId(PARTITION_ID),
                        messages)
                .get(5, TimeUnit.SECONDS);

        log.info("Successfully sent {} messages", messages.size());
    }

    @Test
    @Order(6)
    public void testPollMessages() throws Exception {
        log.info("Testing message polling");

        // Poll messages from partition 1 - Use valid consumer instead of null
        var consumer = Consumer.of(12345L); // Create consumer with ID
        var polledMessages = client.messages()
                .pollMessages(
                        StreamId.of(TEST_STREAM),
                        TopicId.of(TEST_TOPIC),
                        Optional.of(PARTITION_ID),
                        consumer, // Use valid consumer instead of null
                        PollingStrategy.offset(BigInteger.ZERO),
                        10L,
                        false)
                .get(5, TimeUnit.SECONDS);

        assertThat(polledMessages).isNotNull();
        assertThat(polledMessages.partitionId()).isEqualTo(PARTITION_ID);
        assertThat(polledMessages.messages()).isNotEmpty();
        log.info(
                "Successfully polled {} messages from partition {}",
                polledMessages.messages().size(),
                polledMessages.partitionId());

        // Verify message content
        for (var message : polledMessages.messages()) {
            String content = new String(message.payload());
            assertThat(content).startsWith("Test message");
            log.debug("Polled message: {}", content);
        }
    }

    // TODO: Re-enable when server supports null consumer polling
    // This test fails because it uses null consumer which causes server timeout
    @Test
    @Disabled
    @Order(7)
    public void testSendAndPollLargeVolume() throws Exception {
        log.info("Testing high-volume message operations");

        int messageCount = 100;
        List<CompletableFuture<Void>> sendFutures = new ArrayList<>();

        // Send messages in batches asynchronously
        for (int batch = 0; batch < 10; batch++) {
            List<Message> batchMessages = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                int msgNum = batch * 10 + i;
                String content = String.format("Batch message %d - %s", msgNum, System.currentTimeMillis());
                batchMessages.add(Message.of(content));
            }

            var future = client.messages()
                    .sendMessages(
                            StreamId.of(TEST_STREAM),
                            TopicId.of(TEST_TOPIC),
                            Partitioning.partitionId(PARTITION_ID),
                            batchMessages);
            sendFutures.add(future);
        }

        // Wait for all sends to complete
        CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

        log.info("Successfully sent {} messages in batches", messageCount);

        // Poll all messages
        var polledMessages = client.messages()
                .pollMessages(
                        StreamId.of(TEST_STREAM),
                        TopicId.of(TEST_TOPIC),
                        Optional.of(PARTITION_ID),
                        null,
                        PollingStrategy.offset(BigInteger.ZERO),
                        (long) messageCount + 10, // Poll all messages sent
                        false)
                .get(5, TimeUnit.SECONDS);

        assertThat(polledMessages).isNotNull();
        assertThat(polledMessages.messages().size()).isGreaterThanOrEqualTo(messageCount);
        log.info("Successfully polled {} messages", polledMessages.messages().size());
    }

    // TODO: This test fails with connection issues after null consumer timeout
    // The connection gets closed after the previous test's null consumer timeout
    @Test
    @Disabled
    @Order(8)
    public void testUpdateTopic() throws Exception {
        log.info("Testing topic update");

        // Update topic with new compression algorithm
        client.topics()
                .updateTopic(
                        StreamId.of(TEST_STREAM),
                        TopicId.of(TEST_TOPIC),
                        CompressionAlgorithm.Gzip,
                        BigInteger.valueOf(3600000000L), // 1 hour message expiry
                        BigInteger.valueOf(1073741824L), // 1GB max size
                        Optional.empty(),
                        TEST_TOPIC)
                .get(5, TimeUnit.SECONDS);

        // Verify the update
        var updatedTopic = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);

        assertThat(updatedTopic).isPresent();
        assertThat(updatedTopic.get().compressionAlgorithm()).isEqualTo(CompressionAlgorithm.Gzip);
        log.info(
                "Successfully updated topic with compression: {}",
                updatedTopic.get().compressionAlgorithm());
    }

    // TODO: This test fails with connection issues after null consumer timeout
    // The connection gets closed after previous tests' null consumer timeout
    @Test
    @Disabled
    @Order(9)
    public void testDeleteTopic() throws Exception {
        log.info("Testing topic deletion");

        // Create a temporary topic to delete
        String tempTopic = "temp-topic-" + UUID.randomUUID();
        client.topics()
                .createTopic(
                        StreamId.of(TEST_STREAM),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        tempTopic)
                .get(5, TimeUnit.SECONDS);

        // Delete the topic
        client.topics()
                .deleteTopic(StreamId.of(TEST_STREAM), TopicId.of(tempTopic))
                .get(5, TimeUnit.SECONDS);

        // Verify deletion
        var deletedTopic = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(tempTopic))
                .get(5, TimeUnit.SECONDS);

        assertThat(deletedTopic).isNotPresent();
        log.info("Successfully deleted topic: {}", tempTopic);
    }

    @Test
    @Order(10)
    void testConcurrentOperations() throws Exception {
        log.info("Testing concurrent async operations");

        // Create multiple concurrent operations
        List<CompletableFuture<?>> operations = new ArrayList<>();

        // Send messages concurrently
        for (int i = 0; i < 5; i++) {
            final int threadNum = i;
            var future = CompletableFuture.supplyAsync(() -> {
                        List<Message> messages = new ArrayList<>();
                        for (int j = 0; j < 20; j++) {
                            String content = String.format("Thread %d - Message %d", threadNum, j);
                            messages.add(Message.of(content));
                        }
                        return messages;
                    })
                    .thenCompose(messages -> client.messages()
                            .sendMessages(
                                    StreamId.of(TEST_STREAM),
                                    TopicId.of(TEST_TOPIC),
                                    Partitioning.partitionId(PARTITION_ID),
                                    messages));
            operations.add(future);
        }

        // Poll messages concurrently using valid consumer
        for (int i = 0; i < 3; i++) {
            final long consumerId = 10000L + i;
            var future = client.messages()
                    .pollMessages(
                            StreamId.of(TEST_STREAM),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(PARTITION_ID),
                            Consumer.of(consumerId),
                            PollingStrategy.last(),
                            10L,
                            false);
            operations.add(future);
        }

        // Wait for all operations to complete
        CompletableFuture.allOf(operations.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);

        log.info("Successfully completed {} concurrent operations", operations.size());
    }

    // ===== System client tests =====

    @Test
    @Order(11)
    public void testGetStats() throws Exception {
        log.info("Testing system getStats");

        var stats = client.system().getStats().get(5, TimeUnit.SECONDS);

        assertThat(stats).isNotNull();
        log.info("Successfully retrieved server stats");
    }

    @Test
    @Order(12)
    public void testGetMe() throws Exception {
        log.info("Testing system getMe");

        var me = client.system().getMe().get(5, TimeUnit.SECONDS);

        assertThat(me).isNotNull();
        assertThat(me.clientId()).isGreaterThan(0);
        log.info("Successfully retrieved current client info, clientId: {}", me.clientId());
    }

    @Test
    @Order(13)
    public void testGetClients() throws Exception {
        log.info("Testing system getClients");

        var clients = client.system().getClients().get(5, TimeUnit.SECONDS);

        assertThat(clients).isNotNull();
        assertThat(clients).isNotEmpty();
        log.info("Successfully retrieved {} connected clients", clients.size());
    }

    @Test
    @Order(14)
    public void testGetClient() throws Exception {
        log.info("Testing system getClient");

        var me = client.system().getMe().get(5, TimeUnit.SECONDS);
        var clientInfo = client.system().getClient(me.clientId()).get(5, TimeUnit.SECONDS);

        assertThat(clientInfo).isNotNull();
        assertThat(clientInfo.clientId()).isEqualTo(me.clientId());
        log.info("Successfully retrieved client info for clientId: {}", clientInfo.clientId());
    }

    // ===== Consumer groups client tests =====

    @Test
    @Order(20)
    public void testCreateAndGetConsumerGroup() throws Exception {
        log.info("Testing consumer group create and get");

        String groupName = "async-test-group";
        var group = client.consumerGroups()
                .createConsumerGroup(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), groupName)
                .get(5, TimeUnit.SECONDS);

        assertThat(group).isNotNull();
        assertThat(group.name()).isEqualTo(groupName);
        log.info("Successfully created consumer group: {}", group.name());

        // Get by ID
        var retrieved = client.consumerGroups()
                .getConsumerGroup(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), ConsumerId.of(group.id()))
                .get(5, TimeUnit.SECONDS);

        assertThat(retrieved).isPresent();
        assertThat(retrieved.get().id()).isEqualTo(group.id());
        log.info("Successfully retrieved consumer group by ID: {}", group.id());
    }

    @Test
    @Order(21)
    public void testGetConsumerGroups() throws Exception {
        log.info("Testing consumer groups list");

        var groups = client.consumerGroups()
                .getConsumerGroups(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);

        assertThat(groups).isNotNull();
        assertThat(groups).isNotEmpty();
        log.info("Successfully retrieved {} consumer groups", groups.size());
    }

    @Test
    @Order(22)
    public void testDeleteConsumerGroup() throws Exception {
        log.info("Testing consumer group deletion");

        String groupName = "async-delete-group";
        var group = client.consumerGroups()
                .createConsumerGroup(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), groupName)
                .get(5, TimeUnit.SECONDS);

        client.consumerGroups()
                .deleteConsumerGroup(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), ConsumerId.of(group.id()))
                .get(5, TimeUnit.SECONDS);

        var deleted = client.consumerGroups()
                .getConsumerGroup(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), ConsumerId.of(group.id()))
                .get(5, TimeUnit.SECONDS);

        assertThat(deleted).isEmpty();
        log.info("Successfully deleted consumer group: {}", groupName);
    }

    // ===== Consumer offsets client tests =====

    @Test
    @Order(30)
    public void testStoreAndGetConsumerOffset() throws Exception {
        log.info("Testing consumer offset store and get");

        // Send message to partition 0 to ensure it is not empty so we can store offset 0
        // Note: storeConsumerOffset with empty partitionId defaults to partition 0 on server
        client.messages()
                .sendMessages(
                        StreamId.of(TEST_STREAM),
                        TopicId.of(TEST_TOPIC),
                        Partitioning.partitionId(0L),
                        List.of(Message.of("test")))
                .get(5, TimeUnit.SECONDS);

        var consumer = new Consumer(Consumer.Kind.Consumer, ConsumerId.of(5000L));
        var offset = BigInteger.valueOf(0);

        client.consumerOffsets()
                .storeConsumerOffset(
                        StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), Optional.empty(), consumer, offset)
                .get(5, TimeUnit.SECONDS);

        log.info("Successfully stored consumer offset: {}", offset);

        var retrieved = client.consumerOffsets()
                .getConsumerOffset(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), Optional.of(0L), consumer)
                .get(5, TimeUnit.SECONDS);

        assertThat(retrieved).isPresent();
        log.info("Successfully retrieved consumer offset");
    }

    // ===== Partitions client tests =====

    @Test
    @Order(40)
    public void testCreateAndDeletePartitions() throws Exception {
        log.info("Testing partition create and delete");

        // Get initial partition count
        var topicBefore = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);
        assertThat(topicBefore).isPresent();
        long initialCount = topicBefore.get().partitionsCount();
        log.info("Initial partition count: {}", initialCount);

        // Create additional partitions
        client.partitions()
                .createPartitions(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), 3L)
                .get(5, TimeUnit.SECONDS);

        var topicAfterCreate = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);
        assertThat(topicAfterCreate).isPresent();
        assertThat(topicAfterCreate.get().partitionsCount()).isEqualTo(initialCount + 3);
        log.info("Partition count after create: {}", topicAfterCreate.get().partitionsCount());

        // Delete the added partitions
        client.partitions()
                .deletePartitions(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC), 3L)
                .get(5, TimeUnit.SECONDS);

        var topicAfterDelete = client.topics()
                .getTopic(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
                .get(5, TimeUnit.SECONDS);
        assertThat(topicAfterDelete).isPresent();
        assertThat(topicAfterDelete.get().partitionsCount()).isEqualTo(initialCount);
        log.info("Partition count after delete: {}", topicAfterDelete.get().partitionsCount());
    }

    // ===== Personal access tokens client tests =====

    @Test
    @Order(50)
    public void testCreateAndDeletePersonalAccessToken() throws Exception {
        log.info("Testing personal access token create and delete");

        String tokenName = "async-test-token";
        var token = client.personalAccessTokens()
                .createPersonalAccessToken(tokenName, BigInteger.valueOf(50_000))
                .get(5, TimeUnit.SECONDS);

        assertThat(token).isNotNull();
        log.info("Successfully created personal access token: {}", tokenName);

        // List tokens
        var tokens = client.personalAccessTokens().getPersonalAccessTokens().get(5, TimeUnit.SECONDS);

        assertThat(tokens).isNotNull();
        assertThat(tokens).anyMatch(t -> t.name().equals(tokenName));
        log.info("Found {} personal access tokens", tokens.size());

        // Delete token
        client.personalAccessTokens().deletePersonalAccessToken(tokenName).get(5, TimeUnit.SECONDS);

        var tokensAfterDelete =
                client.personalAccessTokens().getPersonalAccessTokens().get(5, TimeUnit.SECONDS);

        assertThat(tokensAfterDelete).noneMatch(t -> t.name().equals(tokenName));
        log.info("Successfully deleted personal access token: {}", tokenName);
    }

    @Test
    @Order(51)
    public void testLoginWithPersonalAccessToken() throws Exception {
        log.info("Testing login with personal access token");

        String tokenName = "async-login-token";
        var token = client.personalAccessTokens()
                .createPersonalAccessToken(tokenName, BigInteger.valueOf(50_000))
                .get(5, TimeUnit.SECONDS);

        assertThat(token).isNotNull();
        assertThat(token.token()).isNotEmpty();
        log.info("Created token for login test");

        // Login with PAT using a separate client
        var patClient = new AsyncIggyTcpClient(serverHost(), serverTcpPort());
        try {
            patClient.connect().get(5, TimeUnit.SECONDS);
            var identity = patClient
                    .personalAccessTokens()
                    .loginWithPersonalAccessToken(token.token())
                    .get(5, TimeUnit.SECONDS);

            assertThat(identity).isNotNull();
            log.info("Successfully logged in with personal access token");
        } finally {
            patClient.close().get(5, TimeUnit.SECONDS);
            // Clean up token
            client.personalAccessTokens().deletePersonalAccessToken(tokenName).get(5, TimeUnit.SECONDS);
        }
    }
}
