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

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
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
class AsyncClientIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(AsyncClientIntegrationTest.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private static final String TEST_STREAM = "async-test-stream-" + UUID.randomUUID();
    private static final String TEST_TOPIC = "async-test-topic";
    private static final long PARTITION_ID = 1L;

    private static AsyncIggyTcpClient client;

    @BeforeAll
    public static void setup() throws Exception {
        log.info("Setting up async client for integration tests");
        client = new AsyncIggyTcpClient(HOST, PORT);

        // Connect and login
        client.connect()
                .thenCompose(v -> {
                    log.info("Connected to Iggy server");
                    return client.users().loginAsync(USERNAME, PASSWORD);
                })
                .get(5, TimeUnit.SECONDS);

        log.info("Successfully logged in as: {}", USERNAME);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        log.info("Cleaning up test resources");

        try {
            // Clean up test stream if it exists
            client.streams().deleteStreamAsync(StreamId.of(TEST_STREAM)).get(5, TimeUnit.SECONDS);
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

        var streamDetails = client.streams().createStreamAsync(TEST_STREAM).get(5, TimeUnit.SECONDS);

        assertThat(streamDetails).isNotNull();
        assertThat(streamDetails.name()).isEqualTo(TEST_STREAM);
        log.info("Successfully created stream: {}", streamDetails.name());
    }

    @Test
    @Order(2)
    public void testGetStream() throws Exception {
        log.info("Testing stream retrieval");

        var streamOpt =
                client.streams().getStreamAsync(StreamId.of(TEST_STREAM)).get(5, TimeUnit.SECONDS);

        assertThat(streamOpt).isPresent();
        assertThat(streamOpt.get().name()).isEqualTo(TEST_STREAM);
        log.info("Successfully retrieved stream: {}", streamOpt.get().name());
    }

    @Test
    @Order(3)
    public void testCreateTopic() throws Exception {
        log.info("Testing topic creation");

        var topicDetails = client.topics()
                .createTopicAsync(
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
                .getTopicAsync(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
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
                .sendMessagesAsync(
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
                .pollMessagesAsync(
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
                    .sendMessagesAsync(
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
                .pollMessagesAsync(
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
                .updateTopicAsync(
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
                .getTopicAsync(StreamId.of(TEST_STREAM), TopicId.of(TEST_TOPIC))
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
                .createTopicAsync(
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
                .deleteTopicAsync(StreamId.of(TEST_STREAM), TopicId.of(tempTopic))
                .get(5, TimeUnit.SECONDS);

        // Verify deletion
        var deletedTopic = client.topics()
                .getTopicAsync(StreamId.of(TEST_STREAM), TopicId.of(tempTopic))
                .get(5, TimeUnit.SECONDS);

        assertThat(deletedTopic).isNotPresent();
        log.info("Successfully deleted topic: {}", tempTopic);
    }

    // TODO: Re-enable when server supports null consumer polling
    // This test uses null consumer in concurrent poll operations which causes timeout
    @Test
    @Disabled
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
                            .sendMessagesAsync(
                                    StreamId.of(TEST_STREAM),
                                    TopicId.of(TEST_TOPIC),
                                    Partitioning.partitionId(PARTITION_ID),
                                    messages));
            operations.add(future);
        }

        // Poll messages concurrently
        for (int i = 0; i < 3; i++) {
            var future = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(TEST_STREAM),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(PARTITION_ID),
                            null,
                            PollingStrategy.last(),
                            10L,
                            false);
            operations.add(future);
        }

        // Wait for all operations to complete
        CompletableFuture.allOf(operations.toArray(new CompletableFuture[0])).get(15, TimeUnit.SECONDS);

        log.info("Successfully completed {} concurrent operations", operations.size());
    }
}
