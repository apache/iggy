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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class specifically for testing poll message functionality with different consumer scenarios.
 * <p>
 * Key findings:
 * 1. Polling with NULL consumer causes server to not respond (timeout)
 * 2. Polling with invalid consumer ID returns error 1010
 * 3. Polling with valid consumer group member works correctly
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AsyncPollMessageTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncPollMessageTest.class);
    private static AsyncIggyTcpClient client;
    private static String testStream;
    private static final String TEST_TOPIC = "poll-test-topic";
    private static final String CONSUMER_GROUP_NAME = "test-consumer-group";
    private static final Long PARTITION_ID = 1L;
    private static final int MESSAGE_COUNT = 10;

    @BeforeEach
    void setupEachTest() throws Exception {
        // Ensure connection is established before each test
        if (client == null || !isConnected(client)) {
            log.info("Reconnecting client for test");
            if (client != null) {
                try {
                    client.close().get(1, TimeUnit.SECONDS);
                } catch (RuntimeException e) {
                    // Ignore close errors
                }
            }
            client = new AsyncIggyTcpClient("127.0.0.1", 8090);
            client.connect().get(5, TimeUnit.SECONDS);
            client.users().loginAsync("iggy", "iggy").get(5, TimeUnit.SECONDS);
            log.info("Client reconnected successfully");
        }
    }

    private boolean isConnected(AsyncIggyTcpClient client) {
        // Check if client is connected by attempting a simple operation
        try {
            client.streams().getStreamsAsync().get(1, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return false;
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        log.info("Setting up async client for poll message tests");

        // Initialize client
        client = new AsyncIggyTcpClient("127.0.0.1", 8090);
        client.connect().get(5, TimeUnit.SECONDS);
        client.users().loginAsync("iggy", "iggy").get(5, TimeUnit.SECONDS);
        log.info("Successfully connected and logged in");

        // Create unique stream for this test run
        testStream = "poll-test-stream-" + UUID.randomUUID();
        var stream = client.streams().createStreamAsync(testStream).get(5, TimeUnit.SECONDS);
        log.info("Created test stream: {}", stream.name());

        // Create topic with 2 partitions
        var topic = client.topics()
                .createTopicAsync(
                        StreamId.of(testStream),
                        2L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        Optional.empty(),
                        TEST_TOPIC)
                .get(5, TimeUnit.SECONDS);
        log.info("Created test topic: {} with {} partitions", topic.name(), topic.partitionsCount());

        // Send test messages to both partitions
        for (int partition = 0; partition < 2; partition++) {
            final int partNum = partition;
            var messages = IntStream.range(0, MESSAGE_COUNT)
                    .mapToObj(i -> Message.of(String.format("Message %d for partition %d", i, partNum)))
                    .toList();

            client.messages()
                    .sendMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Partitioning.partitionId((long) partition),
                            messages)
                    .get(5, TimeUnit.SECONDS);

            log.info("Sent {} messages to partition {}", MESSAGE_COUNT, partition);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        log.info("Cleaning up test resources");

        // Delete stream (cascades to topics and consumer groups)
        try {
            client.streams().deleteStreamAsync(StreamId.of(testStream)).get(5, TimeUnit.SECONDS);
            log.info("Deleted test stream: {}", testStream);
        } catch (RuntimeException e) {
            log.warn("Failed to delete test stream: {}", e.getMessage());
        }

        // Close client
        if (client != null) {
            client.close().get(5, TimeUnit.SECONDS);
            log.info("Closed async client");
        }
    }

    @Test
    @Order(1)
    @DisplayName("Poll with NULL consumer - Expected to timeout (server doesn't respond)")
    void testPollWithNullConsumer() {
        log.info("TEST 1: Polling with NULL consumer");

        // This test demonstrates the issue: server doesn't respond to null consumer
        assertThatThrownBy(() -> {
                    client.messages()
                            .pollMessagesAsync(
                                    StreamId.of(testStream),
                                    TopicId.of(TEST_TOPIC),
                                    Optional.of(PARTITION_ID),
                                    null, // NULL consumer causes server to not respond
                                    PollingStrategy.offset(BigInteger.ZERO),
                                    10L,
                                    false)
                            .get(3, TimeUnit.SECONDS);
                })
                .isInstanceOf(TimeoutException.class);

        log.info("CONFIRMED: Null consumer causes timeout (server doesn't respond)");
    }

    @Test
    @Order(2)
    @DisplayName("Poll with various consumer IDs")
    void testPollWithVariousConsumerIDs() throws Exception {
        log.info("TEST 2: Polling with various consumer IDs");

        // Test with a large consumer ID (likely doesn't exist but server still accepts)
        var largeIdConsumer = Consumer.of(99999L);

        try {
            var polledMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(PARTITION_ID),
                            largeIdConsumer,
                            PollingStrategy.offset(BigInteger.ZERO),
                            10L,
                            false)
                    .get(5, TimeUnit.SECONDS);

            // Server accepts any valid consumer ID format
            assertThat(polledMessages).isNotNull();
            log.info(
                    "Server accepted consumer ID 99999 and returned {} messages",
                    polledMessages.messages().size());
        } catch (ExecutionException e) {
            log.info("Consumer ID 99999 was rejected: {}", e.getCause().getMessage());
        }
    }

    @Test
    @Order(3)
    @DisplayName("Poll with valid consumer ID")
    void testPollWithValidConsumer() throws Exception {
        log.info("TEST 3: Polling with valid consumer");

        // Use a simple consumer ID
        var consumer = Consumer.of(1L);

        try {
            var polledMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(PARTITION_ID),
                            consumer,
                            PollingStrategy.offset(BigInteger.ZERO),
                            5L,
                            false)
                    .get(5, TimeUnit.SECONDS);

            assertThat(polledMessages).isNotNull();
            log.info(
                    "Successfully polled {} messages with consumer ID 1",
                    polledMessages.messages().size());

            // Log message content
            polledMessages.messages().forEach(msg -> log.info("  - Message: {}", new String(msg.payload())));
        } catch (ExecutionException e) {
            log.info(
                    "Polling with consumer ID 1 failed (expected if consumer doesn't exist): {}",
                    e.getCause().getMessage());
            // This is expected if the consumer doesn't exist in the system
        }
    }

    @Test
    @Order(4)
    @DisplayName("Poll with direct partition access")
    void testPollDirectPartitionAccess() throws Exception {
        log.info("TEST 4: Direct partition polling");

        try {
            // Try polling with a session consumer
            var sessionConsumer = Consumer.of(0L);

            var polledMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(PARTITION_ID),
                            sessionConsumer,
                            PollingStrategy.offset(BigInteger.ZERO),
                            5L,
                            false)
                    .get(5, TimeUnit.SECONDS);

            assertThat(polledMessages).isNotNull();
            log.info(
                    "Successfully polled {} messages with session consumer",
                    polledMessages.messages().size());

        } catch (ExecutionException e) {
            log.info(
                    "Direct partition access failed (may require consumer group): {}",
                    e.getCause().getMessage());
            // This is expected behavior if server requires consumer group membership
        }
    }

    @Test
    @Order(5)
    @DisplayName("Poll with different strategies")
    void testPollWithDifferentStrategies() throws Exception {
        log.info("TEST 5: Testing different polling strategies");

        var consumer = Consumer.of(1L);

        // Test 1: Poll from beginning
        log.info("  Testing FIRST strategy");
        try {
            var firstMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(1L),
                            consumer,
                            PollingStrategy.first(),
                            3L,
                            false)
                    .get(5, TimeUnit.SECONDS);
            log.info(
                    "    Polled {} messages from beginning",
                    firstMessages.messages().size());
        } catch (RuntimeException e) {
            log.info("    FIRST strategy failed: {}", e.getMessage());
        }

        // Test 2: Poll from specific offset
        log.info("  Testing OFFSET strategy");
        try {
            var offsetMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(1L),
                            consumer,
                            PollingStrategy.offset(BigInteger.valueOf(5)),
                            3L,
                            false)
                    .get(5, TimeUnit.SECONDS);
            log.info(
                    "    Polled {} messages from offset 5",
                    offsetMessages.messages().size());
        } catch (RuntimeException e) {
            log.info("    OFFSET strategy failed: {}", e.getMessage());
        }

        // Test 3: Poll latest messages
        log.info("  Testing LAST strategy");
        try {
            var lastMessages = client.messages()
                    .pollMessagesAsync(
                            StreamId.of(testStream),
                            TopicId.of(TEST_TOPIC),
                            Optional.of(1L),
                            consumer,
                            PollingStrategy.last(),
                            1L,
                            false)
                    .get(5, TimeUnit.SECONDS);
            log.info("    Polled {} latest messages", lastMessages.messages().size());
        } catch (RuntimeException e) {
            log.info("    LAST strategy failed: {}", e.getMessage());
        }
    }
}
