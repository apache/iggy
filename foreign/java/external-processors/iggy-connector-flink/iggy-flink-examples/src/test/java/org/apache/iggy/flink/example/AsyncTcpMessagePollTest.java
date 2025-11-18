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

package org.apache.iggy.flink.example;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class demonstrating how to poll/read messages from Iggy using Async TCP Client.
 * <p>
 * Prerequisites:
 * - Iggy server running on localhost:8090
 * - Stream 'text-input' exists
 * - Topic 'lines' exists with at least 2 partitions
 * <p>
 * Run docker-compose first:
 * cd $IGGY_ROOT/foreign/java/external-processors/iggy-connector-flink
 * docker-compose up -d
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AsyncTcpMessagePollTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncTcpMessagePollTest.class);

    private static final String IGGY_SERVER_HOST = "localhost";
    private static final int IGGY_SERVER_TCP_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private AsyncIggyTcpClient client;

    @BeforeAll
    void setUp() throws Exception {
        log.info("Setting up Async TCP Client for polling...");

        // Create client with credentials for auto-login
        client = AsyncIggyTcpClient.builder()
                .host(IGGY_SERVER_HOST)
                .port(IGGY_SERVER_TCP_PORT)
                .credentials(USERNAME, PASSWORD)
                .build();

        // Connect - this will also auto-login with the provided credentials
        client.connect().join();
        log.info("Connected to Iggy server at {}:{}", IGGY_SERVER_HOST, IGGY_SERVER_TCP_PORT);

        // Send some test messages to ensure we have data to poll
        seedTestMessages();
    }

    @AfterAll
    void tearDown() {
        if (client != null) {
            try {
                client.users().logoutAsync().join();
                log.info("Logged out successfully");
            } catch (RuntimeException e) {
                log.warn("Error during logout: {}", e.getMessage());
            }

            try {
                client.close().join();
                log.info("Disconnected from Iggy server");
            } catch (RuntimeException e) {
                log.warn("Error during disconnect: {}", e.getMessage());
            }
        }
    }

    private void seedTestMessages() throws ExecutionException, InterruptedException {
        log.info("Seeding test messages...");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String content = "Test message #" + i + " at " + Instant.now();
            messages.add(Message.of(content));
        }

        client.messages()
                .sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages)
                .get();
        log.info("Seeded {} test messages", messages.size());
    }

    @Test
    @Order(1)
    @DisplayName("Poll messages from offset 0")
    void testPollFromBeginning() throws ExecutionException, InterruptedException {
        log.info("Test: Poll messages from offset 0");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Create poll request starting from offset 0
        PollingStrategy pollingStrategy = PollingStrategy.offset(BigInteger.ZERO);
        Consumer consumer = Consumer.of(1L);

        CompletableFuture<PolledMessages> pollFuture = client.messages()
                .pollMessagesAsync(
                        streamId,
                        topicId,
                        Optional.of(1L), // partition 1
                        consumer,
                        pollingStrategy,
                        10L, // count
                        false // autoCommit
                        );

        PolledMessages polledMessages = pollFuture.get();

        log.info("Polled {} messages from offset 0", polledMessages.messages().size());
        assertThat(polledMessages).isNotNull();
        assertThat(polledMessages.messages()).isNotEmpty();

        // Display first message
        if (!polledMessages.messages().isEmpty()) {
            Message firstMessage = polledMessages.messages().get(0);
            String payload = new String(firstMessage.payload(), StandardCharsets.UTF_8);
            log.info("First message - Payload: {}", payload);
        }
    }

    @Test
    @Order(2)
    @DisplayName("Poll messages using NEXT strategy")
    void testPollNext() throws ExecutionException, InterruptedException {
        log.info("Test: Poll messages using NEXT strategy");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Use NEXT polling strategy to get messages after last consumed
        PollingStrategy pollingStrategy = PollingStrategy.next();
        Consumer consumer = Consumer.of(2L);

        CompletableFuture<PolledMessages> pollFuture = client.messages()
                .pollMessagesAsync(
                        streamId,
                        topicId,
                        Optional.of(1L), // partition 1
                        consumer,
                        pollingStrategy,
                        5L, // count
                        true // autoCommit enabled
                        );

        PolledMessages polledMessages = pollFuture.get();

        log.info(
                "Polled {} messages using NEXT strategy",
                polledMessages.messages().size());
        assertThat(polledMessages).isNotNull();

        for (Message msg : polledMessages.messages()) {
            String payload = new String(msg.payload(), StandardCharsets.UTF_8);
            log.info("Message - Payload: {}", payload);
        }
    }

    @Test
    @Order(3)
    @DisplayName("Poll messages from all partitions")
    void testPollAllPartitions() throws ExecutionException, InterruptedException {
        log.info("Test: Poll messages from all partitions");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Poll without specifying partition (all partitions)
        PollingStrategy pollingStrategy = PollingStrategy.offset(BigInteger.ZERO);
        Consumer consumer = Consumer.of(3L);

        CompletableFuture<PolledMessages> pollFuture = client.messages()
                .pollMessagesAsync(
                        streamId,
                        topicId,
                        Optional.empty(), // all partitions
                        consumer,
                        pollingStrategy,
                        10L,
                        false);

        PolledMessages polledMessages = pollFuture.get();

        log.info(
                "Polled {} messages from all partitions",
                polledMessages.messages().size());
        assertThat(polledMessages).isNotNull();
    }

    @Test
    @Order(4)
    @DisplayName("Poll messages from specific offset")
    void testPollFromSpecificOffset() throws ExecutionException, InterruptedException {
        log.info("Test: Poll messages from specific offset");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Poll from a specific offset (e.g., offset 5)
        long specificOffset = 5L;
        PollingStrategy pollingStrategy = PollingStrategy.offset(BigInteger.valueOf(specificOffset));
        Consumer consumer = Consumer.of(4L);

        PolledMessages polledMessages = client.messages()
                .pollMessagesAsync(streamId, topicId, Optional.of(1L), consumer, pollingStrategy, 3L, false)
                .get();

        log.info("Polled {} messages from offset {}", polledMessages.messages().size(), specificOffset);

        for (Message msg : polledMessages.messages()) {
            String payload = new String(msg.payload(), StandardCharsets.UTF_8);
            log.info("Payload: {}", payload);
        }
    }

    @Test
    @Order(5)
    @DisplayName("Poll messages from multiple partitions in parallel")
    void testPollFromMultiplePartitions() throws ExecutionException, InterruptedException {
        log.info("Test: Poll messages from multiple partitions in parallel");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        PollingStrategy strategy = PollingStrategy.offset(BigInteger.ZERO);
        Consumer consumer = Consumer.of(5L);

        // Poll from partition 1
        CompletableFuture<PolledMessages> future1 =
                client.messages().pollMessagesAsync(streamId, topicId, Optional.of(1L), consumer, strategy, 5L, false);

        // Poll from partition 2
        CompletableFuture<PolledMessages> future2 =
                client.messages().pollMessagesAsync(streamId, topicId, Optional.of(2L), consumer, strategy, 5L, false);

        // Wait for both
        CompletableFuture.allOf(future1, future2).get();

        PolledMessages messages1 = future1.get();
        PolledMessages messages2 = future2.get();

        log.info("Polled {} messages from partition 1", messages1.messages().size());
        log.info("Polled {} messages from partition 2", messages2.messages().size());

        assertThat(messages1).isNotNull();
        assertThat(messages2).isNotNull();
    }

    @Test
    @Order(6)
    @DisplayName("Poll with FIRST strategy")
    void testPollFirst() throws ExecutionException, InterruptedException {
        log.info("Test: Poll with FIRST strategy");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Use FIRST polling strategy to get earliest available messages
        PollingStrategy pollingStrategy = PollingStrategy.first();
        Consumer consumer = Consumer.of(6L);

        PolledMessages polledMessages = client.messages()
                .pollMessagesAsync(streamId, topicId, Optional.of(1L), consumer, pollingStrategy, 3L, false)
                .get();

        log.info(
                "Polled {} messages using FIRST strategy",
                polledMessages.messages().size());

        if (!polledMessages.messages().isEmpty()) {
            Message firstMessage = polledMessages.messages().get(0);
            String payload = new String(firstMessage.payload(), StandardCharsets.UTF_8);
            log.info("First available message - Payload: {}", payload);
        }

        assertThat(polledMessages).isNotNull();
    }

    @Test
    @Order(7)
    @DisplayName("Poll with LAST strategy")
    void testPollLast() throws ExecutionException, InterruptedException {
        log.info("Test: Poll with LAST strategy");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        // Use LAST polling strategy to get most recent messages
        PollingStrategy pollingStrategy = PollingStrategy.last();
        Consumer consumer = Consumer.of(7L);

        PolledMessages polledMessages = client.messages()
                .pollMessagesAsync(streamId, topicId, Optional.of(1L), consumer, pollingStrategy, 3L, false)
                .get();

        log.info(
                "Polled {} messages using LAST strategy",
                polledMessages.messages().size());

        if (!polledMessages.messages().isEmpty()) {
            Message lastMessage =
                    polledMessages.messages().get(polledMessages.messages().size() - 1);
            String payload = new String(lastMessage.payload(), StandardCharsets.UTF_8);
            log.info("Last message - Payload: {}", payload);
        }

        assertThat(polledMessages).isNotNull();
    }

    @Test
    @Order(8)
    @DisplayName("Continuous polling simulation")
    void testContinuousPolling() throws ExecutionException, InterruptedException {
        log.info("Test: Continuous polling simulation");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        Consumer consumer = Consumer.of(8L);
        int totalMessagesPolled = 0;
        int maxIterations = 3;

        for (int i = 0; i < maxIterations; i++) {
            PollingStrategy pollingStrategy = PollingStrategy.next();

            PolledMessages polledMessages = client.messages()
                    .pollMessagesAsync(
                            streamId, topicId, Optional.of(1L), consumer, pollingStrategy, 5L, true // autoCommit
                            )
                    .get();

            int messageCount = polledMessages.messages().size();
            totalMessagesPolled += messageCount;

            log.info("Iteration {}: Polled {} messages", i + 1, messageCount);

            if (messageCount == 0) {
                log.info("No more messages available, stopping");
                break;
            }

            // Simulate processing time
            Thread.sleep(100);
        }

        log.info("Total messages polled across all iterations: {}", totalMessagesPolled);
        assertThat(totalMessagesPolled).isGreaterThanOrEqualTo(0);
    }
}
