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
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class demonstrating how to send messages to Iggy using Async TCP Client.
 *
 * Prerequisites:
 * - Iggy server running on localhost:8090
 * - Stream 'text-input' exists
 * - Topic 'lines' exists
 *
 * Run docker-compose first:
 * cd $IGGY_HOME/foreign/java/external-processors/iggy-connector-flink
 * docker-compose up -d
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AsyncTcpMessageSendTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncTcpMessageSendTest.class);

    private static final String IGGY_SERVER_HOST = "localhost";
    private static final int IGGY_SERVER_TCP_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private AsyncIggyTcpClient client;

    @BeforeAll
    void setUp() {
        log.info("Setting up Async TCP Client...");

        // Create client with credentials for auto-login
        client = AsyncIggyTcpClient.builder()
                .host(IGGY_SERVER_HOST)
                .port(IGGY_SERVER_TCP_PORT)
                .credentials(USERNAME, PASSWORD)
                .build();

        // Connect - this will also auto-login with the provided credentials
        client.connect().join();
        log.info("Connected to Iggy server at {}:{}", IGGY_SERVER_HOST, IGGY_SERVER_TCP_PORT);
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

    @Test
    @Order(1)
    @DisplayName("Send single text message")
    void testSendSingleMessage() throws ExecutionException, InterruptedException {
        log.info("Test: Send single text message");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        String messageContent = "Hello from AsyncTcpClient at " + Instant.now();
        Message message = Message.of(messageContent);

        List<Message> messages = new ArrayList<>();
        messages.add(message);

        CompletableFuture<Void> sendFuture =
                client.messages().sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages);

        sendFuture.get();

        log.info("Successfully sent message: {}", messageContent);
        assertThat(sendFuture).isCompleted();
    }

    @Test
    @Order(2)
    @DisplayName("Send batch of messages")
    void testSendBatchMessages() throws ExecutionException, InterruptedException {
        log.info("Test: Send batch of messages");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        List<Message> messages = new ArrayList<>();
        int batchSize = 10;

        for (int i = 0; i < batchSize; i++) {
            String content = String.format("Batch message #%d sent at %s", i, Instant.now());
            messages.add(Message.of(content));
        }

        CompletableFuture<Void> sendFuture =
                client.messages().sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages);
        sendFuture.get();

        log.info("Successfully sent batch of {} messages", batchSize);
        assertThat(sendFuture).isCompleted();
    }

    @Test
    @Order(3)
    @DisplayName("Send message to specific partition")
    void testSendToSpecificPartition() throws ExecutionException, InterruptedException {
        log.info("Test: Send message to specific partition");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        Long targetPartition = 1L;
        String messageContent = "Message to partition " + targetPartition;

        Message message = Message.of(messageContent);
        List<Message> messages = new ArrayList<>();
        messages.add(message);

        CompletableFuture<Void> sendFuture = client.messages()
                .sendMessagesAsync(streamId, topicId, Partitioning.partitionId(targetPartition), messages);
        sendFuture.get();

        log.info("Successfully sent message to partition {}", targetPartition);
        assertThat(sendFuture).isCompleted();
    }

    @Test
    @Order(4)
    @DisplayName("Send messages using message key partitioning")
    void testSendWithMessageKey() throws ExecutionException, InterruptedException {
        log.info("Test: Send messages using message key partitioning");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        String messageContent = "Message with key partitioning";
        Message message = Message.of(messageContent);

        List<Message> messages = new ArrayList<>();
        messages.add(message);

        String messageKey = "test-key-123";
        CompletableFuture<Void> sendFuture =
                client.messages().sendMessagesAsync(streamId, topicId, Partitioning.messagesKey(messageKey), messages);
        sendFuture.get();

        log.info("Successfully sent message with key: {}", messageKey);
        assertThat(sendFuture).isCompleted();
    }

    @Test
    @Order(5)
    @DisplayName("Send JSON-like messages")
    void testSendJsonMessages() throws ExecutionException, InterruptedException {
        log.info("Test: Send JSON messages");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        String jsonPayload = String.format(
                "{\"sensor_id\":\"sensor-001\",\"temperature\":%.2f,\"timestamp\":\"%s\"}", 23.5, Instant.now());

        Message message = Message.of(jsonPayload);
        List<Message> messages = new ArrayList<>();
        messages.add(message);

        CompletableFuture<Void> sendFuture =
                client.messages().sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages);
        sendFuture.get();

        log.info("Successfully sent JSON message: {}", jsonPayload);
        assertThat(sendFuture).isCompleted();
    }

    @Test
    @Order(6)
    @DisplayName("Send multiple messages in parallel")
    void testSendMultipleMessagesInParallel() throws ExecutionException, InterruptedException {
        log.info("Test: Send multiple messages in parallel");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        int parallelRequests = 5;
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < parallelRequests; i++) {
            String content = "Parallel message #" + i;
            Message message = Message.of(content);
            List<Message> messages = new ArrayList<>();
            messages.add(message);

            CompletableFuture<Void> future =
                    client.messages().sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages);
            futures.add(future);
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        allFutures.get();

        log.info("Successfully sent {} messages in parallel", parallelRequests);
        assertThat(allFutures).isCompleted();
    }

    @Test
    @Order(7)
    @DisplayName("Send large batch of messages")
    void testSendLargeBatch() throws ExecutionException, InterruptedException {
        log.info("Test: Send large batch of messages");

        StreamId streamId = StreamId.of("text-input");
        TopicId topicId = TopicId.of("lines");

        List<Message> messages = new ArrayList<>();
        int batchSize = 100;

        for (int i = 0; i < batchSize; i++) {
            messages.add(Message.of("Large batch message #" + i));
        }

        long startTime = System.currentTimeMillis();
        CompletableFuture<Void> sendFuture =
                client.messages().sendMessagesAsync(streamId, topicId, Partitioning.balanced(), messages);
        sendFuture.get();
        long duration = System.currentTimeMillis() - startTime;

        log.info("Successfully sent {} messages in {}ms", batchSize, duration);
        assertThat(sendFuture).isCompleted();
    }
}
