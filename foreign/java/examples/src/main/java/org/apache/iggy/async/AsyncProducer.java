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

package org.apache.iggy.async;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AsyncProducer demonstrates how to use the async client to send messages to Apache Iggy.
 * This producer sends messages asynchronously and handles responses using CompletableFuture.
 */
public class AsyncProducer {
    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private static final String STREAM_NAME = "async-test";
    private static final String TOPIC_NAME = "events";
    private static final long PARTITION_ID = 1L;

    private static final int MESSAGE_COUNT = 100;
    private static final int MESSAGE_SIZE = 256;

    private final AsyncIggyTcpClient client;
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger errorCount = new AtomicInteger(0);

    public AsyncProducer() {
        this.client = new AsyncIggyTcpClient(HOST, PORT);
    }

    public CompletableFuture<Void> start() {
        log.info("Starting AsyncProducer...");

        return client.connect()
                .thenCompose(v -> {
                    log.info("Connected to Iggy server at {}:{}", HOST, PORT);
                    return client.users().loginAsync(USERNAME, PASSWORD);
                })
                .thenCompose(v -> {
                    log.info("Logged in successfully as user: {}", USERNAME);
                    return setupStreamAndTopic();
                })
                .thenCompose(v -> {
                    log.info("Stream and topic setup complete");
                    return sendMessages();
                })
                .thenRun(() -> {
                    log.info("All messages sent. Success: {}, Errors: {}", successCount.get(), errorCount.get());
                })
                .exceptionally(ex -> {
                    log.error("Error in producer flow", ex);
                    return null;
                });
    }

    private CompletableFuture<Void> setupStreamAndTopic() {
        log.info("Checking stream: {}", STREAM_NAME);

        return client.streams()
                .getStreamAsync(StreamId.of(STREAM_NAME))
                .thenCompose(stream -> {
                    if (stream.isEmpty()) {
                        log.info("Creating stream: {}", STREAM_NAME);
                        return client.streams()
                                .createStreamAsync(STREAM_NAME)
                                .thenAccept(created -> log.info("Stream created: {}", created.name()));
                    } else {
                        log.info("Stream exists: {}", STREAM_NAME);
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenCompose(v -> {
                    log.info("Checking topic: {}", TOPIC_NAME);
                    return client.topics().getTopicAsync(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME));
                })
                .thenCompose(topic -> {
                    if (topic.isEmpty()) {
                        log.info("Creating topic: {}", TOPIC_NAME);
                        return client.topics()
                                .createTopicAsync(
                                        StreamId.of(STREAM_NAME),
                                        1L, // 1 partition
                                        CompressionAlgorithm.None,
                                        BigInteger.ZERO,
                                        BigInteger.ZERO,
                                        Optional.empty(),
                                        TOPIC_NAME)
                                .thenAccept(created -> log.info("Topic created: {}", created.name()));
                    } else {
                        log.info("Topic exists: {}", TOPIC_NAME);
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private CompletableFuture<Void> sendMessages() {
        log.info("Sending {} messages...", MESSAGE_COUNT);

        CompletableFuture<?>[] futures = new CompletableFuture[MESSAGE_COUNT];

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final int messageIndex = i;
            futures[i] = sendMessage(messageIndex).handle((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message {}: {}", messageIndex, ex.getMessage());
                    errorCount.incrementAndGet();
                } else {
                    if (messageIndex % 10 == 0) {
                        log.debug("Sent message {}", messageIndex);
                    }
                    successCount.incrementAndGet();
                }
                return null;
            });

            // Add a small delay between messages to avoid overwhelming the server
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> sendMessage(int index) {
        // Create message payload
        String messageContent =
                String.format("Async message %d - %s - %s", index, UUID.randomUUID(), System.currentTimeMillis());

        // Pad message to desired size
        while (messageContent.length() < MESSAGE_SIZE) {
            messageContent += " ";
        }

        byte[] messageBytes = messageContent.getBytes(StandardCharsets.UTF_8);

        // Use the factory method to create a message
        Message message = Message.of(messageContent);

        // Create partitioning strategy (use partition ID)
        Partitioning partitioning = Partitioning.partitionId(PARTITION_ID);

        // Send message using async client
        return client.messages()
                .sendMessagesAsync(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME), partitioning, List.of(message));
    }

    public CompletableFuture<Void> stop() {
        log.info("Stopping AsyncProducer...");
        return client.close().thenRun(() -> log.info("AsyncProducer stopped"));
    }

    public static void main(String[] args) {
        AsyncProducer producer = new AsyncProducer();

        CompletableFuture<Void> producerFuture = producer.start()
                .thenCompose(v -> {
                    // Keep producer running for a while
                    CompletableFuture<Void> delay = new CompletableFuture<>();
                    CompletableFuture.delayedExecutor(2, TimeUnit.SECONDS).execute(() -> delay.complete(null));
                    return delay;
                })
                .thenCompose(v -> producer.stop());

        try {
            producerFuture.get(30, TimeUnit.SECONDS);
            log.info("AsyncProducer completed successfully");
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("AsyncProducer failed", e);
            System.exit(1);
        }
    }
}
