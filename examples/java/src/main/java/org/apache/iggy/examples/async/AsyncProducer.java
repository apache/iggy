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

package org.apache.iggy.examples.async;

import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AsyncProducer demonstrates how to use the async client to send messages to Apache Iggy.
 *
 * <p>This producer sends messages asynchronously and handles responses using CompletableFuture.
 *
 * <p>Run with: {@code ./gradlew runAsyncProducer}
 */
public final class AsyncProducer {
    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

    private static final String HOST = "localhost";
    private static final int PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private static final String STREAM_NAME = "async-test";
    private static final String TOPIC_NAME = "events";
    private static final long PARTITION_ID = 0L;

    private static final int MESSAGE_COUNT = 100;
    private static final int MESSAGE_BATCH_SIZE = 10;
    private static final int TOTAL_BATCHES = MESSAGE_COUNT / MESSAGE_BATCH_SIZE;

    private AsyncProducer() {
        // Utility class
    }

    public static void main(String[] args) {
        AsyncIggyTcpClient client = null;

        try {
            log.info("=== Async Producer Example ===");

            // 1. Connect and authenticate using builder
            log.info("Connecting to Iggy server at {}:{}...", HOST, PORT);
            client = Iggy.tcpClientBuilder()
                    .async()
                    .host(HOST)
                    .port(PORT)
                    .credentials(USERNAME, PASSWORD)
                    .buildAndLogin()
                    .join();

            log.info("Connected successfully");

            // 2. Set up stream and topic
            setupStreamAndTopic(client).join();
            log.info("Stream and topic setup complete");

            // 3. Send messages
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            sendMessages(client, successCount, errorCount).join();
            log.info("All messages sent. Success: {}, Errors: {}", successCount.get(), errorCount.get());

            log.info("=== Producer completed successfully ===");

        } catch (RuntimeException e) {
            log.error("Producer failed", e);
            System.exit(1);
        } finally {
            if (client != null) {
                try {
                    client.close().join();
                    log.info("Client closed");
                } catch (RuntimeException e) {
                    log.error("Error closing client", e);
                }
            }
        }
    }

    private static CompletableFuture<Void> setupStreamAndTopic(AsyncIggyTcpClient client) {
        log.info("Checking stream: {}", STREAM_NAME);

        return client.streams()
                .getStream(StreamId.of(STREAM_NAME))
                .thenCompose(streamOpt -> {
                    if (streamOpt.isPresent()) {
                        log.info("Stream '{}' already exists", STREAM_NAME);
                        return CompletableFuture.completedFuture(null);
                    }
                    log.info("Creating stream: {}", STREAM_NAME);
                    return client.streams()
                            .createStream(STREAM_NAME)
                            .thenAccept(created -> log.info("Stream created: {}", created.name()));
                })
                .thenCompose(v -> {
                    log.info("Checking topic: {}", TOPIC_NAME);
                    return client.topics().getTopic(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME));
                })
                .thenCompose(topicOpt -> {
                    if (topicOpt.isPresent()) {
                        log.info("Topic '{}' already exists", TOPIC_NAME);
                        return CompletableFuture.completedFuture(null);
                    }
                    log.info("Creating topic: {}", TOPIC_NAME);
                    return client.topics()
                            .createTopic(
                                    StreamId.of(STREAM_NAME),
                                    1L,
                                    CompressionAlgorithm.None,
                                    BigInteger.ZERO,
                                    BigInteger.ZERO,
                                    Optional.empty(),
                                    TOPIC_NAME)
                            .thenAccept(created -> log.info("Topic created: {}", created.name()));
                });
    }

    private static CompletableFuture<Void> sendMessages(
            AsyncIggyTcpClient client, AtomicInteger successCount, AtomicInteger errorCount) {
        log.info("Sending {} messages in {} batches...", MESSAGE_COUNT, TOTAL_BATCHES);

        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);
        Partitioning partitioning = Partitioning.partitionId(PARTITION_ID);

        long startTime = System.currentTimeMillis();

        CompletableFuture<?>[] futures = new CompletableFuture[TOTAL_BATCHES];

        for (int b = 0; b < TOTAL_BATCHES; b++) {
            final int batchNum = b;

            List<Message> batch = new ArrayList<>(MESSAGE_BATCH_SIZE);
            for (int i = 0; i < MESSAGE_BATCH_SIZE; i++) {
                int messageId = batchNum * MESSAGE_BATCH_SIZE + i;
                String payload = String.format(
                        "Async message %d - %s - %s", messageId, UUID.randomUUID(), System.currentTimeMillis());
                batch.add(Message.of(payload));
            }

            futures[b] = client.messages()
                    .sendMessages(streamId, topicId, partitioning, batch)
                    .handle((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send batch {}: {}", batchNum, ex.getMessage());
                            errorCount.addAndGet(MESSAGE_BATCH_SIZE);
                        } else {
                            successCount.addAndGet(MESSAGE_BATCH_SIZE);
                            if ((batchNum + 1) % 5 == 0) {
                                log.info("Sent batch {}/{}", batchNum + 1, TOTAL_BATCHES);
                            }
                        }
                        return null;
                    });
        }

        return CompletableFuture.allOf(futures).thenRun(() -> {
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Sent {} messages in {} ms", successCount.get(), elapsed);
        });
    }
}
