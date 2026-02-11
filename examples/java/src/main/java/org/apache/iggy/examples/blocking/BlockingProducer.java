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

package org.apache.iggy.examples.blocking;

import org.apache.iggy.Iggy;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
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

/**
 * Blocking Producer Example
 *
 * <p>Demonstrates basic message production using the blocking (synchronous) Iggy client.
 *
 * <h2>WHEN TO USE THE BLOCKING CLIENT:</h2>
 * <ul>
 *   <li>Simple applications, scripts, CLI tools</li>
 *   <li>Message rate &lt; 1000/sec</li>
 *   <li>Sequential code is easier to reason about</li>
 *   <li>Integration tests</li>
 * </ul>
 *
 * <p>This example shows:
 * <ul>
 *   <li>Client connection and authentication</li>
 *   <li>Stream and topic creation</li>
 *   <li>Batch message sending (recommended for efficiency)</li>
 *   <li>Balanced partitioning for throughput</li>
 *   <li>Proper resource cleanup</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew runBlockingProducer}
 */
public final class BlockingProducer {
    private static final Logger log = LoggerFactory.getLogger(BlockingProducer.class);

    // Configuration
    private static final String IGGY_HOST = "localhost";
    private static final int IGGY_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final String STREAM_NAME = "example-stream";
    private static final String TOPIC_NAME = "example-topic";
    private static final int PARTITION_COUNT = 3;
    private static final int MESSAGE_BATCH_SIZE = 100;
    private static final int TOTAL_BATCHES = 10;

    private BlockingProducer() {
        // Utility class
    }

    public static void main(String[] args) {
        IggyTcpClient client = null;

        try {
            log.info("=== Blocking Producer Example ===");

            // 1. Build and connect the client
            log.info("Connecting to Iggy server at {}:{}...", IGGY_HOST, IGGY_PORT);
            client = Iggy.tcpClientBuilder()
                    .blocking()
                    .host(IGGY_HOST)
                    .port(IGGY_PORT)
                    .credentials(USERNAME, PASSWORD)
                    .buildAndLogin(); // One-liner: build, connect, login

            log.info("Connected successfully");

            // 2. Create stream (idempotent - succeeds if already exists)
            setupStreamAndTopic(client);

            // 3. Send messages in batches
            sendMessages(client);

            log.info("=== Producer completed successfully ===");

        } catch (RuntimeException e) {
            log.error("Producer failed", e);
            System.exit(1);
        } finally {
            // The blocking client doesn't require explicit cleanup
            // Connection will be closed when the JVM exits
            if (client != null) {
                log.info("Client finished - connection will be cleaned up on exit");
            }
        }
    }

    private static void setupStreamAndTopic(IggyTcpClient client) {
        // Check if stream exists, create if not
        if (client.streams().getStream(StreamId.of(STREAM_NAME)).isPresent()) {
            log.info("Stream '{}' already exists", STREAM_NAME);
        } else {
            log.info("Creating stream '{}'...", STREAM_NAME);
            client.streams().createStream(STREAM_NAME);
            log.info("Stream created");
        }

        // Check if topic exists, create if not
        if (client.topics()
                .getTopic(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME))
                .isPresent()) {
            log.info("Topic '{}' already exists", TOPIC_NAME);
        } else {
            log.info("Creating topic '{}' with {} partitions...", TOPIC_NAME, PARTITION_COUNT);
            client.topics()
                    .createTopic(
                            StreamId.of(STREAM_NAME),
                            (long) PARTITION_COUNT, // partitionCount
                            CompressionAlgorithm.None, // compressionAlgorithm
                            BigInteger.ZERO, // messageExpiry (0 = never)
                            BigInteger.ZERO, // maxTopicSize (0 = unlimited)
                            Optional.empty(), // replicationFactor
                            TOPIC_NAME);
            log.info("Topic created");
        }
    }

    private static void sendMessages(IggyTcpClient client) {
        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);

        log.info("Sending {} batches of {} messages each...", TOTAL_BATCHES, MESSAGE_BATCH_SIZE);

        long startTime = System.currentTimeMillis();
        int totalSent = 0;

        for (int batchNum = 0; batchNum < TOTAL_BATCHES; batchNum++) {
            // Build a batch of messages
            // BEST PRACTICE: Send in batches (10-1000 messages) for optimal throughput.
            // Iggy transmits all messages in one network round-trip.
            List<Message> batch = new ArrayList<>(MESSAGE_BATCH_SIZE);
            for (int i = 0; i < MESSAGE_BATCH_SIZE; i++) {
                int messageId = batchNum * MESSAGE_BATCH_SIZE + i;
                String payload = String.format("Message %d from blocking producer", messageId);
                batch.add(Message.of(payload));
            }

            // Send the batch
            // PARTITIONING STRATEGY: Partitioning.balanced() uses round-robin across partitions.
            // This maximizes throughput when message order doesn't matter.
            // Alternatives:
            //   - Partitioning.partitionId(id) for explicit partition control
            //   - Partitioning.messagesKey("key") for hash-based routing (per-key ordering)
            client.messages().sendMessages(streamId, topicId, Partitioning.balanced(), batch);

            totalSent += batch.size();

            if ((batchNum + 1) % 5 == 0) {
                log.info("Sent {} messages so far...", totalSent);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double messagesPerSec = (totalSent * 1000.0) / elapsed;

        log.info("Sent {} messages in {} ms ({} msg/sec)", totalSent, elapsed, String.format("%.2f", messagesPerSec));

        // PERFORMANCE NOTE:
        // The blocking client sends one request at a time. For throughput > 5000 msg/sec,
        // consider the async client which pipelines requests over a single connection.
    }
}
