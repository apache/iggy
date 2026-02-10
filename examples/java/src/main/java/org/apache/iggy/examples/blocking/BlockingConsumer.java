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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Blocking Consumer Example
 *
 * <p>Demonstrates message consumption using the blocking (synchronous) Iggy client.
 *
 * <p>This example shows:
 * <ul>
 *   <li>Connection and authentication</li>
 *   <li>Consumer group creation and membership</li>
 *   <li>Continuous message polling with backpressure handling</li>
 *   <li>Offset management (auto-commit)</li>
 *   <li>Graceful shutdown</li>
 * </ul>
 *
 * <p>Run this after running BlockingProducer to see messages flow through.
 *
 * <p>Run with: {@code ./gradlew runBlockingConsumer}
 */
public final class BlockingConsumer {
    private static final Logger log = LoggerFactory.getLogger(BlockingConsumer.class);

    // Configuration (must match producer)
    private static final String IGGY_HOST = "localhost";
    private static final int IGGY_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final String STREAM_NAME = "example-stream";
    private static final String TOPIC_NAME = "example-topic";
    private static final String CONSUMER_GROUP_NAME = "example-consumer-group";
    private static final long CONSUMER_ID = 1L;

    // Polling configuration
    private static final int POLL_BATCH_SIZE = 100; // Max messages per poll
    private static final int POLL_INTERVAL_MS = 1000; // Poll every second

    private static volatile boolean running = true;

    private BlockingConsumer() {
        // Utility class
    }

    public static void main(String[] args) {
        IggyTcpClient client = null;

        // Handle Ctrl+C gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping consumer...");
            running = false;
        }));

        try {
            log.info("=== Blocking Consumer Example ===");

            // 1. Connect and authenticate
            log.info("Connecting to Iggy server at {}:{}...", IGGY_HOST, IGGY_PORT);
            client = Iggy.tcpClientBuilder()
                    .blocking()
                    .host(IGGY_HOST)
                    .port(IGGY_PORT)
                    .credentials(USERNAME, PASSWORD)
                    .buildAndLogin();

            log.info("Connected successfully");

            // 2. Setup consumer group
            setupConsumerGroup(client);

            // 3. Poll messages continuously
            pollMessages(client);

            log.info("=== Consumer stopped gracefully ===");

        } catch (RuntimeException e) {
            log.error("Consumer failed", e);
            System.exit(1);
        } finally {
            // The blocking client doesn't require explicit cleanup
            // Connection will be closed when the JVM exits
            if (client != null) {
                log.info("Client finished - connection will be cleaned up on exit");
            }
        }
    }

    private static void setupConsumerGroup(IggyTcpClient client) {
        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);

        try {
            // Create consumer group (will fail silently if it already exists)
            log.info("Creating consumer group '{}'...", CONSUMER_GROUP_NAME);
            client.consumerGroups().createConsumerGroup(streamId, topicId, CONSUMER_GROUP_NAME);
            log.info("Consumer group created");
        } catch (RuntimeException e) {
            // Consumer group might already exist
            log.info("Consumer group '{}' already exists or creation failed (continuing)", CONSUMER_GROUP_NAME);
        }

        // NOTE: For the blocking client, joining a consumer group is typically handled
        // automatically when polling with a group consumer. The async client requires
        // explicit join/leave due to its connection-based membership model.
    }

    private static void pollMessages(IggyTcpClient client) {
        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);

        log.info("Starting to poll messages (Ctrl+C to stop)...");

        int totalReceived = 0;
        int emptyPolls = 0;

        while (running) {
            try {
                // POLLING STRATEGY EXPLAINED:
                //
                // PollingStrategy.next() - Continue from last committed offset (most common)
                //   Use this for sequential processing where you want to resume from where you left off.
                //
                // PollingStrategy.offset(BigInteger.ZERO) - Start from specific offset
                //   Use this to replay messages or start from a known position.
                //
                // PollingStrategy.first() - Read from the beginning
                //   Use for initial load or reprocessing all data.
                //
                // PollingStrategy.last() - Read only new messages
                //   Use when you only care about recent data.

                PolledMessages polled = client.messages()
                        .pollMessages(
                                streamId,
                                topicId,
                                Optional.empty(), // Let server assign partition (load balancing)
                                Consumer.group(CONSUMER_ID), // Use consumer group for distributed consumption
                                PollingStrategy.next(), // Continue from last commit
                                (long) POLL_BATCH_SIZE,
                                true // Auto-commit offset after successful poll
                                );

                int messageCount = polled.messages().size();

                if (messageCount > 0) {
                    // Process messages
                    for (Message message : polled.messages()) {
                        String payload = new String(message.payload());
                        // In a real application, you'd process the message here
                        // For this example, we just log a subset to avoid spam
                        if (totalReceived < 5 || totalReceived % 100 == 0) {
                            log.info(
                                    "Received: offset={}, payload='{}'",
                                    message.header().offset(),
                                    payload);
                        }
                    }

                    totalReceived += messageCount;
                    emptyPolls = 0; // Reset empty poll counter

                    log.info("Processed {} messages (total: {})", messageCount, totalReceived);

                } else {
                    // No messages available
                    emptyPolls++;
                    if (emptyPolls == 1) {
                        log.info("Caught up - no new messages. Waiting...");
                    }

                    // BACKPRESSURE HANDLING:
                    // When no messages are available, sleep to avoid tight polling loop.
                    // This reduces CPU usage and network traffic.
                    Thread.sleep(POLL_INTERVAL_MS);
                }

            } catch (InterruptedException e) {
                log.info("Polling interrupted");
                running = false;
            } catch (RuntimeException e) {
                // ERROR RECOVERY:
                // In production, you'd implement retry logic with exponential backoff.
                // For this example, we log and continue.
                log.error("Error polling messages: {}", e.getMessage());
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    running = false;
                }
            }
        }

        log.info("Stopped polling. Total messages received: {}", totalReceived);
    }
}
