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
 *
 * <ul>
 *   <li>Connection and authentication</li>
 *   <li>Continuous message polling from a specific partition</li>
 *   <li>Offset-based consumption</li>
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
    private static final long PARTITION_ID = 0L;
    private static final long CONSUMER_ID = 0L;

    // Polling configuration
    private static final int POLL_BATCH_SIZE = 100; // Max messages per poll
    private static final int POLL_INTERVAL_MS = 1000; // Poll every second
    private static final int BATCHES_LIMIT = 5; // Exit after receiving this many batches

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

            // 2. Poll messages
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

    private static void pollMessages(IggyTcpClient client) {
        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);

        log.info(
                "Messages will be consumed from stream: {}, topic: {}, partition: {}",
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID);

        java.math.BigInteger offset = java.math.BigInteger.ZERO;
        int consumedBatches = 0;
        Consumer consumer = Consumer.of(CONSUMER_ID);

        while (running && consumedBatches < BATCHES_LIMIT) {
            try {
                PolledMessages polled = client.messages()
                        .pollMessages(
                                streamId,
                                topicId,
                                Optional.of(PARTITION_ID),
                                consumer,
                                PollingStrategy.offset(offset),
                                (long) POLL_BATCH_SIZE,
                                false);

                if (polled.messages().isEmpty()) {
                    log.info("No messages found, waiting...");
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }

                for (Message message : polled.messages()) {
                    String payload = new String(message.payload());
                    log.info(
                            "Received: offset={}, payload='{}'",
                            message.header().offset(),
                            payload);
                }

                consumedBatches++;
                offset = offset.add(
                        java.math.BigInteger.valueOf(polled.messages().size()));

                log.info("Consumed batch {} of {}", consumedBatches, BATCHES_LIMIT);
                Thread.sleep(POLL_INTERVAL_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            } catch (RuntimeException e) {
                log.error("Error polling messages: {}", e.getMessage());
                running = false;
            }
        }

        log.info("Finished consuming {} batches.", consumedBatches);
    }
}
