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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Async Producer Example - High Throughput
 *
 * <p>Demonstrates high-throughput message production using the async (non-blocking) Iggy client.
 *
 * <h2>WHEN TO USE THE ASYNC CLIENT:</h2>
 * <ul>
 *   <li>You need &gt; 5000 msg/sec throughput</li>
 *   <li>Your application is already async/reactive (Spring WebFlux, Vert.x, etc.)</li>
 *   <li>You want to pipeline multiple requests over a single connection</li>
 *   <li>You're building a service that handles many concurrent streams</li>
 * </ul>
 *
 * <h2>KEY DIFFERENCES FROM BLOCKING CLIENT:</h2>
 * <ul>
 *   <li>All methods return CompletableFuture instead of direct values</li>
 *   <li>Built on Netty's non-blocking I/O (no thread-per-request)</li>
 *   <li>Can pipeline multiple send operations without waiting for each to complete</li>
 *   <li>Single connection handles all concurrent requests via event loop multiplexing</li>
 * </ul>
 *
 * <h2>PERFORMANCE CHARACTERISTICS:</h2>
 * <ul>
 *   <li>Throughput: Higher (pipelines requests, no thread contention)</li>
 *   <li>Latency per request: Similar to blocking</li>
 *   <li>Thread usage: Minimal (Netty event loop threads)</li>
 *   <li>Code complexity: Higher (futures, callbacks)</li>
 * </ul>
 *
 * <p>This example shows:
 * <ul>
 *   <li>Async client setup with CompletableFuture chaining</li>
 *   <li>Pipelined message sending (fire multiple sends without blocking)</li>
 *   <li>Error handling with exceptionally()</li>
 *   <li>Performance measurement</li>
 *   <li>Proper async shutdown</li>
 * </ul>
 *
 * <p>Run with: {@code ./gradlew runAsyncProducer}
 */
public final class AsyncProducer {
    private static final Logger log = LoggerFactory.getLogger(AsyncProducer.class);

    // Configuration
    private static final String IGGY_HOST = "localhost";
    private static final int IGGY_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final String STREAM_NAME = "async-example-stream";
    private static final String TOPIC_NAME = "async-example-topic";
    private static final int PARTITION_COUNT = 3;

    // High-throughput configuration
    private static final int MESSAGE_BATCH_SIZE = 500; // Larger batches for async
    private static final int TOTAL_BATCHES = 20;
    private static final int MAX_IN_FLIGHT = 5; // Pipeline up to 5 concurrent sends

    private AsyncProducer() {
        // Utility class
    }

    public static void main(String[] args) {
        AsyncIggyTcpClient client = null;

        try {
            log.info("=== Async Producer Example (High Throughput) ===");

            // 1. Build, connect, and login - all chained with CompletableFuture
            log.info("Connecting to Iggy server at {}:{}...", IGGY_HOST, IGGY_PORT);

            // ASYNC PATTERN: Use join() only at the end to block until client is ready.
            // In a real async app (e.g. Spring WebFlux), you'd chain everything without join().
            client = Iggy.tcpClientBuilder()
                    .async()
                    .host(IGGY_HOST)
                    .port(IGGY_PORT)
                    .credentials(USERNAME, PASSWORD)
                    .buildAndLogin()
                    .join(); // Block here to wait for connection

            log.info("Connected successfully");

            // 2. Setup stream and topic
            AsyncIggyTcpClient finalClient = client;
            setupStreamAndTopic(finalClient).join();

            // 3. Send messages with pipelining
            sendMessagesAsync(finalClient).join();

            log.info("=== Producer completed successfully ===");

        } catch (RuntimeException e) {
            log.error("Producer failed", e);
            System.exit(1);
        } finally {
            // Always close the client
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
        // ASYNC CHAINING PATTERN:
        // Each operation returns CompletableFuture. We chain them with thenCompose().
        // Errors propagate down the chain and can be handled with exceptionally().

        return client.streams()
                .getStream(StreamId.of(STREAM_NAME))
                .thenApply(stream -> {
                    log.info("Stream '{}' already exists", STREAM_NAME);
                    return null;
                })
                .exceptionally(e -> {
                    log.info("Creating stream '{}'...", STREAM_NAME);
                    return null;
                })
                .thenCompose(v -> client.streams().createStream(STREAM_NAME).exceptionally(e -> {
                    // Stream might exist (race condition), ignore error
                    return null;
                }))
                .thenCompose(v -> client.topics()
                        .getTopic(StreamId.of(STREAM_NAME), TopicId.of(TOPIC_NAME))
                        .thenApply(topic -> {
                            log.info("Topic '{}' already exists", TOPIC_NAME);
                            return null;
                        })
                        .exceptionally(e -> {
                            log.info("Creating topic '{}' with {} partitions...", TOPIC_NAME, PARTITION_COUNT);
                            return null;
                        }))
                .thenCompose(v -> client.topics()
                        .createTopic(
                                StreamId.of(STREAM_NAME),
                                (long) PARTITION_COUNT,
                                CompressionAlgorithm.None,
                                BigInteger.ZERO,
                                BigInteger.ZERO,
                                Optional.empty(),
                                TOPIC_NAME)
                        .exceptionally(e -> {
                            // Topic might exist, ignore error
                            return null;
                        }))
                .thenRun(() -> log.info("Stream and topic setup complete"));
    }

    private static CompletableFuture<Void> sendMessagesAsync(AsyncIggyTcpClient client) {
        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);

        log.info("Sending {} batches of {} messages each with pipelining...", TOTAL_BATCHES, MESSAGE_BATCH_SIZE);
        log.info("Max in-flight requests: {}", MAX_IN_FLIGHT);

        long startTime = System.currentTimeMillis();
        AtomicInteger totalSent = new AtomicInteger(0);
        AtomicInteger completedBatches = new AtomicInteger(0);

        // PIPELINING PATTERN:
        // Instead of waiting for each send to complete, we fire up to MAX_IN_FLIGHT
        // concurrent sends. This is where async shines - the single Netty connection
        // multiplexes all these requests without blocking threads.

        List<CompletableFuture<Void>> inFlightBatches = new ArrayList<>();

        for (int batchNum = 0; batchNum < TOTAL_BATCHES; batchNum++) {
            // Build batch
            List<Message> batch = new ArrayList<>(MESSAGE_BATCH_SIZE);
            for (int i = 0; i < MESSAGE_BATCH_SIZE; i++) {
                int messageId = batchNum * MESSAGE_BATCH_SIZE + i;
                String payload = String.format("Async message %d", messageId);
                batch.add(Message.of(payload));
            }

            final int currentBatch = batchNum;

            // Fire the send (non-blocking)
            CompletableFuture<Void> sendFuture = client.messages()
                    .sendMessages(streamId, topicId, Partitioning.balanced(), batch)
                    .thenAccept(v -> {
                        int sent = totalSent.addAndGet(MESSAGE_BATCH_SIZE);
                        int completed = completedBatches.incrementAndGet();

                        if (completed % 5 == 0) {
                            log.info("Completed {} batches ({} messages)...", completed, sent);
                        }
                    })
                    .exceptionally(e -> {
                        log.error("Batch {} failed: {}", currentBatch, e.getMessage());
                        return null;
                    });

            inFlightBatches.add(sendFuture);

            // BACKPRESSURE: If we have MAX_IN_FLIGHT batches in flight, wait for one to complete
            // before sending the next. This prevents memory exhaustion from unlimited pipelining.
            if (inFlightBatches.size() >= MAX_IN_FLIGHT) {
                // Wait for the oldest batch to complete
                CompletableFuture<Void> oldest = inFlightBatches.remove(0);
                oldest.join();
            }
        }

        // Wait for all remaining in-flight batches
        return CompletableFuture.allOf(inFlightBatches.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    long elapsed = System.currentTimeMillis() - startTime;
                    int total = totalSent.get();
                    double messagesPerSec = (total * 1000.0) / elapsed;

                    log.info(
                            "Sent {} messages in {} ms ({} msg/sec)",
                            total,
                            elapsed,
                            String.format("%.2f", messagesPerSec));

                    // PERFORMANCE COMPARISON:
                    // The async client typically achieves 2-5x higher throughput than blocking
                    // for the same hardware, due to pipelining and non-blocking I/O.
                    //
                    // Blocking: ~5,000 msg/sec (depends on batch size and network latency)
                    // Async:    ~20,000+ msg/sec (with proper pipelining)
                });
    }
}
