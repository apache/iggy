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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async Consumer Example - Backpressure and Error Handling
 *
 * <p>Demonstrates advanced async message consumption patterns including:
 * <ul>
 *   <li>Non-blocking continuous polling</li>
 *   <li>Backpressure management (don't poll faster than you can process)</li>
 *   <li>Error recovery with exponential backoff</li>
 *   <li>Offloading CPU-intensive work from Netty threads</li>
 *   <li>Graceful shutdown</li>
 * </ul>
 *
 * <h2>CRITICAL ASYNC PATTERN - Thread Pool Management:</h2>
 *
 * <p>The async client uses Netty's event loop threads for I/O operations.
 * <strong>NEVER</strong> block these threads with:
 * <ul>
 *   <li>{@code .join()} or {@code .get()} inside {@code thenApply/thenAccept}</li>
 *   <li>{@code Thread.sleep()}</li>
 *   <li>Blocking database calls</li>
 *   <li>Long-running computations</li>
 * </ul>
 *
 * <p>If your message processing involves blocking operations, offload to a separate
 * thread pool using {@code thenApplyAsync(fn, executor)}.
 *
 * <p>This example shows the correct pattern.
 *
 * <p>Run after AsyncProducer to see messages flow.
 *
 * <p>Run with: {@code ./gradlew runAsyncConsumer}
 */
public final class AsyncConsumer {
    private static final Logger log = LoggerFactory.getLogger(AsyncConsumer.class);

    // Configuration (must match AsyncProducer)
    private static final String IGGY_HOST = "localhost";
    private static final int IGGY_PORT = 8090;
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final String STREAM_NAME = "async-test";
    private static final String TOPIC_NAME = "events";
    private static final long PARTITION_ID = 0L;
    private static final long CONSUMER_ID = 0L;

    // Polling configuration
    private static final int POLL_BATCH_SIZE = 100;
    private static final int POLL_INTERVAL_MS = 1000;
    private static final int BATCHES_LIMIT = 5; // Exit after receiving this many batches
    private static final int MAX_EMPTY_POLLS = 5; // Exit if no messages after consecutive empty polls

    // Error recovery configuration
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final int INITIAL_BACKOFF_MS = 100;
    private static final int MAX_BACKOFF_MS = 5000;

    // Thread pool for message processing (separate from Netty threads)
    // Size based on workload: CPU-bound = availableProcessors, I/O-bound = 2x or more
    private static final int PROCESSING_THREADS = Runtime.getRuntime().availableProcessors();

    private static volatile boolean running = true;

    private AsyncConsumer() {
        // Utility class
    }

    public static void main(String[] args) {
        AsyncIggyTcpClient client = null;
        ExecutorService processingPool = null;

        // Handle Ctrl+C gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping consumer...");
            running = false;
        }));

        try {
            log.info("=== Async Consumer Example (Backpressure + Error Handling) ===");

            // Create thread pool for message processing
            processingPool = Executors.newFixedThreadPool(PROCESSING_THREADS, r -> {
                Thread t = new Thread(r, "message-processor");
                t.setDaemon(true);
                return t;
            });

            log.info("Created processing thread pool with {} threads", PROCESSING_THREADS);

            // 1. Connect and authenticate
            log.info("Connecting to Iggy server at {}:{}...", IGGY_HOST, IGGY_PORT);
            client = Iggy.tcpClientBuilder()
                    .async()
                    .host(IGGY_HOST)
                    .port(IGGY_PORT)
                    .credentials(USERNAME, PASSWORD)
                    .buildAndLogin()
                    .join();

            log.info("Connected successfully");

            // 2. Poll messages continuously with backpressure
            AsyncIggyTcpClient finalClient = client;
            pollMessagesAsync(finalClient, processingPool).join();

            log.info("=== Consumer stopped gracefully ===");

        } catch (RuntimeException e) {
            log.error("Consumer failed", e);
            System.exit(1);
        } finally {
            // Cleanup
            if (client != null) {
                try {
                    client.close().join();
                    log.info("Client closed");
                } catch (RuntimeException e) {
                    log.error("Error closing client", e);
                }
            }

            if (processingPool != null) {
                processingPool.shutdown();
                try {
                    if (!processingPool.awaitTermination(5, TimeUnit.SECONDS)) {
                        processingPool.shutdownNow();
                    }
                    log.info("Processing thread pool shut down");
                } catch (InterruptedException e) {
                    processingPool.shutdownNow();
                }
            }
        }
    }

    private static CompletableFuture<Void> pollMessagesAsync(
            AsyncIggyTcpClient client, ExecutorService processingPool) {
        log.info("Starting async polling loop (limit: {} batches)...", BATCHES_LIMIT);

        AtomicInteger totalReceived = new AtomicInteger(0);
        AtomicInteger emptyPolls = new AtomicInteger(0);
        AtomicInteger consumedBatches = new AtomicInteger(0);
        AtomicReference<BigInteger> offset = new AtomicReference<>(BigInteger.ZERO);

        // RECURSIVE ASYNC POLLING PATTERN:
        // Each poll schedules the next poll after processing completes.
        // This provides natural backpressure - we don't poll for new messages
        // until we've finished processing the current batch.

        CompletableFuture<Void> pollingLoop = new CompletableFuture<>();
        pollBatch(client, processingPool, totalReceived, emptyPolls, consumedBatches, offset, 0, pollingLoop);
        return pollingLoop;
    }

    private static void pollBatch(
            AsyncIggyTcpClient client,
            ExecutorService processingPool,
            AtomicInteger totalReceived,
            AtomicInteger emptyPolls,
            AtomicInteger consumedBatches,
            AtomicReference<BigInteger> offset,
            int retryAttempt,
            CompletableFuture<Void> loopFuture) {
        if (!running || consumedBatches.get() >= BATCHES_LIMIT) {
            log.info(
                    "Finished consuming {} batches. Total messages received: {}",
                    consumedBatches.get(),
                    totalReceived.get());
            loopFuture.complete(null);
            return;
        }

        StreamId streamId = StreamId.of(STREAM_NAME);
        TopicId topicId = TopicId.of(TOPIC_NAME);
        Consumer consumer = Consumer.of(CONSUMER_ID);

        client.messages()
                .pollMessages(
                        streamId,
                        topicId,
                        Optional.of(PARTITION_ID),
                        consumer,
                        PollingStrategy.offset(offset.get()),
                        (long) POLL_BATCH_SIZE,
                        false)
                .thenComposeAsync(
                        polled -> {
                            // OFFLOAD TO PROCESSING POOL:
                            // We use thenComposeAsync with processingPool to move message processing
                            // off the Netty event loop. This is critical for heavy workloads.

                            int messageCount = polled.messages().size();

                            if (messageCount > 0) {
                                // Update offset for next poll
                                offset.updateAndGet(current -> current.add(BigInteger.valueOf(messageCount)));
                                consumedBatches.incrementAndGet();

                                return processMessages(polled, totalReceived, processingPool)
                                        .thenRun(() -> emptyPolls.set(0));
                            } else {
                                int empty = emptyPolls.incrementAndGet();
                                if (empty >= MAX_EMPTY_POLLS) {
                                    log.info("No more messages after {} empty polls, finishing.", MAX_EMPTY_POLLS);
                                    running = false;
                                    return CompletableFuture.completedFuture(null);
                                }
                                log.info("Caught up - no new messages. Waiting...");
                                // Sleep without blocking Netty threads
                                return CompletableFuture.runAsync(
                                        () -> {
                                            try {
                                                Thread.sleep(POLL_INTERVAL_MS);
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                            }
                                        },
                                        processingPool);
                            }
                        },
                        processingPool)
                .thenRun(() -> {
                    // SUCCESS: Reset retry counter and schedule next poll
                    pollBatch(
                            client, processingPool, totalReceived, emptyPolls, consumedBatches, offset, 0, loopFuture);
                })
                .exceptionally(e -> {
                    // ERROR RECOVERY WITH EXPONENTIAL BACKOFF:
                    // Don't give up on the first error. Retry with increasing delays.
                    log.error(
                            "Error polling messages (attempt {}/{}): {}",
                            retryAttempt + 1,
                            MAX_RETRY_ATTEMPTS,
                            e.getMessage());

                    if (retryAttempt < MAX_RETRY_ATTEMPTS) {
                        int backoffMs = Math.min(INITIAL_BACKOFF_MS * (1 << retryAttempt), MAX_BACKOFF_MS);
                        log.info("Retrying in {} ms...", backoffMs);

                        // Schedule retry after backoff
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        Thread.sleep(backoffMs);
                                    } catch (InterruptedException ie) {
                                        Thread.currentThread().interrupt();
                                    }
                                    pollBatch(
                                            client,
                                            processingPool,
                                            totalReceived,
                                            emptyPolls,
                                            consumedBatches,
                                            offset,
                                            retryAttempt + 1,
                                            loopFuture);
                                },
                                processingPool);
                    } else {
                        log.error("Max retry attempts reached. Stopping consumer.");
                        loopFuture.completeExceptionally(e);
                    }
                    return null;
                });
    }

    private static CompletableFuture<Void> processMessages(
            PolledMessages polled, AtomicInteger totalReceived, ExecutorService processingPool) {
        // Process each message (this runs on processingPool, not Netty threads)
        return CompletableFuture.runAsync(
                () -> {
                    int messageCount = polled.messages().size();

                    for (Message message : polled.messages()) {
                        String payload = new String(message.payload());

                        // Simulate message processing (in real app: parse, validate, store, etc.)
                        // This could be CPU-intensive or involve blocking I/O (database, HTTP calls)
                        processMessage(payload, message.header().offset());
                    }

                    int total = totalReceived.addAndGet(messageCount);
                    log.info("Processed {} messages (total: {})", messageCount, total);
                },
                processingPool);
    }

    private static void processMessage(String payload, BigInteger offset) {
        // In a real application, this would be your business logic:
        //   - Parse JSON
        //   - Validate data
        //   - Call external APIs
        //   - Update database
        //   - Send to downstream systems

        // For this example, just log occasionally
        if (offset.compareTo(BigInteger.valueOf(5)) < 0
                || offset.mod(BigInteger.valueOf(100)).equals(BigInteger.ZERO)) {
            log.debug("Processed message at offset {}: '{}'", offset, payload);
        }

        // Simulate some processing time (remove in real app)
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
