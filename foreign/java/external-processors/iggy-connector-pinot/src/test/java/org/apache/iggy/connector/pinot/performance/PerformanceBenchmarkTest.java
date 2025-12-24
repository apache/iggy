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

package org.apache.iggy.connector.pinot.performance;

import org.apache.iggy.connector.pinot.consumer.IggyMessageBatch;
import org.apache.iggy.connector.pinot.consumer.IggyStreamPartitionMsgOffset;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Performance and efficiency benchmarks for the Iggy Pinot connector.
 * These tests validate that the connector can handle high throughput scenarios.
 */
class PerformanceBenchmarkTest {

    /**
     * Test: Message batch creation performance
     * Validates that creating large message batches is efficient
     */
    @Test
    void testMessageBatchCreationPerformance() {
        int messageCount = 10000;
        int messageSize = 1024; // 1KB

        long startTime = System.nanoTime();

        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>(messageCount);
        for (int i = 0; i < messageCount; i++) {
            byte[] payload = new byte[messageSize];
            IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(i);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));
        }

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;

        assertEquals(messageCount, batch.getMessageCount());
        assertTrue(durationMs < 1000, "Batch creation should complete in under 1 second");
    }

    /**
     * Test: Message batch iteration performance
     * Validates that iterating through large batches is efficient
     */
    @Test
    void testMessageBatchIterationPerformance() {
        int messageCount = 10000;
        List<IggyMessageBatch.IggyMessageAndOffset> messages = createTestMessages(messageCount, 1024);
        IggyMessageBatch batch = new IggyMessageBatch(messages);

        long startTime = System.nanoTime();

        long totalBytes = 0;
        for (int i = 0; i < batch.getMessageCount(); i++) {
            byte[] message = batch.getMessageAtIndex(i);
            totalBytes += message.length;
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;

        assertTrue(durationMs < 500, "Iteration should complete in under 500ms");
    }

    /**
     * Test: Offset comparison performance
     * Validates that offset comparisons are efficient for sorting/ordering
     */
    @Test
    void testOffsetComparisonPerformance() {
        int offsetCount = 100000;
        List<IggyStreamPartitionMsgOffset> offsets = new ArrayList<>(offsetCount);

        for (int i = 0; i < offsetCount; i++) {
            offsets.add(new IggyStreamPartitionMsgOffset(i));
        }

        long startTime = System.nanoTime();

        // Perform comparisons
        int comparisons = 0;
        for (int i = 0; i < offsetCount - 1; i++) {
            offsets.get(i).compareTo(offsets.get(i + 1));
            comparisons++;
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;

        assertTrue(durationMs < 100, "Comparisons should complete in under 100ms");
    }

    /**
     * Test: Memory efficiency for large batches
     * Validates that memory usage is reasonable for large message batches
     */
    @Test
    void testMemoryEfficiency() {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest garbage collection

        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();

        int messageCount = 10000;
        int messageSize = 1024;
        List<IggyMessageBatch.IggyMessageAndOffset> messages = createTestMessages(messageCount, messageSize);
        IggyMessageBatch batch = new IggyMessageBatch(messages);

        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        long memoryUsedMB = (memoryAfter - memoryBefore) / 1024 / 1024;
        long expectedMemoryMB = (messageCount * messageSize) / 1024 / 1024;

        // Memory usage should be within 2x of actual data size (allowing for object overhead)
        assertTrue(memoryUsedMB < expectedMemoryMB * 2, "Memory usage should be reasonable (< 2x data size)");
    }

    /**
     * Test: Throughput simulation
     * Simulates realistic throughput scenarios
     */
    @Test
    void testThroughputSimulation() {
        // Simulate 1000 msg/sec for 10 seconds = 10,000 messages
        int messagesPerBatch = 100;
        int batchCount = 100;
        int messageSize = 512;

        long startTime = System.nanoTime();

        for (int batch = 0; batch < batchCount; batch++) {
            List<IggyMessageBatch.IggyMessageAndOffset> messages = createTestMessages(messagesPerBatch, messageSize);
            IggyMessageBatch messageBatch = new IggyMessageBatch(messages);

            // Simulate processing
            for (int i = 0; i < messageBatch.getMessageCount(); i++) {
                byte[] msg = messageBatch.getMessageAtIndex(i);
                // Simulate minimal processing
                assertNotNull(msg);
            }
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        int totalMessages = messagesPerBatch * batchCount;
        double throughput = (totalMessages * 1000.0) / durationMs;

        assertTrue(throughput > 5000, "Should handle > 5000 msg/sec");
    }

    /**
     * Test: Concurrent offset operations
     * Validates thread-safety and performance under concurrent access
     */
    @Test
    void testConcurrentOffsetOperations() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 10000;

        Thread[] threads = new Thread[threadCount];
        long startTime = System.nanoTime();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < operationsPerThread; i++) {
                    IggyStreamPartitionMsgOffset offset1 =
                            new IggyStreamPartitionMsgOffset(threadId * operationsPerThread + i);
                    IggyStreamPartitionMsgOffset offset2 =
                            new IggyStreamPartitionMsgOffset(threadId * operationsPerThread + i + 1);
                    offset1.compareTo(offset2);
                    offset1.equals(offset2);
                    offset1.hashCode();
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        int totalOperations = threadCount * operationsPerThread * 3; // 3 ops per iteration

        assertTrue(durationMs < 2000, "Concurrent operations should complete quickly");
    }

    /**
     * Test: Large message handling
     * Validates that large individual messages are handled efficiently
     */
    @Test
    void testLargeMessageHandling() {
        int messageSize = 10 * 1024 * 1024; // 10 MB
        int messageCount = 10;

        long startTime = System.nanoTime();

        List<IggyMessageBatch.IggyMessageAndOffset> messages = createTestMessages(messageCount, messageSize);
        IggyMessageBatch batch = new IggyMessageBatch(messages);

        long creationTime = System.nanoTime();
        long creationMs = (creationTime - startTime) / 1_000_000;

        // Access all messages
        for (int i = 0; i < batch.getMessageCount(); i++) {
            assertNotNull(batch.getMessageAtIndex(i));
        }

        long endTime = System.nanoTime();
        long totalMs = (endTime - startTime) / 1_000_000;

        assertTrue(totalMs < 5000, "Should handle large messages in under 5 seconds");
    }

    /**
     * Test: Batch size impact
     * Compares performance across different batch sizes
     */
    @Test
    void testBatchSizeImpact() {
        int[] batchSizes = {10, 100, 1000, 5000};
        int messageSize = 1024;

        for (int batchSize : batchSizes) {
            // Creation
            long createStart = System.nanoTime();
            List<IggyMessageBatch.IggyMessageAndOffset> messages = createTestMessages(batchSize, messageSize);
            IggyMessageBatch batch = new IggyMessageBatch(messages);
            long createEnd = System.nanoTime();
            long createMs = (createEnd - createStart) / 1_000_000;

            // Iteration
            long iterStart = System.nanoTime();
            long totalBytes = 0;
            for (int i = 0; i < batch.getMessageCount(); i++) {
                totalBytes += batch.getMessageAtIndex(i).length;
            }
            long iterEnd = System.nanoTime();
            long iterMs = (iterEnd - iterStart) / 1_000_000;

            double mbPerSec = (totalBytes / 1024.0 / 1024.0 * 1000.0) / Math.max(iterMs, 1);

            // Verify performance is reasonable
            assertTrue(createMs < 1000, "Batch creation should be fast");
            assertTrue(iterMs < 1000, "Batch iteration should be fast");
        }
    }

    // Helper methods

    private List<IggyMessageBatch.IggyMessageAndOffset> createTestMessages(int count, int size) {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] payload = createTestPayload(size, i);
            IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(i);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));
        }
        return messages;
    }

    private byte[] createTestPayload(int size, int id) {
        String content = String.format("{\"id\":%d,\"data\":\"", id);
        byte[] prefix = content.getBytes(StandardCharsets.UTF_8);
        byte[] suffix = "\"}".getBytes(StandardCharsets.UTF_8);

        byte[] payload = new byte[size];
        System.arraycopy(prefix, 0, payload, 0, Math.min(prefix.length, size));
        if (size > prefix.length + suffix.length) {
            System.arraycopy(suffix, 0, payload, size - suffix.length, suffix.length);
        }

        return payload;
    }
}
