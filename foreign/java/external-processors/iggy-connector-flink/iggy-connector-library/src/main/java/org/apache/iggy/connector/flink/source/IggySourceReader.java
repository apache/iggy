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

package org.apache.iggy.connector.flink.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.serialization.DeserializationSchema;
import org.apache.iggy.consumergroup.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Flink SourceReader implementation for Iggy.
 * Manages multiple partition split readers and coordinates reading from them.
 */
public class IggySourceReader<T> implements SourceReader<T, IggySourceSplit> {

    private static final Logger log = LoggerFactory.getLogger(IggySourceReader.class);

    private final SourceReaderContext context;
    private final AsyncIggyTcpClient asyncClient;
    private final DeserializationSchema<T> deserializer;
    private final Consumer consumer;
    private final long pollBatchSize;

    private final Map<String, IggyPartitionSplitReader<T>> splitReaders;
    private final Queue<T> recordQueue;

    private boolean noMoreSplits;

    /**
     * Creates a new Iggy source reader.
     *
     * @param context the source reader context
     * @param asyncClient the async Iggy client
     * @param deserializer the deserialization schema
     * @param consumer the consumer identifier
     * @param pollBatchSize number of messages to fetch per poll
     */
    public IggySourceReader(
            SourceReaderContext context,
            AsyncIggyTcpClient asyncClient,
            DeserializationSchema<T> deserializer,
            Consumer consumer,
            long pollBatchSize) {

        this.context = context;
        this.asyncClient = asyncClient;
        this.deserializer = deserializer;
        this.consumer = consumer;
        this.pollBatchSize = pollBatchSize;
        this.splitReaders = new HashMap<>();
        this.recordQueue = new ArrayDeque<>();
        this.noMoreSplits = false;
    }

    @Override
    public void start() {
        // Initialization if needed
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        // If we have records in the queue, emit them
        if (!recordQueue.isEmpty()) {
            T record = recordQueue.poll();
            output.collect(record);
            return InputStatus.MORE_AVAILABLE;
        }

        // Poll from all split readers
        boolean hasData = false;
        for (IggyPartitionSplitReader<T> splitReader : splitReaders.values()) {
            List<T> records = splitReader.poll();
            if (!records.isEmpty()) {
                recordQueue.addAll(records);
                hasData = true;
            }
        }

        // If we got data, emit the first record
        if (hasData) {
            T record = recordQueue.poll();
            output.collect(record);
            return InputStatus.MORE_AVAILABLE;
        }

        // No data available
        if (noMoreSplits && splitReaders.isEmpty()) {
            return InputStatus.END_OF_INPUT;
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<IggySourceSplit> snapshotState(long checkpointId) {
        // Return current state of all splits for checkpointing
        List<IggySourceSplit> splits = new ArrayList<>();
        for (IggyPartitionSplitReader<T> splitReader : splitReaders.values()) {
            splits.add(splitReader.getSplit());
        }
        return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // If we have data in queue, immediately available
        if (!recordQueue.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Check if any split reader has more data
        for (IggyPartitionSplitReader<T> splitReader : splitReaders.values()) {
            if (splitReader.hasMoreData()) {
                return CompletableFuture.completedFuture(null);
            }
        }

        // No data available right now, return a future that will complete later
        // For simplicity, we return completed future and rely on polling
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<IggySourceSplit> splits) {
        for (IggySourceSplit split : splits) {
            String splitId = split.splitId();
            if (!splitReaders.containsKey(splitId)) {
                IggyPartitionSplitReader<T> splitReader =
                        new IggyPartitionSplitReader<>(asyncClient, split, deserializer, consumer, pollBatchSize);
                splitReaders.put(splitId, splitReader);
            }
        }
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        // Handle custom source events if needed
    }

    @Override
    public void close() throws Exception {
        // Clean up resources
        splitReaders.clear();
        recordQueue.clear();

        // CRITICAL FIX: Close async client to shutdown EventLoopGroup and prevent memory leak
        if (asyncClient != null) {
            try {
                asyncClient.close().join();
            } catch (RuntimeException e) {
                // Log but don't throw - we're in cleanup phase
                log.error("Error closing async Iggy client: " + e.getMessage());
            }
        }
    }
}
