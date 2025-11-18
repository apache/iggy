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

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.error.ConnectorException;
import org.apache.iggy.connector.serialization.DeserializationSchema;
import org.apache.iggy.connector.serialization.RecordMetadata;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads messages from a single Iggy partition.
 * Manages offset tracking and message deserialization.
 */
public class IggyPartitionSplitReader<T> {
    private static final Logger log = LoggerFactory.getLogger(IggyPartitionSplitReader.class);

    private final AsyncIggyTcpClient asyncClient;
    private final IggySourceSplit split;
    private final DeserializationSchema<T> deserializer;
    private final Consumer consumer;
    private final long pollBatchSize;

    private long currentOffset;
    private boolean hasMoreData;
    private boolean consumerGroupJoined;

    /**
     * Creates a new partition split reader.
     *
     * @param asyncClient   the async Iggy client
     * @param split         the source split to read from
     * @param deserializer  the deserialization schema
     * @param consumer      the consumer identifier
     * @param pollBatchSize number of messages to fetch per poll
     */
    public IggyPartitionSplitReader(
            AsyncIggyTcpClient asyncClient,
            IggySourceSplit split,
            DeserializationSchema<T> deserializer,
            Consumer consumer,
            long pollBatchSize) {

        if (asyncClient == null) {
            throw new IllegalArgumentException("asyncClient cannot be null");
        }
        if (split == null) {
            throw new IllegalArgumentException("split cannot be null");
        }
        if (deserializer == null) {
            throw new IllegalArgumentException("deserializer cannot be null");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer cannot be null");
        }
        if (pollBatchSize <= 0) {
            throw new IllegalArgumentException("pollBatchSize must be > 0");
        }

        this.asyncClient = asyncClient;
        this.split = split;
        this.deserializer = deserializer;
        this.consumer = consumer;
        this.pollBatchSize = pollBatchSize;
        this.currentOffset = split.getCurrentOffset();
        this.hasMoreData = true;
        this.consumerGroupJoined = false;
    }

    /**
     * Polls messages from the partition.
     *
     * @return list of deserialized records, empty if no messages available
     * @throws ConnectorException if polling or deserialization fails
     */
    public List<T> poll() {
        // For unbounded streams, always poll (don't check hasMoreData)

        try {
            StreamId streamId = parseStreamId(split.getStreamId());
            TopicId topicId = parseTopicId(split.getTopicId());

            // Join consumer group on first poll (idempotent operation in Iggy)
            if (!consumerGroupJoined) {
                log.info(
                        "IggyPartitionSplitReader: Joining consumer group for stream={}, "
                                + "topic={}, consumer={}, partition={}",
                        split.getStreamId(),
                        split.getTopicId(),
                        consumer.id(),
                        split.getPartitionId());
                asyncClient
                        .consumerGroups()
                        .joinConsumerGroup(streamId, topicId, consumer.id())
                        .join();
                consumerGroupJoined = true;
                log.info("IggyPartitionSplitReader: Successfully joined consumer group");
            }
            Optional<Long> partitionId = Optional.of((long) split.getPartitionId());

            // CRITICAL FIX: Use consumer group managed offset (next available message)
            // When using consumer groups, Iggy manages offsets automatically.
            // Using PollingStrategy.offset() conflicts with consumer group offset management.
            PollingStrategy strategy = PollingStrategy.next();

            log.info(
                    "IggyPartitionSplitReader: Polling partition={}, "
                            + "strategy=NEXT (consumer-group-managed), batchSize={}",
                    split.getPartitionId(),
                    pollBatchSize);

            // CRITICAL FIX: Enable autoCommit to advance consumer group offset after each poll
            // Without this, offset never advances and we read the same messages repeatedly
            // Poll messages from Iggy (async with blocking)
            PolledMessages polledMessages = asyncClient
                    .messages()
                    .pollMessagesAsync(streamId, topicId, partitionId, consumer, strategy, pollBatchSize, true)
                    .join();

            log.info(
                    "IggyPartitionSplitReader: Polled partition={}, messagesCount={}, " + "currentOffset={}",
                    split.getPartitionId(),
                    polledMessages.messages().size(),
                    polledMessages.currentOffset() != null
                            ? polledMessages.currentOffset().longValue()
                            : "null");

            if (polledMessages.messages().isEmpty()) {
                // For unbounded streams, keep polling even if no messages available
                // Don't set hasMoreData = false
                return List.of();
            }

            // Deserialize messages
            List<T> records = new ArrayList<>(polledMessages.messages().size());
            for (Message message : polledMessages.messages()) {
                T record = deserializeMessage(message);
                if (record != null) {
                    log.info(
                            "IggyPartitionSplitReader: Deserialized message at " + "offset={}, record={}",
                            message.header().offset(),
                            record);
                    records.add(record);
                }
            }

            // Update current offset
            if (polledMessages.currentOffset() != null) {
                currentOffset = polledMessages.currentOffset().longValue() + 1;
                log.info("IggyPartitionSplitReader: Updated currentOffset to {}", currentOffset);
            }

            return records;

        } catch (RuntimeException e) {
            throw new ConnectorException(
                    "Failed to poll messages from partition " + split.getPartitionId(),
                    e,
                    ConnectorException.ErrorCode.RESOURCE_EXHAUSTED,
                    true);
        }
    }

    /**
     * Deserializes a single message.
     *
     * @param message the Iggy message
     * @return deserialized record, or null if deserialization fails (logs warning)
     */
    private T deserializeMessage(Message message) {
        try {
            RecordMetadata metadata = RecordMetadata.of(
                    split.getStreamId(),
                    split.getTopicId(),
                    split.getPartitionId(),
                    message.header().offset().longValue());

            return deserializer.deserialize(message.payload(), metadata);

        } catch (IOException e) {
            // Log warning and skip this message (can be made configurable)
            log.warn(
                    "Failed to deserialize message at offset {}: {}",
                    message.header().offset(),
                    e.getMessage(),
                    e);
            return null;
        }
    }

    /**
     * Gets the current offset position.
     *
     * @return current offset
     */
    public long getCurrentOffset() {
        return currentOffset;
    }

    /**
     * Gets the source split being read.
     *
     * @return the source split
     */
    public IggySourceSplit getSplit() {
        return split.withCurrentOffset(currentOffset);
    }

    /**
     * Checks if there is potentially more data to read.
     *
     * @return true if more data might be available
     */
    public boolean hasMoreData() {
        return hasMoreData;
    }

    /**
     * Parses stream ID from string (supports both numeric and named streams).
     */
    private StreamId parseStreamId(String streamIdStr) {
        try {
            return StreamId.of(Long.parseLong(streamIdStr));
        } catch (NumberFormatException e) {
            return StreamId.of(streamIdStr);
        }
    }

    /**
     * Parses topic ID from string (supports both numeric and named topics).
     */
    private TopicId parseTopicId(String topicIdStr) {
        try {
            return TopicId.of(Long.parseLong(topicIdStr));
        } catch (NumberFormatException e) {
            return TopicId.of(topicIdStr);
        }
    }
}
