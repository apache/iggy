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

package org.apache.iggy.connector.flink.sink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.connector.error.ConnectorException;
import org.apache.iggy.connector.serialization.SerializationSchema;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.MessageId;
import org.apache.iggy.message.Partitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Sink writer implementation for writing records to Iggy.
 * Buffers records and flushes them in batches for efficiency.
 */
public class IggySinkWriter<T> implements SinkWriter<T> {

    private static final Logger log = LoggerFactory.getLogger(IggySinkWriter.class);

    private final IggyHttpClient httpClient;
    private final String streamId;
    private final String topicId;
    private final SerializationSchema<T> serializer;
    private final int batchSize;
    private final Duration flushInterval;
    private final PartitioningStrategy partitioningStrategy;

    private final List<T> buffer;
    private long lastFlushTime;
    private long totalWritten;

    /**
     * Strategy for determining partition assignment.
     */
    public enum PartitioningStrategy {
        BALANCED,
        PARTITION_ID,
        MESSAGE_KEY
    }

    /**
     * Creates a new sink writer.
     *
     * @param httpClient           the HTTP Iggy client
     * @param streamId             the stream identifier
     * @param topicId              the topic identifier
     * @param serializer           the serialization schema
     * @param batchSize            maximum number of records to buffer before flushing
     * @param flushInterval        maximum time to wait before flushing
     * @param partitioningStrategy the partitioning strategy
     */
    public IggySinkWriter(
            IggyHttpClient httpClient,
            String streamId,
            String topicId,
            SerializationSchema<T> serializer,
            int batchSize,
            Duration flushInterval,
            PartitioningStrategy partitioningStrategy) {

        if (httpClient == null) {
            throw new IllegalArgumentException("httpClient cannot be null");
        }
        if (StringUtils.isBlank(streamId)) {
            throw new IllegalArgumentException("streamId cannot be null or empty");
        }
        if (StringUtils.isBlank(topicId)) {
            throw new IllegalArgumentException("topicId cannot be null or empty");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("serializer cannot be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        if (flushInterval == null || flushInterval.isNegative()) {
            throw new IllegalArgumentException("flushInterval must be positive");
        }

        this.httpClient = httpClient;
        this.streamId = streamId;
        this.topicId = topicId;
        this.serializer = serializer;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.partitioningStrategy = partitioningStrategy != null ? partitioningStrategy : PartitioningStrategy.BALANCED;

        this.buffer = new ArrayList<>(batchSize);
        this.lastFlushTime = System.currentTimeMillis();
        this.totalWritten = 0;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        log.debug("IggySinkWriter.write() called - element: {}, buffer size: {}", element, buffer.size());
        buffer.add(element);

        // Flush if batch size reached or flush interval exceeded
        if (buffer.size() >= batchSize || shouldFlushByTime()) {
            log.debug("IggySinkWriter: Flushing buffer of size {}", buffer.size());
            flush(false);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (buffer.isEmpty()) {
            log.debug("IggySinkWriter.flush() - buffer is empty, skipping");
            return;
        }

        log.debug(
                "IggySinkWriter.flush() - flushing {} messages to stream={}, topic={}",
                buffer.size(),
                streamId,
                topicId);

        try {
            // Serialize all buffered records
            List<Message> messages = new ArrayList<>(buffer.size());
            for (T element : buffer) {
                byte[] payload = serializer.serialize(element);
                // Use createMessage to avoid String conversion
                messages.add(createMessage(payload));
            }

            // Determine partitioning
            Partitioning partitioning = determinePartitioning();

            // Send messages to Iggy via HTTP
            StreamId stream = parseStreamId(streamId);
            TopicId topic = parseTopicId(topicId);

            log.debug("IggySinkWriter: Sending {} messages with partitioning={}", messages.size(), partitioning);
            httpClient.messages().sendMessages(stream, topic, partitioning, messages);

            totalWritten += buffer.size();
            log.debug("IggySinkWriter: Successfully sent {} messages. Total written: {}", buffer.size(), totalWritten);

            // Clear buffer and update flush time
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

        } catch (RuntimeException e) {
            log.error("IggySinkWriter.flush() - ERROR: {}", e.getMessage(), e);
            throw new ConnectorException(
                    "Failed to send messages to Iggy", e, ConnectorException.ErrorCode.RESOURCE_EXHAUSTED, true);
        }
    }

    @Override
    public void close() throws Exception {
        // Flush any remaining buffered records
        flush(true);
        // Note: HTTP client doesn't have close() method - connections managed by Java HttpClient pool
    }

    /**
     * Checks if flush should occur based on time interval.
     *
     * @return true if flush interval has been exceeded
     */
    private boolean shouldFlushByTime() {
        long timeSinceLastFlush = System.currentTimeMillis() - lastFlushTime;
        return timeSinceLastFlush >= flushInterval.toMillis();
    }

    /**
     * Determines the partitioning strategy for the current batch.
     *
     * @return the partitioning configuration
     */
    private Partitioning determinePartitioning() {
        switch (partitioningStrategy) {
            case BALANCED:
                return Partitioning.balanced();

            case PARTITION_ID:
                // Use first element's partition key if available
                if (!buffer.isEmpty()) {
                    Optional<Integer> partitionKey = serializer.extractPartitionKey(buffer.get(0));
                    if (partitionKey.isPresent()) {
                        return Partitioning.partitionId(partitionKey.get().longValue());
                    }
                }
                return Partitioning.balanced();

            case MESSAGE_KEY:
                // Use first element's partition key as message key
                if (!buffer.isEmpty()) {
                    Optional<Integer> partitionKey = serializer.extractPartitionKey(buffer.get(0));
                    if (partitionKey.isPresent()) {
                        return Partitioning.messagesKey(String.valueOf(partitionKey.get()));
                    }
                }
                return Partitioning.balanced();

            default:
                return Partitioning.balanced();
        }
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

    /**
     * Gets the total number of records written.
     *
     * @return total records written
     */
    public long getTotalWritten() {
        return totalWritten;
    }

    /**
     * Gets the current buffer size.
     *
     * @return number of buffered records
     */
    public int getBufferSize() {
        return buffer.size();
    }

    /**
     * Creates a Message from raw bytes without String conversion.
     * This avoids charset encoding issues that can corrupt binary data.
     *
     * @param payload the message payload as bytes
     * @return a Message instance
     */
    private Message createMessage(byte[] payload) {
        MessageHeader header = new MessageHeader(
                BigInteger.ZERO,
                MessageId.serverGenerated(),
                BigInteger.ZERO, // offset
                BigInteger.ZERO, // timestamp
                BigInteger.ZERO, // originTimestamp
                0L, // userHeadersLength
                (long) payload.length); // payloadLength

        return new Message(header, payload, Optional.empty());
    }
}
