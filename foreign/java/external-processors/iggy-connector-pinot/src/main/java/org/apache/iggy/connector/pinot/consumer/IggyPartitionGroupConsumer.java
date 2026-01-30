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

package org.apache.iggy.connector.pinot.consumer;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.pinot.config.IggyStreamConfig;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Partition-level consumer implementation for Iggy streams.
 * Reads messages from a single Iggy partition using the AsyncIggyTcpClient.
 *
 * <p>This consumer manages:
 * <ul>
 *   <li>TCP connection to Iggy server</li>
 *   <li>Single consumer mode (not consumer groups)</li>
 *   <li>Message polling with explicit offset tracking</li>
 *   <li>Offset management controlled by Pinot</li>
 * </ul>
 */
public class IggyPartitionGroupConsumer implements PartitionGroupConsumer {

    private static final Logger log = LoggerFactory.getLogger(IggyPartitionGroupConsumer.class);

    private final IggyStreamConfig config;
    private final int partitionId;

    private AsyncIggyTcpClient asyncClient;
    private StreamId streamId;
    private TopicId topicId;
    private Consumer consumer;
    private long currentOffset;

    /**
     * Creates a new partition consumer.
     *
     * @param clientId unique identifier for this consumer
     * @param config Iggy stream configuration
     * @param partitionId the partition to consume from
     */
    public IggyPartitionGroupConsumer(String clientId, IggyStreamConfig config, int partitionId) {
        this.config = config;
        this.partitionId = partitionId;
        this.currentOffset = 0;

        log.info(
                "Created IggyPartitionGroupConsumer: clientId={}, partition={}, config={}",
                clientId,
                partitionId,
                config);
    }

    /**
     * Fetches the next batch of messages from the Iggy partition.
     * This method is called repeatedly by Pinot to poll for new messages.
     *
     * @param startOffset the offset to start consuming from (may be null)
     * @param timeoutMillis timeout for the fetch operation
     * @return batch of messages, or empty batch if no messages available
     */
    @Override
    public MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMillis) {
        try {
            ensureConnected();

            // No need to join consumer group when using single consumer

            // Determine starting offset
            long fetchOffset = determineStartOffset(startOffset);
            log.debug("Fetching messages from partition {} at offset {}", partitionId, fetchOffset);

            // Poll messages from Iggy
            PolledMessages polledMessages = pollMessages(fetchOffset);
            log.debug(
                    "Polled {} messages from partition {}",
                    polledMessages.messages().size(),
                    partitionId);

            // Convert to Pinot MessageBatch
            MessageBatch batch = convertToMessageBatch(polledMessages);
            return batch;

        } catch (RuntimeException e) {
            log.error("Error fetching messages from partition {}: {}", partitionId, e.getMessage(), e);
            return new IggyMessageBatch(new ArrayList<>());
        }
    }

    /**
     * Ensures TCP connection to Iggy server is established.
     */
    private void ensureConnected() {
        if (asyncClient == null) {
            log.info("Connecting to Iggy server: {}", config.getServerAddress());

            asyncClient = AsyncIggyTcpClient.builder()
                    .host(config.getHost())
                    .port(config.getPort())
                    .credentials(config.getUsername(), config.getPassword())
                    .buildAndLogin()
                    .join();

            // Parse stream and topic IDs
            streamId = parseStreamId(config.getStreamId());
            topicId = parseTopicId(config.getTopicId());
            // Use single consumer instead of consumer group for explicit offset control
            consumer = Consumer.of(ConsumerId.of(Long.valueOf(partitionId)));

            log.info("Connected to Iggy server successfully");
        }
    }

    /**
     * Determines the starting offset for polling.
     */
    private long determineStartOffset(StreamPartitionMsgOffset startOffset) {
        if (startOffset != null && startOffset instanceof IggyStreamPartitionMsgOffset) {
            IggyStreamPartitionMsgOffset iggyOffset = (IggyStreamPartitionMsgOffset) startOffset;
            currentOffset = iggyOffset.getOffset();
            log.debug("Using provided start offset: {}", currentOffset);
            return currentOffset;
        }

        // Use current tracked offset when no explicit offset provided
        log.debug("Using current tracked offset for partition {}: {}", partitionId, currentOffset);
        return currentOffset;
    }

    /**
     * Polls messages from Iggy using TCP client.
     */
    private PolledMessages pollMessages(long fetchOffset) {
        try {
            Optional<Long> partition = Optional.of((long) partitionId);

            // Use explicit offset strategy to fetch from the offset Pinot requested
            PollingStrategy strategy = PollingStrategy.offset(java.math.BigInteger.valueOf(fetchOffset));

            log.debug(
                    "Polling messages: partition={}, offset={}, batchSize={}",
                    partitionId,
                    fetchOffset,
                    config.getPollBatchSize());

            // Poll with auto-commit disabled (we'll manage offsets via Pinot)
            PolledMessages polledMessages = asyncClient
                    .messages()
                    .pollMessages(
                            streamId,
                            topicId,
                            partition,
                            consumer,
                            strategy,
                            Long.valueOf(config.getPollBatchSize()),
                            false)
                    .join();

            log.debug(
                    "Polled {} messages from partition {}, currentOffset={}",
                    polledMessages.messages().size(),
                    partitionId,
                    polledMessages.currentOffset());

            // Update current offset only if we got messages
            if (!polledMessages.messages().isEmpty() && polledMessages.currentOffset() != null) {
                currentOffset = polledMessages.currentOffset().longValue() + 1;
            }

            return polledMessages;

        } catch (RuntimeException e) {
            log.error("Error polling messages: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to poll messages", e);
        }
    }

    /**
     * Converts Iggy PolledMessages to Pinot MessageBatch.
     */
    private MessageBatch convertToMessageBatch(PolledMessages polledMessages) {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();

        for (Message message : polledMessages.messages()) {
            long offset = message.header().offset().longValue();
            byte[] payload = message.payload();

            IggyStreamPartitionMsgOffset msgOffset = new IggyStreamPartitionMsgOffset(offset);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, msgOffset));
        }

        return new IggyMessageBatch(messages);
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

    @Override
    public void close() {
        if (asyncClient != null) {
            try {
                log.info("Closing Iggy consumer for partition {}", partitionId);
                asyncClient.close().join();
                log.info("Iggy consumer closed successfully");
            } catch (RuntimeException e) {
                log.error("Error closing Iggy client: {}", e.getMessage(), e);
            } finally {
                asyncClient = null;
            }
        }
    }
}
