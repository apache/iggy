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

package org.apache.iggy.connector.pinot.metadata;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.pinot.config.IggyStreamConfig;
import org.apache.iggy.connector.pinot.consumer.IggyStreamPartitionMsgOffset;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.topic.TopicDetails;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Metadata provider for Iggy streams.
 * Provides information about partitions, offsets, and message counts.
 *
 * <p>This provider connects to Iggy via TCP to query:
 * <ul>
 *   <li>Number of partitions in a topic</li>
 *   <li>Oldest available offset per partition</li>
 *   <li>Latest offset per partition</li>
 *   <li>Message counts</li>
 * </ul>
 */
public class IggyStreamMetadataProvider implements StreamMetadataProvider {

    private static final Logger log = LoggerFactory.getLogger(IggyStreamMetadataProvider.class);

    private static final long DETAILS_CACHE_MS = 5000; // 5 seconds cache

    private final IggyStreamConfig config;
    private final Integer partitionId; // null for stream-level, non-null for partition-level

    private AsyncIggyTcpClient asyncClient;
    private StreamId streamId;
    private TopicId topicId;
    private TopicDetails cachedTopicDetails;
    private long lastDetailsRefresh;

    /**
     * Creates a stream-level metadata provider (all partitions).
     *
     * @param clientId unique identifier
     * @param config Iggy stream configuration
     */
    public IggyStreamMetadataProvider(String clientId, IggyStreamConfig config) {
        this(clientId, config, null);
    }

    /**
     * Creates a partition-level metadata provider.
     *
     * @param clientId unique identifier
     * @param config Iggy stream configuration
     * @param partitionId specific partition ID
     */
    public IggyStreamMetadataProvider(String clientId, IggyStreamConfig config, Integer partitionId) {
        this.config = config;
        this.partitionId = partitionId;

        log.info(
                "Created IggyStreamMetadataProvider: clientId={}, partitionId={}, config={}",
                clientId,
                partitionId,
                config);
    }

    /**
     * Retrieves the number of partitions and their metadata.
     * Called by Pinot to discover available partitions in the stream.
     *
     * @param timeoutMillis timeout for the operation
     * @return number of partitions in the topic
     */
    @Override
    public int fetchPartitionCount(long timeoutMillis) {
        try {
            ensureConnected();
            TopicDetails topicDetails = fetchTopicDetails();
            int partitionCount = topicDetails.partitionsCount().intValue();
            log.info("Found {} partitions for topic {}", partitionCount, config.getTopicId());
            return partitionCount;
        } catch (RuntimeException e) {
            log.error("Error fetching partition count: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to fetch partition count", e);
        }
    }

    /**
     * Computes one partition group per Iggy partition.
     * A partition that Pinot already consumes keeps the end offset of its latest segment.
     * A new partition starts at the offset that the stream offset criteria selects.
     *
     * @param clientId unique identifier for this metadata provider instance
     * @param streamConfig Pinot stream configuration
     * @param partitionGroupConsumptionStatuses consumption status of the current partition groups
     * @param timeoutMillis timeout for the operation
     * @return metadata for every partition in the topic
     */
    @Override
    public List<PartitionGroupMetadata> computePartitionGroupMetadata(
            String clientId,
            StreamConfig streamConfig,
            List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses,
            int timeoutMillis) {
        Map<Integer, StreamPartitionMsgOffset> endOffsets = new HashMap<>(partitionGroupConsumptionStatuses.size());
        for (PartitionGroupConsumptionStatus status : partitionGroupConsumptionStatuses) {
            endOffsets.put(status.getStreamPartitionGroupId(), status.getEndOffset());
        }

        int partitionCount = fetchPartitionCount(timeoutMillis);
        List<PartitionGroupMetadata> partitionGroups = new ArrayList<>(partitionCount);
        for (int streamPartitionId = 0; streamPartitionId < partitionCount; streamPartitionId++) {
            // Pinot passes a null end offset for a partition group that is still consuming, and expects it back as is.
            StreamPartitionMsgOffset startOffset = endOffsets.containsKey(streamPartitionId)
                    ? endOffsets.get(streamPartitionId)
                    : resolveOffset(streamPartitionId, streamConfig.getOffsetCriteria());
            partitionGroups.add(new PartitionGroupMetadata(streamPartitionId, startOffset));
        }
        return partitionGroups;
    }

    /**
     * Fetches the offset that the offset criteria selects for this partition.
     * The {@code largest} criteria gives the next offset to read. Every other criteria gives offset 0,
     * where a poll starts at the first retained message.
     *
     * @param offsetCriteria offset criteria (earliest, latest, etc.)
     * @param timeoutMillis timeout for the operation
     * @return current offset for the partition
     */
    @Override
    public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis) {
        try {
            ensureConnected();

            if (partitionId == null) {
                throw new IllegalStateException("Partition ID must be set for offset queries");
            }

            return resolveOffset(partitionId, offsetCriteria);

        } catch (RuntimeException e) {
            log.error("Error fetching partition offset: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to fetch partition offset", e);
        }
    }

    /**
     * Resolves the offset that the offset criteria selects for a partition.
     */
    private StreamPartitionMsgOffset resolveOffset(int streamPartitionId, OffsetCriteria offsetCriteria) {
        Partition partition = getPartitionInfo(streamPartitionId);

        if (offsetCriteria != null && offsetCriteria.isLargest()) {
            return new IggyStreamPartitionMsgOffset(nextOffset(partition));
        }
        return new IggyStreamPartitionMsgOffset(0);
    }

    /**
     * Returns the offset that the next message appended to the partition gets.
     */
    static long nextOffset(Partition partition) {
        // The server reports the offset of the last message, and 0 also for an empty partition.
        // The messages count cannot be the next offset, because retention lowers it.
        long currentOffset = partition.currentOffset().longValue();
        if (currentOffset == 0 && partition.messagesCount().signum() == 0) {
            return 0;
        }
        return currentOffset + 1;
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

            log.info("Connected to Iggy server successfully");
        }
    }

    /**
     * Fetches topic details with caching.
     */
    private TopicDetails fetchTopicDetails() {
        long now = System.currentTimeMillis();
        if (cachedTopicDetails == null || (now - lastDetailsRefresh) > DETAILS_CACHE_MS) {
            try {
                Optional<TopicDetails> details =
                        asyncClient.topics().getTopic(streamId, topicId).join();
                cachedTopicDetails =
                        details.orElseThrow(() -> new RuntimeException("Topic not found: " + config.getTopicId()));
                lastDetailsRefresh = now;
            } catch (RuntimeException e) {
                log.error("Error fetching topic details: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to fetch topic details", e);
            }
        }
        return cachedTopicDetails;
    }

    /**
     * Gets information for a specific partition.
     */
    private Partition getPartitionInfo(int partitionId) {
        TopicDetails details = fetchTopicDetails();
        return details.partitions().stream()
                .filter(p -> p.id().intValue() == partitionId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Partition " + partitionId + " not found"));
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
    public boolean supportsOffsetLag() {
        return false;
    }

    @Override
    public void close() {
        if (asyncClient != null) {
            try {
                log.info("Closing Iggy metadata provider");
                asyncClient.close().join();
                log.info("Iggy metadata provider closed successfully");
            } catch (RuntimeException e) {
                log.error("Error closing Iggy client: {}", e.getMessage(), e);
            } finally {
                asyncClient = null;
            }
        }
    }
}
