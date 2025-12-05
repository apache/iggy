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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.pinot.config.IggyStreamConfig;
import org.apache.iggy.connector.pinot.consumer.IggyStreamPartitionMsgOffset;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.MessageState;
import org.apache.iggy.stats.TopicStats;
import org.apache.iggy.topic.Topic;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final String clientId;
    private final IggyStreamConfig config;
    private final Integer partitionId; // null for stream-level, non-null for partition-level

    private AsyncIggyTcpClient asyncClient;
    private StreamId streamId;
    private TopicId topicId;
    private TopicStats cachedTopicStats;
    private long lastStatsRefresh;
    private static final long STATS_CACHE_MS = 5000; // 5 seconds cache

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
        this.clientId = clientId;
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
     * @return list of partition metadata
     * @throws IOException if metadata retrieval fails
     */
    @Override
    public List<PartitionGroupMetadata> computePartitionGroupMetadata(
            String clientId, StreamConfig streamConfig, List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList, long timeoutMillis)
            throws IOException {
        try {
            ensureConnected();

            Topic topic = fetchTopicInfo();
            int partitionCount = topic.partitionsCount();

            log.info("Found {} partitions for topic {}", partitionCount, config.getTopicId());

            List<PartitionGroupMetadata> metadataList = new ArrayList<>();
            for (int i = 0; i < partitionCount; i++) {
                metadataList.add(new PartitionGroupMetadata(i, new IggyStreamPartitionMsgOffset(0)));
            }

            return metadataList;

        } catch (Exception e) {
            log.error("Error computing partition metadata: {}", e.getMessage(), e);
            throw new IOException("Failed to compute partition metadata", e);
        }
    }

    /**
     * Fetches the current offset for consumption.
     * For Iggy, we rely on consumer group state, so this returns the earliest offset.
     *
     * @param partition partition identifier
     * @param timeoutMillis timeout for the operation
     * @return current offset for the partition
     * @throws IOException if fetch fails
     */
    @Override
    public StreamPartitionMsgOffset fetchStreamPartitionOffset(
            PartitionGroupMetadata partition, long timeoutMillis) throws IOException {
        try {
            ensureConnected();

            // For consumer group based consumption, return earliest offset
            // The consumer group will manage the actual offset
            return new IggyStreamPartitionMsgOffset(0);

        } catch (Exception e) {
            log.error("Error fetching partition offset: {}", e.getMessage(), e);
            throw new IOException("Failed to fetch partition offset", e);
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
                    .connectionPoolSize(config.getConnectionPoolSize())
                    .build();

            // Connect and authenticate
            asyncClient.connect().join();

            // Parse stream and topic IDs
            streamId = parseStreamId(config.getStreamId());
            topicId = parseTopicId(config.getTopicId());

            log.info("Connected to Iggy server successfully");
        }
    }

    /**
     * Fetches topic information from Iggy.
     */
    private Topic fetchTopicInfo() {
        try {
            return asyncClient.topics().getTopic(streamId, topicId).join();
        } catch (Exception e) {
            log.error("Error fetching topic info: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to fetch topic info", e);
        }
    }

    /**
     * Fetches topic statistics with caching.
     */
    private TopicStats fetchTopicStats() {
        long now = System.currentTimeMillis();
        if (cachedTopicStats == null || (now - lastStatsRefresh) > STATS_CACHE_MS) {
            try {
                cachedTopicStats = asyncClient.topics().getTopicStats(streamId, topicId).join();
                lastStatsRefresh = now;
            } catch (Exception e) {
                log.error("Error fetching topic stats: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to fetch topic stats", e);
            }
        }
        return cachedTopicStats;
    }

    /**
     * Gets the number of partitions in the topic.
     */
    public int getPartitionCount() {
        ensureConnected();
        Topic topic = fetchTopicInfo();
        return topic.partitionsCount();
    }

    /**
     * Gets statistics for a specific partition.
     */
    public MessageState getPartitionStats(int partitionId) {
        ensureConnected();
        TopicStats stats = fetchTopicStats();

        Optional<MessageState> partitionStats =
                stats.partitions().stream().filter(p -> p.id() == partitionId).findFirst();

        return partitionStats.orElseThrow(
                () -> new IllegalArgumentException("Partition " + partitionId + " not found"));
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
    public void close() throws IOException {
        if (asyncClient != null) {
            try {
                log.info("Closing Iggy metadata provider");
                asyncClient.close().join();
                log.info("Iggy metadata provider closed successfully");
            } catch (Exception e) {
                log.error("Error closing Iggy client: {}", e.getMessage(), e);
            } finally {
                asyncClient = null;
            }
        }
    }
}
