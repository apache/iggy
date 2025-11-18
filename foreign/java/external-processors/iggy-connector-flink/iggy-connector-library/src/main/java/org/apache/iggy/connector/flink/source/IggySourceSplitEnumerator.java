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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.config.OffsetConfig;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.topic.TopicDetails;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Split enumerator for Iggy source.
 * Discovers partitions and assigns them to source readers.
 */
public class IggySourceSplitEnumerator implements SplitEnumerator<IggySourceSplit, IggySourceEnumeratorState> {

    private final SplitEnumeratorContext<IggySourceSplit> context;
    private final AsyncIggyTcpClient asyncClient;
    private final String streamId;
    private final String topicId;
    private final String consumerGroupName;
    private final OffsetConfig offsetConfig;

    private final Set<IggySourceSplit> assignedSplits;
    private final Set<Integer> discoveredPartitions;
    private final Set<String> sentToReaders; // Track splits already sent to readers

    /**
     * Creates a new split enumerator.
     *
     * @param context           the split enumerator context
     * @param asyncClient       the async Iggy client
     * @param streamId          the stream ID
     * @param topicId           the topic ID
     * @param consumerGroupName the consumer group name
     * @param offsetConfig      the offset configuration
     * @param initialState      the initial state (for recovery)
     */
    public IggySourceSplitEnumerator(
            SplitEnumeratorContext<IggySourceSplit> context,
            AsyncIggyTcpClient asyncClient,
            String streamId,
            String topicId,
            String consumerGroupName,
            OffsetConfig offsetConfig,
            @Nullable IggySourceEnumeratorState initialState) {

        this.context = context;
        this.asyncClient = asyncClient;
        this.streamId = streamId;
        this.topicId = topicId;
        this.consumerGroupName = consumerGroupName;
        this.offsetConfig = offsetConfig;

        if (initialState != null) {
            this.assignedSplits = new HashSet<>(initialState.getAssignedSplits());
            this.discoveredPartitions = new HashSet<>(initialState.getDiscoveredPartitions());
        } else {
            this.assignedSplits = new HashSet<>();
            this.discoveredPartitions = new HashSet<>();
        }
        this.sentToReaders = new HashSet<>();
    }

    @Override
    public void start() {
        // Create consumer group if it doesn't exist
        ensureConsumerGroupExists();

        // Discover initial partitions
        discoverPartitions();
    }

    /**
     * Ensures the consumer group exists, creating it if necessary.
     * <p>
     * Note: In Iggy SDK async API, consumer group creation is not available.
     * Consumer groups must be created beforehand using the blocking HTTP client
     * or through the setup scripts. The async API only supports joining/leaving
     * consumer groups.
     */
    private void ensureConsumerGroupExists() {
        // Consumer group creation removed - must be created externally
        // The docker-compose setup already creates consumer groups via iggy-setup
        // Users should ensure consumer groups exist before starting Flink jobs
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // Not used in batch assignment mode
    }

    @Override
    public void addSplitsBack(List<IggySourceSplit> splits, int subtaskId) {
        // Re-add splits that were returned (e.g., on reader failure)
        for (IggySourceSplit split : splits) {
            assignedSplits.remove(split);
        }
        // Reassign them
        assignSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        // New reader registered, assign splits to it
        assignSplits();
    }

    @Override
    public IggySourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new IggySourceEnumeratorState(assignedSplits, discoveredPartitions);
    }

    @Override
    public void close() throws IOException {
        // Clean up resources if needed
    }

    /**
     * Discovers partitions from Iggy topic.
     */
    private void discoverPartitions() {
        try {
            StreamId stream = parseStreamId(streamId);
            TopicId topic = parseTopicId(topicId);

            // Get topic details including partitions
            Optional<TopicDetails> topicDetailsOpt =
                    asyncClient.topics().getTopicAsync(stream, topic).join();

            if (topicDetailsOpt.isPresent()) {
                TopicDetails topicDetails = topicDetailsOpt.get();
                if (topicDetails.partitions() != null) {
                    for (Partition partition : topicDetails.partitions()) {
                        int partitionId = partition.id().intValue();

                        // Skip if already discovered
                        if (discoveredPartitions.contains(partitionId)) {
                            continue;
                        }

                        // Determine starting offset
                        long startOffset = determineStartOffset(partition);

                        // Create split
                        IggySourceSplit split = IggySourceSplit.create(streamId, topicId, partitionId, startOffset);

                        discoveredPartitions.add(partitionId);

                        // Check if this split is already assigned
                        boolean alreadyAssigned =
                                assignedSplits.stream().anyMatch(s -> s.getPartitionId() == partitionId);

                        if (!alreadyAssigned) {
                            assignedSplits.add(split);
                        }
                    }
                }
            }

            // Assign discovered splits to readers
            assignSplits();

        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to discover partitions", e);
        }
    }

    /**
     * Assigns pending splits to available readers.
     */
    private void assignSplits() {
        if (context.registeredReaders().isEmpty()) {
            return;
        }

        // Get only splits that haven't been sent to readers yet
        List<IggySourceSplit> unassignedSplits = assignedSplits.stream()
                .filter(split -> !sentToReaders.contains(split.splitId()))
                .collect(java.util.stream.Collectors.toList());

        if (unassignedSplits.isEmpty()) {
            return;
        }

        // Simple round-robin assignment
        Map<Integer, List<IggySourceSplit>> assignments = new HashMap<>();
        List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());

        for (int i = 0; i < unassignedSplits.size(); i++) {
            int readerIndex = i % readers.size();
            int readerId = readers.get(readerIndex);

            assignments.computeIfAbsent(readerId, k -> new ArrayList<>()).add(unassignedSplits.get(i));
        }

        // Assign splits to readers and mark them as sent
        for (Map.Entry<Integer, List<IggySourceSplit>> entry : assignments.entrySet()) {
            context.assignSplits(new SplitsAssignment<>(Map.of(entry.getKey(), entry.getValue())));

            // Mark these splits as sent
            for (IggySourceSplit split : entry.getValue()) {
                sentToReaders.add(split.splitId());
            }
        }

        // DON'T signal no more splits - this is a continuous stream!
        // Removed: context.signalNoMoreSplits(readerId);
    }

    /**
     * Determines the starting offset for a partition based on offset configuration.
     *
     * @param partition the partition
     * @return the starting offset
     */
    private long determineStartOffset(Partition partition) {
        return switch (offsetConfig.getResetStrategy()) {
            case EARLIEST -> 0L;
            case LATEST -> partition.currentOffset().longValue();
            case NONE -> 0L; // Default to earliest
        };
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
