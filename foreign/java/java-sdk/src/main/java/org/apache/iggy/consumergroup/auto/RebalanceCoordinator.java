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

package org.apache.iggy.consumergroup.auto;

import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Coordinates rebalancing operations for automatic consumer group management.
 */
public class RebalanceCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceCoordinator.class);
    
    private final ConsumerGroupsClient consumerGroupsClient;
    private final StreamId streamId;
    private final TopicId topicId;
    private final ConsumerId groupId;
    private final Consumer<List<Long>> onPartitionsAssigned;
    private final Consumer<List<Long>> onPartitionsRevoked;
    
    private final AtomicReference<Set<Long>> currentPartitions = new AtomicReference<>(new HashSet<>());
    private volatile long lastMembersCount = 0;
    
    public RebalanceCoordinator(
            ConsumerGroupsClient consumerGroupsClient,
            StreamId streamId,
            TopicId topicId,
            ConsumerId groupId,
            Consumer<List<Long>> onPartitionsAssigned,
            Consumer<List<Long>> onPartitionsRevoked) {
        this.consumerGroupsClient = consumerGroupsClient;
        this.streamId = streamId;
        this.topicId = topicId;
        this.groupId = groupId;
        this.onPartitionsAssigned = onPartitionsAssigned;
        this.onPartitionsRevoked = onPartitionsRevoked;
    }
    
    /**
     * Checks for rebalance and handles partition assignment changes.
     */
    public void checkAndHandleRebalance() {
        try {
            // Get the latest consumer group details
            ConsumerGroupDetails details = consumerGroupsClient.getConsumerGroup(streamId, topicId, groupId)
                    .orElse(null);
            
            if (details == null) {
                logger.warn("Consumer group not found: stream={}, topic={}, group={}", streamId, topicId, groupId);
                return;
            }
            
            // Check if rebalance is needed
            boolean needsRebalance = lastMembersCount != details.membersCount();
            lastMembersCount = details.membersCount();
            
            if (!needsRebalance) {
                return;
            }
            
            logger.info("Rebalance detected for consumer group: members={}", details.membersCount());
            
            // Find partitions assigned to this consumer
            Set<Long> newPartitions = new HashSet<>();
            for (var member : details.members()) {
                // In a real implementation, we would need to identify which member is this consumer
                // For now, we'll just collect all partitions as an example
                newPartitions.addAll(member.partitions());
            }
            
            // Compare with current partitions
            Set<Long> oldPartitions = currentPartitions.get();
            
            // Find partitions that were revoked
            Set<Long> revokedPartitions = new HashSet<>(oldPartitions);
            revokedPartitions.removeAll(newPartitions);
            
            // Find partitions that were assigned
            Set<Long> assignedPartitions = new HashSet<>(newPartitions);
            assignedPartitions.removeAll(oldPartitions);
            
            // Update current partitions
            currentPartitions.set(newPartitions);
            
            // Trigger callbacks
            if (!revokedPartitions.isEmpty()) {
                logger.info("Partitions revoked: {}", revokedPartitions);
                try {
                    onPartitionsRevoked.accept(List.copyOf(revokedPartitions));
                } catch (Exception e) {
                    logger.error("Error in onPartitionsRevoked callback", e);
                }
            }
            
            if (!assignedPartitions.isEmpty()) {
                logger.info("Partitions assigned: {}", assignedPartitions);
                try {
                    onPartitionsAssigned.accept(List.copyOf(assignedPartitions));
                } catch (Exception e) {
                    logger.error("Error in onPartitionsAssigned callback", e);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error checking for rebalance", e);
        }
    }
    
    /**
     * Gets the currently assigned partitions.
     * 
     * @return Set of partition IDs
     */
    public Set<Long> getCurrentPartitions() {
        return Collections.unmodifiableSet(currentPartitions.get());
    }
}