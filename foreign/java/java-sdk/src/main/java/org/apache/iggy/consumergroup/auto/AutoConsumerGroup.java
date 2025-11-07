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
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Automatic consumer group with built-in rebalancing capabilities.
 */
public class AutoConsumerGroup implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AutoConsumerGroup.class);
    
    private final IggyBaseClient client;
    private final StreamId streamId;
    private final TopicId topicId;
    private final ConsumerId groupId;
    private final ConsumerGroupConfig config;
    
    private final HeartbeatManager heartbeatManager;
    private final RebalanceCoordinator rebalanceCoordinator;
    private final ScheduledExecutorService rebalanceScheduler;
    
    private final AtomicReference<ConsumerGroupState> state = new AtomicReference<>(ConsumerGroupState.CREATED);
    private volatile Consumer consumer;
    
    private AutoConsumerGroup(
            IggyBaseClient client,
            StreamId streamId,
            TopicId topicId,
            ConsumerId groupId,
            ConsumerGroupConfig config) {
        this.client = client;
        this.streamId = streamId;
        this.topicId = topicId;
        this.groupId = groupId;
        this.config = config;
        
        this.heartbeatManager = new HeartbeatManager(client.system(), config.getHeartbeatInterval());
        this.rebalanceCoordinator = new RebalanceCoordinator(
                client.consumerGroups(),
                streamId,
                topicId,
                groupId,
                config.getOnPartitionsAssigned(),
                config.getOnPartitionsRevoked()
        );
        
        this.rebalanceScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Iggy-Rebalance-Thread");
            t.setDaemon(true);
            return t;
        });
        
        this.consumer = Consumer.group(groupId);
    }
    
    /**
     * Creates a new builder for AutoConsumerGroup.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Initializes the consumer group, joining the group and starting heartbeat.
     *
     * @throws Exception if initialization fails
     */
    public void init() throws Exception {
        if (!state.compareAndSet(ConsumerGroupState.CREATED, ConsumerGroupState.INITIALIZING)) {
            throw new IllegalStateException("Consumer group is already initialized or in invalid state: " + state.get());
        }
        
        logger.info("Initializing consumer group: stream={}, topic={}, group={}", streamId, topicId, groupId);
        
        try {
            // Verify stream and topic exist
            validateStreamAndTopic();
            
            // Create or get consumer group
            getOrCreateConsumerGroup();
            
            // Join consumer group
            joinConsumerGroup();
            
            // Start heartbeat manager
            heartbeatManager.start();
            
            // Start rebalance checking
            startRebalanceChecking();
            
            state.set(ConsumerGroupState.ACTIVE);
            logger.info("Consumer group initialized successfully");
        } catch (Exception e) {
            state.set(ConsumerGroupState.CREATED);
            throw e;
        }
    }
    
    private void validateStreamAndTopic() throws Exception {
        // In a full implementation, we would verify the stream and topic exist
        logger.debug("Validating stream and topic existence");
    }
    
    private void getOrCreateConsumerGroup() {
        ConsumerGroupsClient consumerGroupsClient = client.consumerGroups();
        
        Optional<ConsumerGroupDetails> existingGroup = consumerGroupsClient.getConsumerGroup(
                streamId, topicId, groupId);
        
        if (existingGroup.isPresent()) {
            logger.debug("Consumer group already exists");
            return;
        }
        
        if (config.isCreateIfNotExists()) {
            logger.info("Creating consumer group");
            consumerGroupsClient.createConsumerGroup(
                    streamId, topicId, Optional.of(groupId.getId()), "auto-group-" + groupId.getId());
        } else {
            throw new IllegalStateException("Consumer group does not exist and auto-creation is disabled");
        }
    }
    
    private void joinConsumerGroup() {
        logger.info("Joining consumer group");
        client.consumerGroups().joinConsumerGroup(streamId, topicId, groupId);
    }
    
    private void startRebalanceChecking() {
        rebalanceScheduler.scheduleAtFixedRate(
                this::checkRebalance,
                config.getHeartbeatInterval().toMillis(),
                config.getHeartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS
        );
    }
    
    private void checkRebalance() {
        if (state.get() == ConsumerGroupState.ACTIVE || state.get() == ConsumerGroupState.POLLING) {
            rebalanceCoordinator.checkAndHandleRebalance();
        }
    }
    
    /**
     * Polls for messages from the assigned partitions.
     *
     * @param timeout the timeout for polling
     * @return polled messages
     */
    public PolledMessages poll(Duration timeout) {
        if (state.get() != ConsumerGroupState.ACTIVE && state.get() != ConsumerGroupState.POLLING) {
            throw new IllegalStateException("Consumer group is not active, current state: " + state.get());
        }
        
        state.set(ConsumerGroupState.POLLING);
        
        try {
            // Check for rebalance before polling
            checkRebalance();
            
            MessagesClient messagesClient = client.messages();
            return messagesClient.pollMessages(
                    streamId,
                    topicId,
                    Optional.empty(), // Let server assign partition
                    consumer,
                    PollingStrategy.next(),
                    100L, // Default count
                    config.isAutoCommitEnabled()
            );
        } finally {
            state.set(ConsumerGroupState.ACTIVE);
        }
    }
    
    /**
     * Gets the current state of the consumer group.
     *
     * @return the current state
     */
    public ConsumerGroupState getState() {
        return state.get();
    }
    
    @Override
    public void close() {
        logger.info("Closing consumer group");
        
        // Leave consumer group
        try {
            if (state.get() == ConsumerGroupState.ACTIVE || state.get() == ConsumerGroupState.POLLING) {
                state.set(ConsumerGroupState.LEAVING);
                client.consumerGroups().leaveConsumerGroup(streamId, topicId, groupId);
                logger.info("Left consumer group");
            }
        } catch (Exception e) {
            logger.warn("Error leaving consumer group", e);
        }
        
        // Stop heartbeat
        try {
            heartbeatManager.close();
        } catch (Exception e) {
            logger.warn("Error stopping heartbeat manager", e);
        }
        
        // Stop rebalance scheduler
        rebalanceScheduler.shutdown();
        try {
            if (!rebalanceScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                rebalanceScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            rebalanceScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        state.set(ConsumerGroupState.CREATED);
        logger.info("Consumer group closed");
    }
    
    /**
     * Builder for AutoConsumerGroup.
     */
    public static class Builder {
        private IggyBaseClient client;
        private StreamId streamId;
        private TopicId topicId;
        private ConsumerId groupId;
        private ConsumerGroupConfig config;
        
        private Builder() {}
        
        public Builder client(IggyBaseClient client) {
            this.client = client;
            return this;
        }
        
        public Builder stream(String streamId) {
            this.streamId = StreamId.of(streamId);
            return this;
        }
        
        public Builder stream(Long streamId) {
            this.streamId = StreamId.of(streamId);
            return this;
        }
        
        public Builder stream(StreamId streamId) {
            this.streamId = streamId;
            return this;
        }
        
        public Builder topic(String topicId) {
            this.topicId = TopicId.of(topicId);
            return this;
        }
        
        public Builder topic(Long topicId) {
            this.topicId = TopicId.of(topicId);
            return this;
        }
        
        public Builder topic(TopicId topicId) {
            this.topicId = topicId;
            return this;
        }
        
        public Builder groupName(String groupId) {
            this.groupId = ConsumerId.of(groupId);
            return this;
        }
        
        public Builder groupName(Long groupId) {
            this.groupId = ConsumerId.of(groupId);
            return this;
        }
        
        public Builder groupName(ConsumerId groupId) {
            this.groupId = groupId;
            return this;
        }
        
        public Builder config(ConsumerGroupConfig config) {
            this.config = config;
            return this;
        }
        
        public AutoConsumerGroup build() {
            if (client == null) {
                throw new IllegalStateException("Client is required");
            }
            if (streamId == null) {
                throw new IllegalStateException("Stream ID is required");
            }
            if (topicId == null) {
                throw new IllegalStateException("Topic ID is required");
            }
            if (groupId == null) {
                throw new IllegalStateException("Group ID is required");
            }
            if (config == null) {
                config = ConsumerGroupConfig.builder().build();
            }
            
            return new AutoConsumerGroup(client, streamId, topicId, groupId, config);
        }
    }
}