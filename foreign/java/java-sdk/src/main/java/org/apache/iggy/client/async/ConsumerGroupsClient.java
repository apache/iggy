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

package org.apache.iggy.client.async;

import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async interface for consumer group operations.
 */
public interface ConsumerGroupsClient {

    /**
     * Gets a consumer group asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param groupId The consumer group identifier (numeric ID)
     * @return A CompletableFuture containing consumer group details if it exists
     */
    default CompletableFuture<Optional<ConsumerGroupDetails>> getConsumerGroup(
            Long streamId, Long topicId, Long groupId) {
        return getConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    /**
     * Gets a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture containing consumer group details if it exists
     */
    CompletableFuture<Optional<ConsumerGroupDetails>> getConsumerGroup(
            StreamId streamId, TopicId topicId, ConsumerId groupId);

    /**
     * Gets all consumer groups for a topic asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @return A CompletableFuture containing list of consumer groups
     */
    default CompletableFuture<List<ConsumerGroup>> getConsumerGroups(Long streamId, Long topicId) {
        return getConsumerGroups(StreamId.of(streamId), TopicId.of(topicId));
    }

    /**
     * Gets all consumer groups for a topic asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @return A CompletableFuture containing list of consumer groups
     */
    CompletableFuture<List<ConsumerGroup>> getConsumerGroups(StreamId streamId, TopicId topicId);

    /**
     * Creates a consumer group asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param name The name of the consumer group
     * @return A CompletableFuture containing the created consumer group details
     */
    default CompletableFuture<ConsumerGroupDetails> createConsumerGroup(Long streamId, Long topicId, String name) {
        return createConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), name);
    }

    /**
     * Creates a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param name The name of the consumer group
     * @return A CompletableFuture containing the created consumer group details
     */
    CompletableFuture<ConsumerGroupDetails> createConsumerGroup(StreamId streamId, TopicId topicId, String name);

    /**
     * Deletes a consumer group asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param groupId The consumer group identifier (numeric ID)
     * @return A CompletableFuture that completes when the operation is done
     */
    default CompletableFuture<Void> deleteConsumerGroup(Long streamId, Long topicId, Long groupId) {
        return deleteConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    /**
     * Deletes a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    /**
     * Joins a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    /**
     * Leaves a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);
}
