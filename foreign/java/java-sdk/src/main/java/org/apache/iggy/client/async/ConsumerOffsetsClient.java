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

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async interface for consumer offset operations.
 */
public interface ConsumerOffsetsClient {

    /**
     * Stores a consumer offset asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param partitionId The partition identifier (optional)
     * @param consumerId The consumer identifier (numeric ID)
     * @param offset The offset to store
     * @return A CompletableFuture that completes when the operation is done
     */
    default CompletableFuture<Void> storeConsumerOffset(
            Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId, BigInteger offset) {
        return storeConsumerOffset(
                StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId), offset);
    }

    /**
     * Stores a consumer offset asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param partitionId The partition identifier (optional)
     * @param consumer The consumer
     * @param offset The offset to store
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> storeConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset);

    /**
     * Gets a consumer offset asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param partitionId The partition identifier (optional)
     * @param consumerId The consumer identifier (numeric ID)
     * @return A CompletableFuture containing the consumer offset info if it exists
     */
    default CompletableFuture<Optional<ConsumerOffsetInfo>> getConsumerOffset(
            Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId) {
        return getConsumerOffset(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId));
    }

    /**
     * Gets a consumer offset asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param partitionId The partition identifier (optional)
     * @param consumer The consumer
     * @return A CompletableFuture containing the consumer offset info if it exists
     */
    CompletableFuture<Optional<ConsumerOffsetInfo>> getConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer);
}
