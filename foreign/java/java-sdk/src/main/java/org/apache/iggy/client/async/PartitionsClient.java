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

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.concurrent.CompletableFuture;

/**
 * Async interface for partition operations.
 */
public interface PartitionsClient {

    /**
     * Creates partitions asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param partitionsCount The number of partitions to create
     * @return A CompletableFuture that completes when the operation is done
     */
    default CompletableFuture<Void> createPartitions(Long streamId, Long topicId, Long partitionsCount) {
        return createPartitions(StreamId.of(streamId), TopicId.of(topicId), partitionsCount);
    }

    /**
     * Creates partitions asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param partitionsCount The number of partitions to create
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> createPartitions(StreamId streamId, TopicId topicId, Long partitionsCount);

    /**
     * Deletes partitions asynchronously.
     *
     * @param streamId The stream identifier (numeric ID)
     * @param topicId The topic identifier (numeric ID)
     * @param partitionsCount The number of partitions to delete
     * @return A CompletableFuture that completes when the operation is done
     */
    default CompletableFuture<Void> deletePartitions(Long streamId, Long topicId, Long partitionsCount) {
        return deletePartitions(StreamId.of(streamId), TopicId.of(topicId), partitionsCount);
    }

    /**
     * Deletes partitions asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param partitionsCount The number of partitions to delete
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> deletePartitions(StreamId streamId, TopicId topicId, Long partitionsCount);
}
