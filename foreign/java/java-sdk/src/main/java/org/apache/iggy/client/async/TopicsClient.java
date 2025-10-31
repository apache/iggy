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
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async client for topic operations.
 */
public interface TopicsClient {

    /**
     * Gets topic details by stream ID and topic ID.
     *
     * @param streamId The stream identifier
     * @param topicId  The topic identifier
     * @return CompletableFuture with Optional TopicDetails
     */
    CompletableFuture<Optional<TopicDetails>> getTopicAsync(StreamId streamId, TopicId topicId);

    /**
     * Gets all topics in a stream.
     *
     * @param streamId The stream identifier
     * @return CompletableFuture with list of Topics
     */
    CompletableFuture<List<Topic>> getTopicsAsync(StreamId streamId);

    /**
     * Creates a new topic.
     *
     * @param streamId             The stream identifier
     * @param partitionsCount      Number of partitions
     * @param compressionAlgorithm Compression algorithm to use
     * @param messageExpiry        Message expiry time in microseconds
     * @param maxTopicSize         Maximum topic size in bytes
     * @param replicationFactor    Optional replication factor
     * @param name                 Topic name
     * @return CompletableFuture with created TopicDetails
     */
    CompletableFuture<TopicDetails> createTopicAsync(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    /**
     * Updates an existing topic.
     *
     * @param streamId             The stream identifier
     * @param topicId              The topic identifier
     * @param compressionAlgorithm Compression algorithm to use
     * @param messageExpiry        Message expiry time in microseconds
     * @param maxTopicSize         Maximum topic size in bytes
     * @param replicationFactor    Optional replication factor
     * @param name                 Topic name
     * @return CompletableFuture that completes when update is done
     */
    CompletableFuture<Void> updateTopicAsync(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    /**
     * Deletes a topic.
     *
     * @param streamId The stream identifier
     * @param topicId  The topic identifier
     * @return CompletableFuture that completes when deletion is done
     */
    CompletableFuture<Void> deleteTopicAsync(StreamId streamId, TopicId topicId);
}
