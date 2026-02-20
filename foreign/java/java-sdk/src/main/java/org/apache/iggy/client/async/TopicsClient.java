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
 * Async client interface for topic management operations.
 *
 * <p>Topics exist within streams and contain one or more partitions that hold the actual
 * messages. Each topic has configurable properties including compression, message expiry,
 * maximum size, and replication factor.
 *
 * <p>Usage example:
 * <pre>{@code
 * TopicsClient topics = client.topics();
 *
 * // Create a topic with 3 partitions and no message expiry
 * topics.createTopic(
 *         StreamId.of(1L), 3L, CompressionAlgorithm.none(),
 *         BigInteger.ZERO, BigInteger.ZERO, Optional.empty(), "events")
 *     .thenAccept(details -> System.out.println("Topic created: " + details.name()));
 *
 * // List all topics in a stream
 * topics.getTopics(StreamId.of(1L))
 *     .thenAccept(list -> list.forEach(t -> System.out.println(t.name())));
 * }</pre>
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#topics()
 */
public interface TopicsClient {

    /**
     * Gets detailed information about a specific topic.
     *
     * <p>The returned {@link TopicDetails} includes the topic's metadata, configuration
     * (compression, expiry, max size), and partition information.
     *
     * @param streamId the stream identifier containing the topic
     * @param topicId  the topic identifier
     * @return a {@link CompletableFuture} that completes with an {@link Optional} containing
     *         the {@link TopicDetails} if found, or empty if the topic does not exist
     */
    CompletableFuture<Optional<TopicDetails>> getTopic(StreamId streamId, TopicId topicId);

    /**
     * Gets a list of all topics within a stream.
     *
     * @param streamId the stream identifier
     * @return a {@link CompletableFuture} that completes with a list of {@link Topic} objects
     */
    CompletableFuture<List<Topic>> getTopics(StreamId streamId);

    /**
     * Creates a new topic within a stream.
     *
     * <p>The topic is created with the specified number of partitions and configuration.
     * Partition count cannot be changed after creation, but additional partitions can be
     * added via the partitions API.
     *
     * @param streamId             the stream identifier to create the topic in
     * @param partitionsCount      the initial number of partitions (must be at least 1)
     * @param compressionAlgorithm the compression algorithm for stored messages
     *                             (e.g., {@link CompressionAlgorithm#None})
     * @param messageExpiry        message expiry time in microseconds; {@link BigInteger#ZERO}
     *                             means messages never expire
     * @param maxTopicSize         maximum topic size in bytes; {@link BigInteger#ZERO}
     *                             means unlimited
     * @param replicationFactor    optional replication factor for the topic; if empty,
     *                             the server default is used
     * @param name                 the topic name (must be unique within the stream)
     * @return a {@link CompletableFuture} that completes with the created {@link TopicDetails}
     * @throws org.apache.iggy.exception.IggyException if the stream does not exist or a
     *         topic with the same name already exists
     */
    CompletableFuture<TopicDetails> createTopic(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    /**
     * Updates the configuration of an existing topic.
     *
     * <p>This allows changing the topic's name, compression, expiry, size limit, and
     * replication factor. Partition count is not affected by this operation.
     *
     * @param streamId             the stream identifier containing the topic
     * @param topicId              the topic identifier to update
     * @param compressionAlgorithm the new compression algorithm
     * @param messageExpiry        the new message expiry in microseconds
     * @param maxTopicSize         the new maximum topic size in bytes
     * @param replicationFactor    optional new replication factor
     * @param name                 the new topic name
     * @return a {@link CompletableFuture} that completes when the update is done
     * @throws org.apache.iggy.exception.IggyException if the topic does not exist
     */
    CompletableFuture<Void> updateTopic(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    /**
     * Deletes a topic and all of its partitions and messages.
     *
     * <p><strong>Warning:</strong> This operation is irreversible and will permanently
     * delete all messages within the topic.
     *
     * @param streamId the stream identifier containing the topic
     * @param topicId  the topic identifier to delete
     * @return a {@link CompletableFuture} that completes when the deletion is done
     * @throws org.apache.iggy.exception.IggyException if the topic does not exist
     */
    CompletableFuture<Void> deleteTopic(StreamId streamId, TopicId topicId);
}
