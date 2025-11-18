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

package org.apache.iggy.client.blocking;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

public interface TopicsClient {

    default Optional<TopicDetails> getTopic(Long streamId, Long topicId) {
        return getTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId);

    default List<Topic> getTopics(Long streamId) {
        return getTopics(StreamId.of(streamId));
    }

    List<Topic> getTopics(StreamId streamId);

    default TopicDetails createTopic(
            Long streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {
        return createTopic(
                StreamId.of(streamId),
                partitionsCount,
                compressionAlgorithm,
                messageExpiry,
                maxTopicSize,
                replicationFactor,
                name);
    }

    TopicDetails createTopic(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    default void updateTopic(
            Long streamId,
            Long topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {
        updateTopic(
                StreamId.of(streamId),
                TopicId.of(topicId),
                compressionAlgorithm,
                messageExpiry,
                maxTopicSize,
                replicationFactor,
                name);
    }

    void updateTopic(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name);

    default void deleteTopic(Long streamId, Long topicId) {
        deleteTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    void deleteTopic(StreamId streamId, TopicId topicId);
}
