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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

final class TopicsTcpClient implements TopicsClient {

    private final org.apache.iggy.client.async.TopicsClient delegate;

    TopicsTcpClient(org.apache.iggy.client.async.TopicsClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
        return FutureUtil.resolve(delegate.getTopic(streamId, topicId));
    }

    @Override
    public List<Topic> getTopics(StreamId streamId) {
        return FutureUtil.resolve(delegate.getTopics(streamId));
    }

    @Override
    public TopicDetails createTopic(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {
        return FutureUtil.resolve(delegate.createTopic(
                streamId, partitionsCount, compressionAlgorithm, messageExpiry, maxTopicSize, replicationFactor, name));
    }

    @Override
    public void updateTopic(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {
        FutureUtil.resolve(delegate.updateTopic(
                streamId, topicId, compressionAlgorithm, messageExpiry, maxTopicSize, replicationFactor, name));
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        FutureUtil.resolve(delegate.deleteTopic(streamId, topicId));
    }
}
