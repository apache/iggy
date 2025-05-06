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

package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.TopicsClient;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.topic.CompressionAlgorithm;
import rs.iggy.topic.Topic;
import rs.iggy.topic.TopicDetails;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

class TopicsHttpClient implements TopicsClient {

    private static final String STREAMS = "/streams";
    private static final String TOPICS = "/topics";
    private final InternalHttpClient httpClient;

    public TopicsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        return httpClient.executeWithOptionalResponse(request, TopicDetails.class);
    }

    @Override
    public List<Topic> getTopics(StreamId streamId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public TopicDetails createTopic(StreamId streamId,
                                    Optional<Long> topicId,
                                    Long partitionsCount,
                                    CompressionAlgorithm compressionAlgorithm,
                                    BigInteger messageExpiry,
                                    BigInteger maxTopicSize,
                                    Optional<Short> replicationFactor,
                                    String name) {
        var request = httpClient.preparePostRequest(STREAMS + "/" + streamId + TOPICS,
                new CreateTopic(topicId,
                        partitionsCount,
                        compressionAlgorithm,
                        messageExpiry,
                        maxTopicSize,
                        replicationFactor,
                        name));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void updateTopic(StreamId streamId,
                            TopicId topicId,
                            CompressionAlgorithm compressionAlgorithm,
                            BigInteger messageExpiry,
                            BigInteger maxTopicSize,
                            Optional<Short> replicationFactor,
                            String name) {
        var request = httpClient.preparePutRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId,
                new UpdateTopic(compressionAlgorithm, messageExpiry, maxTopicSize, replicationFactor, name));
        httpClient.execute(request);
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        httpClient.execute(request);
    }

    record CreateTopic(
            Optional<Long> topicId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name
    ) {
    }

    record UpdateTopic(
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name
    ) {
    }
}
