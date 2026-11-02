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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import tools.jackson.core.type.TypeReference;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class TopicsHttpClient implements TopicsClient {

    private static final String STREAMS = "/streams";
    private static final String TOPICS = "/topics";
    private final InternalHttpClient httpClient;
    private final SendContexts sendContexts;

    public TopicsHttpClient(InternalHttpClient httpClient, SendContexts sendContexts) {
        this.httpClient = httpClient;
        this.sendContexts = sendContexts;
    }

    @Override
    public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        return httpClient
                .executeWithOptionalResponse(request, HttpTopicDetails.class)
                .map(HttpTopicDetails::toTopicDetails);
    }

    @Override
    public List<Topic> getTopics(StreamId streamId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS);
        List<HttpTopic> topics = httpClient.execute(request, new TypeReference<>() {});
        return topics.stream().map(HttpTopic::toTopic).toList();
    }

    @Override
    public TopicDetails createTopic(
            StreamId streamId, String name, long partitionsCount, Map<String, HeaderValue> options) {
        var request = httpClient.preparePostRequest(
                STREAMS + "/" + streamId + TOPICS,
                new CreateTopic(
                        partitionsCount,
                        name,
                        HttpOptionValue.requestValues(TopicOptions.withDurabilityDefaults(options))));
        return httpClient.execute(request, HttpTopicDetails.class).toTopicDetails();
    }

    @Override
    public void updateTopic(StreamId streamId, TopicId topicId, TopicUpdate update) {
        var request = httpClient.preparePutRequest(
                STREAMS + "/" + streamId + TOPICS + "/" + topicId,
                HttpOptionValue.updateBody(update.entries(), update.resetKeys()));
        httpClient.execute(request);
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        httpClient.execute(request);
        sendContexts.clear();
    }

    record CreateTopic(long partitionsCount, String name, Map<String, Object> options) {}

    /**
     * The REST shape of a topic.
     *
     * <p>It differs from {@link Topic} in one place: REST reports every option in a single map with a
     * per-entry {@code explicit} flag, where the binary protocol sends an explicit block and a derived
     * block. Splitting here is what lets a caller read {@code options()} and {@code derivedOptions()}
     * the same way on either transport, each value in the kind the server stored.
     */
    record HttpTopic(
            Long id,
            BigInteger createdAt,
            String name,
            String size,
            BigInteger messagesCount,
            Long partitionsCount,
            Map<String, HttpOptionValue> options) {

        Topic toTopic() {
            return new Topic(
                    id,
                    createdAt,
                    name,
                    size,
                    messagesCount,
                    partitionsCount,
                    HttpOptionValue.select(options, true),
                    HttpOptionValue.select(options, false));
        }
    }

    /** The REST shape of a topic with its partitions. See {@link HttpTopic}. */
    record HttpTopicDetails(
            Long id,
            BigInteger createdAt,
            String name,
            String size,
            BigInteger messagesCount,
            Long partitionsCount,
            List<Partition> partitions,
            Map<String, HttpOptionValue> options) {

        TopicDetails toTopicDetails() {
            return new TopicDetails(
                    id,
                    createdAt,
                    name,
                    size,
                    messagesCount,
                    partitionsCount,
                    partitions == null ? List.of() : partitions,
                    HttpOptionValue.select(options, true),
                    HttpOptionValue.select(options, false));
        }
    }
}
