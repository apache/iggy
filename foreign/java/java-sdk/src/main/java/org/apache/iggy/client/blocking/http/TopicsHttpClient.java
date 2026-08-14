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
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import tools.jackson.core.type.TypeReference;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name,
            Map<String, HeaderValue> options) {
        var request = httpClient.preparePostRequest(
                STREAMS + "/" + streamId + TOPICS,
                new CreateTopic(
                        partitionsCount,
                        compressionAlgorithm,
                        messageExpiry,
                        maxTopicSize,
                        name,
                        toStringOptions(options)));
        return httpClient.execute(request, HttpTopicDetails.class).toTopicDetails();
    }

    @Override
    public void updateTopic(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name,
            Map<String, HeaderValue> options) {
        var request = httpClient.preparePutRequest(
                STREAMS + "/" + streamId + TOPICS + "/" + topicId,
                new UpdateTopic(compressionAlgorithm, messageExpiry, maxTopicSize, name, toStringOptions(options)));
        httpClient.execute(request);
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        httpClient.execute(request);
    }

    /**
     * Renders option values as the strings the REST body carries them in.
     *
     * <p>The binary transports send a typed TLV block, but the JSON body takes a plain string map
     * that the server parses by the same rules a config file value goes through, so a typed value
     * handed in here is rendered: sending a {@code Uint64} 134217728 and sending "134217728" land
     * the same stored value.
     */
    private static Map<String, String> toStringOptions(Map<String, HeaderValue> options) {
        Map<String, String> rendered = new LinkedHashMap<>();
        options.forEach((key, value) -> rendered.put(key, value.toStringValue()));
        return rendered;
    }

    record CreateTopic(
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name,
            Map<String, String> options) {}

    record UpdateTopic(
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name,
            Map<String, String> options) {}

    /**
     * One option entry as REST renders it: the value in the string form the create body takes, plus
     * the provenance flag that the binary protocol carries as two separate blocks instead.
     */
    record HttpOptionValue(String value, boolean explicit) {}

    /**
     * The REST shape of a topic.
     *
     * <p>It differs from {@link Topic} in one place: REST reports every option in a single map with a
     * per-entry {@code explicit} flag, where the binary protocol sends an explicit block and a derived
     * block. Splitting here is what lets a caller read {@code options()} and {@code derivedOptions()}
     * the same way on either transport. Values arrive in string form, so each is String-kinded rather
     * than carrying the kind the server stored.
     */
    record HttpTopic(
            Long id,
            BigInteger createdAt,
            String name,
            String size,
            BigInteger messageExpiry,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger maxTopicSize,
            BigInteger messagesCount,
            Long partitionsCount,
            Map<String, HttpOptionValue> options) {

        Topic toTopic() {
            return new Topic(
                    id,
                    createdAt,
                    name,
                    size,
                    messageExpiry,
                    compressionAlgorithm,
                    maxTopicSize,
                    messagesCount,
                    partitionsCount,
                    optionsOf(options, true),
                    optionsOf(options, false));
        }
    }

    /** The REST shape of a topic with its partitions. See {@link HttpTopic}. */
    record HttpTopicDetails(
            Long id,
            BigInteger createdAt,
            String name,
            String size,
            BigInteger messageExpiry,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger maxTopicSize,
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
                    messageExpiry,
                    compressionAlgorithm,
                    maxTopicSize,
                    messagesCount,
                    partitionsCount,
                    partitions == null ? List.of() : partitions,
                    optionsOf(options, true),
                    optionsOf(options, false));
        }
    }

    private static Map<String, HeaderValue> optionsOf(Map<String, HttpOptionValue> options, boolean explicit) {
        if (options == null) {
            return Map.of();
        }
        Map<String, HeaderValue> selected = new LinkedHashMap<>();
        options.forEach((key, option) -> {
            if (option.explicit() == explicit) {
                selected.put(key, HeaderValue.fromString(option.value()));
            }
        });
        return Map.copyOf(selected);
    }
}
