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

package org.apache.iggy.benchmark.util;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.client.blocking.ConsumerOffsetsClient;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.PartitionsClient;
import org.apache.iggy.client.blocking.PersonalAccessTokensClient;
import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.UsersClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class BlockingAsyncClientAdapter implements IggyBaseClient {

    private final AsyncIggyTcpClient asyncClient;

    public BlockingAsyncClientAdapter(AsyncIggyTcpClient asyncClient) {
        this.asyncClient = asyncClient;
    }

    @Override
    public StreamsClient streams() {
        return new StreamsClient() {
            @Override
            public StreamDetails createStream(String name) {
                try {
                    return asyncClient.streams().createStreamAsync(name).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to create stream", e);
                }
            }

            @Override
            public void deleteStream(StreamId streamId) {
                try {
                    asyncClient.streams().deleteStreamAsync(streamId).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to delete stream", e);
                }
            }

            @Override
            public Optional<StreamDetails> getStream(StreamId streamId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<org.apache.iggy.stream.StreamBase> getStreams() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void updateStream(StreamId streamId, String name) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public TopicsClient topics() {
        return new TopicsClient() {
            @Override
            public TopicDetails createTopic(
                    StreamId streamId,
                    Long partitionsCount,
                    CompressionAlgorithm compressionAlgorithm,
                    BigInteger messageExpiry,
                    BigInteger maxTopicSize,
                    Optional<Short> replicationFactor,
                    String name) {
                try {
                    return asyncClient
                            .topics()
                            .createTopicAsync(
                                    streamId,
                                    partitionsCount,
                                    compressionAlgorithm,
                                    messageExpiry,
                                    maxTopicSize,
                                    replicationFactor,
                                    name)
                            .get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to create topic", e);
                }
            }

            @Override
            public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<org.apache.iggy.topic.Topic> getTopics(StreamId streamId) {
                throw new UnsupportedOperationException();
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
                throw new UnsupportedOperationException();
            }

            @Override
            public void deleteTopic(StreamId streamId, TopicId topicId) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public MessagesClient messages() {
        return new MessagesClient() {
            @Override
            public void sendMessages(
                    StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
                try {
                    asyncClient
                            .messages()
                            .sendMessagesAsync(streamId, topicId, partitioning, messages)
                            .get();
                } catch (InterruptedException | java.util.concurrent.ExecutionException e) {
                    throw new RuntimeException("Failed to send messages", e);
                }
            }

            @Override
            public PolledMessages pollMessages(
                    StreamId streamId,
                    TopicId topicId,
                    Optional<Long> partitionId,
                    Consumer consumer,
                    PollingStrategy strategy,
                    Long count,
                    boolean autoCommit) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public SystemClient system() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UsersClient users() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionsClient partitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        throw new UnsupportedOperationException();
    }
}
