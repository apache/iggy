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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.TopicsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.iggy.serde.BytesSerializer.toBytes;

/**
 * Async TCP implementation of TopicsClient using Netty for non-blocking I/O.
 */
public class TopicsTcpClient implements TopicsClient {

    private final Supplier<AsyncTcpConnection> connectionSupplier;
    private final Runnable topologyChanged;

    public TopicsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, () -> {});
    }

    /**
     * @param topologyChanged runs after this client deletes a topic, when the
     *     partition contexts cached for it no longer hold
     */
    TopicsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier, Runnable topologyChanged) {
        this.connectionSupplier = connectionSupplier;
        this.topologyChanged = topologyChanged;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
    }

    @Override
    public CompletableFuture<Optional<TopicDetails>> getTopic(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Topic.GET.getValue(), payload), response -> {
                    try {
                        if (response.isReadable()) {
                            return Optional.of(BytesDeserializer.readTopicDetails(response));
                        }
                        return Optional.<TopicDetails>empty();
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<List<Topic>> getTopics(StreamId streamId) {
        var payload = toBytes(streamId);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Topic.GET_ALL.getValue(), payload), response -> {
                    try {
                        var topicsCount = response.readUnsignedIntLE();
                        List<Topic> topics = new ArrayList<>(Math.toIntExact(topicsCount));
                        for (long i = 0; i < topicsCount; i++) {
                            topics.add(BytesDeserializer.readTopic(response));
                        }
                        return topics;
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<TopicDetails> createTopic(
            StreamId streamId, String name, long partitionsCount, Map<String, HeaderValue> options) {
        var payload = BytesSerializer.toCreateTopic(
                streamId, name, partitionsCount, TopicOptions.withDurabilityDefaults(options));

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Topic.CREATE.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readTopicDetails(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> updateTopic(StreamId streamId, TopicId topicId, TopicUpdate update) {
        var payload = BytesSerializer.toUpdateResource(streamId, topicId, update);

        return connection().sendAndRelease(CommandCode.System.UPDATE_RESOURCE, payload);
    }

    @Override
    public CompletableFuture<Void> deleteTopic(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return AsyncTcpConnection.mapResponse(connection().send(CommandCode.Topic.DELETE, payload), response -> {
            response.release();
            topologyChanged.run();
            return null;
        });
    }
}
