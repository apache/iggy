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
import org.apache.iggy.client.blocking.tcp.CommandCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.nameToBytes;
import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.toBytes;
import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.toBytesAsU64;

/**
 * Async TCP implementation of TopicsClient using Netty for non-blocking I/O.
 */
public class TopicsTcpClient implements TopicsClient {

    private final AsyncTcpConnection connection;

    public TopicsTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Optional<TopicDetails>> getTopicAsync(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return connection.sendAsync(CommandCode.Topic.GET.getValue(), payload).thenApply(response -> {
            try {
                if (response.isReadable()) {
                    return Optional.of(AsyncBytesDeserializer.readTopicDetails(response));
                }
                return Optional.<TopicDetails>empty();
            } finally {
                response.release();
            }
        });
    }

    @Override
    public CompletableFuture<List<Topic>> getTopicsAsync(StreamId streamId) {
        var payload = toBytes(streamId);

        return connection
                .sendAsync(CommandCode.Topic.GET_ALL.getValue(), payload)
                .thenApply(response -> {
                    try {
                        List<Topic> topics = new ArrayList<>();
                        while (response.isReadable()) {
                            topics.add(AsyncBytesDeserializer.readTopic(response));
                        }
                        return topics;
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<TopicDetails> createTopicAsync(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {

        var streamIdBytes = toBytes(streamId);
        var payload = Unpooled.buffer(23 + streamIdBytes.readableBytes() + name.length());

        payload.writeBytes(streamIdBytes);
        payload.writeIntLE(partitionsCount.intValue());
        payload.writeByte(compressionAlgorithm.asCode());
        payload.writeBytes(toBytesAsU64(messageExpiry));
        payload.writeBytes(toBytesAsU64(maxTopicSize));
        payload.writeByte(replicationFactor.orElse((short) 0));
        payload.writeBytes(nameToBytes(name));

        return connection
                .sendAsync(CommandCode.Topic.CREATE.getValue(), payload)
                .thenApply(response -> {
                    try {
                        return AsyncBytesDeserializer.readTopicDetails(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> updateTopicAsync(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name) {

        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeByte(compressionAlgorithm.asCode());
        payload.writeBytes(toBytesAsU64(messageExpiry));
        payload.writeBytes(toBytesAsU64(maxTopicSize));
        payload.writeByte(replicationFactor.orElse((short) 0));
        payload.writeBytes(nameToBytes(name));

        return connection
                .sendAsync(CommandCode.Topic.UPDATE.getValue(), payload)
                .thenAccept(response -> response.release());
    }

    @Override
    public CompletableFuture<Void> deleteTopicAsync(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return connection
                .sendAsync(CommandCode.Topic.DELETE.getValue(), payload)
                .thenAccept(response -> response.release());
    }
}
