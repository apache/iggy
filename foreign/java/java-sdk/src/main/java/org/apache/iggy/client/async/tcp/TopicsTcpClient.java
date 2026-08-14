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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.TopicsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    private static final String COMPRESSION_ALGORITHM_OPTION = "compression_algorithm";
    private static final String MESSAGE_EXPIRY_OPTION = "message_expiry";
    private static final String MAX_TOPIC_SIZE_OPTION = "max_topic_size";

    private final Supplier<AsyncTcpConnection> connectionSupplier;

    public TopicsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
    }

    @Override
    public CompletableFuture<Optional<TopicDetails>> getTopic(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return connection().send(CommandCode.Topic.GET.getValue(), payload).thenApply(response -> {
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

        return connection().send(CommandCode.Topic.GET_ALL.getValue(), payload).thenApply(response -> {
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
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name) {

        var payload =
                createTopicPayload(streamId, partitionsCount, compressionAlgorithm, messageExpiry, maxTopicSize, name);

        return connection().send(CommandCode.Topic.CREATE.getValue(), payload).thenApply(response -> {
            try {
                return BytesDeserializer.readTopicDetails(response);
            } finally {
                response.release();
            }
        });
    }

    static ByteBuf createTopicPayload(
            StreamId streamId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name) {
        // partitions_count is a fixed field of the command, never an option:
        // admission consumes it to compute partition assignments.
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeIntLE(partitionsCount.intValue());
        payload.writeBytes(BytesSerializer.toBytes(name));
        payload.writeBytes(
                BytesSerializer.toBytes(createTopicOptions(compressionAlgorithm, messageExpiry, maxTopicSize)));
        return payload;
    }

    private static Map<HeaderKey, HeaderValue> createTopicOptions(
            CompressionAlgorithm compressionAlgorithm, BigInteger messageExpiry, BigInteger maxTopicSize) {
        // Server-default sentinels (compression none, expiry 0, size 0) are
        // omitted so the admitting server resolves them from its config.
        Map<HeaderKey, HeaderValue> options = new LinkedHashMap<>();
        if (compressionAlgorithm != CompressionAlgorithm.None) {
            var compressionName =
                    switch (compressionAlgorithm) {
                        case None -> "none";
                        case Gzip -> "gzip";
                    };
            options.put(HeaderKey.fromString(COMPRESSION_ALGORITHM_OPTION), HeaderValue.fromString(compressionName));
        }
        if (messageExpiry.signum() != 0) {
            options.put(HeaderKey.fromString(MESSAGE_EXPIRY_OPTION), HeaderValue.fromUint64(messageExpiry));
        }
        if (maxTopicSize.signum() != 0) {
            options.put(HeaderKey.fromString(MAX_TOPIC_SIZE_OPTION), HeaderValue.fromUint64(maxTopicSize));
        }
        return options;
    }

    @Override
    public CompletableFuture<Void> updateTopic(
            StreamId streamId,
            TopicId topicId,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            String name) {

        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(BytesSerializer.toBytes(name));
        // Settings ride the options block. A default value means the caller did
        // not set the key, so it is omitted and the server leaves the topic's
        // current value alone.
        payload.writeBytes(
                BytesSerializer.toBytes(createTopicOptions(compressionAlgorithm, messageExpiry, maxTopicSize)));

        return connection()
                .send(CommandCode.Topic.UPDATE.getValue(), payload)
                .thenAccept(response -> response.release());
    }

    @Override
    public CompletableFuture<Void> deleteTopic(StreamId streamId, TopicId topicId) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));

        return connection()
                .send(CommandCode.Topic.DELETE.getValue(), payload)
                .thenAccept(response -> response.release());
    }
}
