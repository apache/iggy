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
import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of consumer groups client.
 */
public class ConsumerGroupsTcpClient implements ConsumerGroupsClient {
    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupsTcpClient.class);

    private final AsyncTcpConnection connection;

    public ConsumerGroupsTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Optional<ConsumerGroupDetails>> getConsumerGroup(
            StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = BytesSerializer.toBytes(streamId);
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeBytes(BytesSerializer.toBytes(groupId));

        log.debug("Getting consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .send(CommandCode.ConsumerGroup.GET.getValue(), payload)
                .thenApply(response -> {
                    try {
                        if (response.isReadable()) {
                            return Optional.of(BytesDeserializer.readConsumerGroupDetails(response));
                        } else {
                            return Optional.empty();
                        }
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<List<ConsumerGroup>> getConsumerGroups(StreamId streamId, TopicId topicId) {
        var payload = BytesSerializer.toBytes(streamId);
        payload.writeBytes(BytesSerializer.toBytes(topicId));

        log.debug("Getting consumer groups - Stream: {}, Topic: {}", streamId, topicId);

        return connection
                .send(CommandCode.ConsumerGroup.GET_ALL.getValue(), payload)
                .thenApply(response -> {
                    try {
                        List<ConsumerGroup> groups = new ArrayList<>();
                        while (response.isReadable()) {
                            groups.add(BytesDeserializer.readConsumerGroup(response));
                        }
                        return groups;
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<ConsumerGroupDetails> createConsumerGroup(
            StreamId streamId, TopicId topicId, String name) {
        var streamIdBytes = BytesSerializer.toBytes(streamId);
        var topicIdBytes = BytesSerializer.toBytes(topicId);
        var payload = Unpooled.buffer(1 + streamIdBytes.readableBytes() + topicIdBytes.readableBytes() + name.length());

        payload.writeBytes(streamIdBytes);
        payload.writeBytes(topicIdBytes);
        payload.writeBytes(BytesSerializer.toBytes(name));

        log.debug("Creating consumer group - Stream: {}, Topic: {}, Name: {}", streamId, topicId, name);

        return connection
                .send(CommandCode.ConsumerGroup.CREATE.getValue(), payload)
                .thenApply(response -> {
                    try {
                        return BytesDeserializer.readConsumerGroupDetails(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = BytesSerializer.toBytes(streamId);
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeBytes(BytesSerializer.toBytes(groupId));

        log.debug("Deleting consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .send(CommandCode.ConsumerGroup.DELETE.getValue(), payload)
                .thenAccept(response -> {
                    response.release();
                });
    }

    @Override
    public CompletableFuture<Void> joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = Unpooled.buffer();

        // Serialize stream ID
        payload.writeBytes(BytesSerializer.toBytes(streamId));

        // Serialize topic ID
        payload.writeBytes(BytesSerializer.toBytes(topicId));

        // Serialize consumer group ID
        payload.writeBytes(BytesSerializer.toBytes(groupId));

        log.debug("Joining consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .send(CommandCode.ConsumerGroup.JOIN.getValue(), payload)
                .thenAccept(response -> {
                    log.debug("Successfully joined consumer group");
                    response.release();
                });
    }

    @Override
    public CompletableFuture<Void> leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = Unpooled.buffer();

        // Serialize stream ID
        payload.writeBytes(BytesSerializer.toBytes(streamId));

        // Serialize topic ID
        payload.writeBytes(BytesSerializer.toBytes(topicId));

        // Serialize consumer group ID
        payload.writeBytes(BytesSerializer.toBytes(groupId));

        log.debug("Leaving consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .send(CommandCode.ConsumerGroup.LEAVE.getValue(), payload)
                .thenAccept(response -> {
                    log.debug("Successfully left consumer group");
                    response.release();
                });
    }
}
