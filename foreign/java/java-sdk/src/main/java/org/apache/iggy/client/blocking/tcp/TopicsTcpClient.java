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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readTopic;
import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readTopicDetails;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.nameToBytes;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytes;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytesAsU64;

class TopicsTcpClient implements TopicsClient {

    private final InternalTcpClient tcpClient;

    TopicsTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        var response = tcpClient.send(CommandCode.Topic.GET, payload);
        if (response.isReadable()) {
            return Optional.of(readTopicDetails(response));
        }
        return Optional.empty();
    }

    @Override
    public List<Topic> getTopics(StreamId streamId) {
        var payload = toBytes(streamId);
        var response = tcpClient.send(CommandCode.Topic.GET_ALL, payload);
        List<Topic> topics = new ArrayList<>();
        while (response.isReadable()) {
            topics.add(readTopic(response));
        }
        return topics;
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
        var streamIdBytes = toBytes(streamId);
        var payload = Unpooled.buffer(23 + streamIdBytes.readableBytes() + name.length());

        payload.writeBytes(streamIdBytes);
        payload.writeIntLE(partitionsCount.intValue());
        payload.writeByte(compressionAlgorithm.asCode());
        payload.writeBytes(toBytesAsU64(messageExpiry));
        payload.writeBytes(toBytesAsU64(maxTopicSize));
        payload.writeByte(replicationFactor.orElse((short) 0));
        payload.writeBytes(nameToBytes(name));

        var response = tcpClient.send(CommandCode.Topic.CREATE, payload);
        return readTopicDetails(response);
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
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeByte(compressionAlgorithm.asCode());
        payload.writeBytes(toBytesAsU64(messageExpiry));
        payload.writeBytes(toBytesAsU64(maxTopicSize));
        payload.writeByte(replicationFactor.orElse((short) 0));
        payload.writeBytes(nameToBytes(name));

        tcpClient.send(CommandCode.Topic.UPDATE, payload);
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        tcpClient.send(CommandCode.Topic.DELETE, payload);
    }
}
