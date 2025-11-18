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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readConsumerGroup;
import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readConsumerGroupDetails;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.nameToBytes;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytes;

class ConsumerGroupsTcpClient implements ConsumerGroupsClient {

    private final InternalTcpClient tcpClient;

    public ConsumerGroupsTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));
        var response = tcpClient.send(CommandCode.ConsumerGroup.GET, payload);
        if (response.isReadable()) {
            return Optional.of(readConsumerGroupDetails(response));
        }
        return Optional.empty();
    }

    @Override
    public List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        var response = tcpClient.send(CommandCode.ConsumerGroup.GET_ALL, payload);
        List<ConsumerGroup> groups = new ArrayList<>();
        while (response.isReadable()) {
            groups.add(readConsumerGroup(response));
        }
        return groups;
    }

    @Override
    public ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, String name) {
        var streamIdBytes = toBytes(streamId);
        var topicIdBytes = toBytes(topicId);
        var payload = Unpooled.buffer(1 + streamIdBytes.readableBytes() + topicIdBytes.readableBytes() + name.length());

        payload.writeBytes(streamIdBytes);
        payload.writeBytes(topicIdBytes);
        payload.writeBytes(nameToBytes(name));

        ByteBuf response = tcpClient.send(CommandCode.ConsumerGroup.CREATE, payload);
        return readConsumerGroupDetails(response);
    }

    @Override
    public void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));
        tcpClient.send(CommandCode.ConsumerGroup.DELETE, payload);
    }

    @Override
    public void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));

        tcpClient.send(CommandCode.ConsumerGroup.JOIN, payload);
    }

    @Override
    public void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));

        tcpClient.send(CommandCode.ConsumerGroup.LEAVE, payload);
    }
}
