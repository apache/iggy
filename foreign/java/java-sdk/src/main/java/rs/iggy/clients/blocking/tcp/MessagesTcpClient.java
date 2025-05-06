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

package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.MessagesClient;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.message.Message;
import rs.iggy.message.Partitioning;
import rs.iggy.message.PolledMessages;
import rs.iggy.message.PollingStrategy;
import java.util.List;
import java.util.Optional;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytes;

class MessagesTcpClient implements MessagesClient {

    private static final int POLL_MESSAGES_CODE = 100;
    private static final int SEND_MESSAGES_CODE = 101;

    private final InternalTcpClient tcpClient;

    public MessagesTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public PolledMessages pollMessages(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, PollingStrategy strategy, Long count, boolean autoCommit) {
        var payload = toBytes(consumer);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionId.orElse(0L).intValue());
        payload.writeBytes(toBytes(strategy));
        payload.writeIntLE(count.intValue());
        payload.writeByte(autoCommit ? 1 : 0);

        var response = tcpClient.send(POLL_MESSAGES_CODE, payload);

        return BytesDeserializer.readPolledMessages(response);
    }

    @Override
    public void sendMessages(StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(partitioning));
        for (var message : messages) {
            payload.writeBytes(toBytes(message));
        }

        tcpClient.send(SEND_MESSAGES_CODE, payload);
    }
}
