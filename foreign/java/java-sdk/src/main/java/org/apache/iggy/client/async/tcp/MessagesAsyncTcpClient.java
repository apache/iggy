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
import org.apache.iggy.client.async.MessagesAsyncClient;
import org.apache.iggy.client.blocking.tcp.CommandCode;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.iggy.client.async.tcp.BytesSerializer.toBytes;

/**
 * Asynchronous TCP client implementation for message operations.
 */
class MessagesAsyncTcpClient implements MessagesAsyncClient {

    private final InternalAsyncTcpClient tcpClient;

    /**
     * Creates a new MessagesAsyncTcpClient.
     *
     * @param tcpClient the internal async TCP client
     */
    public MessagesAsyncTcpClient(InternalAsyncTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public CompletableFuture<PolledMessages> pollMessages(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, PollingStrategy strategy, Long count, boolean autoCommit) {
        return CompletableFuture.supplyAsync(() -> {
            var payload = Unpooled.buffer();
            payload.writeBytes(toBytes(consumer));
            payload.writeBytes(toBytes(streamId));
            payload.writeBytes(toBytes(topicId));
            payload.writeIntLE(partitionId.orElse(0L).intValue());
            payload.writeBytes(toBytes(strategy));
            payload.writeIntLE(count.intValue());
            payload.writeByte(autoCommit ? 1 : 0);
            return payload;
        }).thenCompose(payload -> tcpClient.send(CommandCode.Messages.POLL, payload))
          .thenApply(BytesDeserializer::readPolledMessages);
    }

    @Override
    public CompletableFuture<Void> sendMessages(StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        return CompletableFuture.supplyAsync(() -> {
            // Length of streamId, topicId, partitioning and messages count (4 bytes)
            var metadataLength = streamId.getSize() + topicId.getSize() + partitioning.getSize() + 4;
            var payload = Unpooled.buffer(4 + metadataLength);
            payload.writeIntLE(metadataLength);
            payload.writeBytes(toBytes(streamId));
            payload.writeBytes(toBytes(topicId));
            payload.writeBytes(toBytes(partitioning));
            payload.writeIntLE(messages.size());

            // Writing index
            var position = 0;
            for (var message : messages) {
                // The logic in messages_batch_mut.rs#message_start_position checks the
                // previous index to get the starting position of the message.
                // For the first message it's always 0.
                // This is the reason why we are setting the position to start of the next
                // message.

                // This used as both start index of next message and
                // the end position for the current message.
                position += message.getSize();

                // offset
                payload.writeIntLE(0);
                // position
                payload.writeIntLE(position);
                // timestamp.
                payload.writeZero(8);
            }

            for (var message : messages) {
                payload.writeBytes(toBytes(message));
            }

            return payload;
        }).thenCompose(payload -> tcpClient.send(CommandCode.Messages.SEND, payload))
          .thenApply(result -> null);
    }
}
