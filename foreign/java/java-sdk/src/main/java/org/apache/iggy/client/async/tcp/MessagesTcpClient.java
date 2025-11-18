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
import org.apache.iggy.client.async.MessagesClient;
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

import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.toBytes;

/**
 * Async TCP implementation of MessagesClient using Netty for non-blocking I/O.
 */
public class MessagesTcpClient implements MessagesClient {

    private final AsyncTcpConnection connection;

    public MessagesTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<PolledMessages> pollMessagesAsync(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {

        // Build the request payload
        var payload = Unpooled.buffer();

        var consumerBytes = toBytes(consumer);
        payload.writeBytes(consumerBytes);

        var streamBytes = toBytes(streamId);
        payload.writeBytes(streamBytes);

        var topicBytes = toBytes(topicId);
        payload.writeBytes(topicBytes);

        payload.writeBytes(toBytes(partitionId));

        var strategyBytes = toBytes(strategy);
        payload.writeBytes(strategyBytes);

        payload.writeIntLE(count.intValue());

        payload.writeByte(autoCommit ? 1 : 0);

        // Send async request and transform response
        return connection
                .sendAsync(CommandCode.Messages.POLL.getValue(), payload)
                .thenApply(response -> {
                    try {
                        return AsyncBytesDeserializer.readPolledMessages(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> sendMessagesAsync(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {

        // Build metadata section following the blocking client pattern
        var metadataLength = streamId.getSize() + topicId.getSize() + partitioning.getSize() + 4;
        var payload = Unpooled.buffer(4 + metadataLength);

        // Write metadata length and components
        payload.writeIntLE(metadataLength);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(partitioning));
        payload.writeIntLE(messages.size());

        // Write message index metadata (required by server)
        var position = 0;
        for (var message : messages) {
            // Calculate position for next message
            position += message.getSize();

            // offset (4 bytes)
            payload.writeIntLE(0);
            // position (4 bytes)
            payload.writeIntLE(position);
            // timestamp (8 bytes)
            payload.writeZero(8);
        }

        // Write actual message data
        for (var message : messages) {
            payload.writeBytes(toBytes(message));
        }

        // Send async request (no response data expected for send)
        return connection
                .sendAsync(CommandCode.Messages.SEND.getValue(), payload)
                .thenAccept(response -> {
                    // Response received, messages sent successfully
                    response.release(); // Release the buffer
                });
    }
}
