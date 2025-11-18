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
import org.apache.iggy.client.blocking.tcp.CommandCode;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public CompletableFuture<Void> joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = Unpooled.buffer();

        // Serialize stream ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(streamId));

        // Serialize topic ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(topicId));

        // Serialize consumer group ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(groupId));

        log.debug("Joining consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .sendAsync(CommandCode.ConsumerGroup.JOIN.getValue(), payload)
                .thenAccept(response -> {
                    log.debug("Successfully joined consumer group");
                    response.release();
                });
    }

    @Override
    public CompletableFuture<Void> leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = Unpooled.buffer();

        // Serialize stream ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(streamId));

        // Serialize topic ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(topicId));

        // Serialize consumer group ID
        payload.writeBytes(AsyncBytesSerializer.toBytes(groupId));

        log.debug("Leaving consumer group - Stream: {}, Topic: {}, Group: {}", streamId, topicId, groupId);

        return connection
                .sendAsync(CommandCode.ConsumerGroup.LEAVE.getValue(), payload)
                .thenAccept(response -> {
                    log.debug("Successfully left consumer group");
                    response.release();
                });
    }
}
