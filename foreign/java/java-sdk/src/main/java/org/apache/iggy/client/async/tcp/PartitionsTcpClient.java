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

import org.apache.iggy.client.async.PartitionsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of partitions client.
 */
public class PartitionsTcpClient implements PartitionsClient {
    private static final Logger log = LoggerFactory.getLogger(PartitionsTcpClient.class);

    private final AsyncTcpConnection connection;

    public PartitionsTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Void> createPartitions(StreamId streamId, TopicId topicId, Long partitionsCount) {
        var payload = BytesSerializer.toBytes(streamId);
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeIntLE(partitionsCount.intValue());

        log.debug("Creating {} partitions for stream: {}, topic: {}", partitionsCount, streamId, topicId);

        return connection.send(CommandCode.Partition.CREATE.getValue(), payload).thenAccept(response -> {
            response.release();
        });
    }

    @Override
    public CompletableFuture<Void> deletePartitions(StreamId streamId, TopicId topicId, Long partitionsCount) {
        var payload = BytesSerializer.toBytes(streamId);
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeIntLE(partitionsCount.intValue());

        log.debug("Deleting {} partitions for stream: {}, topic: {}", partitionsCount, streamId, topicId);

        return connection.send(CommandCode.Partition.DELETE.getValue(), payload).thenAccept(response -> {
            response.release();
        });
    }
}
