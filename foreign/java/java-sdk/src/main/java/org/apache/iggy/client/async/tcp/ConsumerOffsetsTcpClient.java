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

import org.apache.iggy.client.async.ConsumerOffsetsClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of consumer offsets client.
 */
public class ConsumerOffsetsTcpClient implements ConsumerOffsetsClient {
    private static final Logger log = LoggerFactory.getLogger(ConsumerOffsetsTcpClient.class);

    private final AsyncTcpConnection connection;

    public ConsumerOffsetsTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Void> storeConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        var payload = BytesSerializer.toBytes(consumer);
        payload.writeBytes(BytesSerializer.toBytes(streamId));
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeBytes(BytesSerializer.toBytes(partitionId));
        payload.writeBytes(BytesSerializer.toBytesAsU64(offset));

        log.debug(
                "Storing consumer offset - Stream: {}, Topic: {}, Partition: {}, Consumer: {}, Offset: {}",
                streamId,
                topicId,
                partitionId,
                consumer,
                offset);

        return connection
                .send(CommandCode.ConsumerOffset.STORE.getValue(), payload)
                .thenAccept(response -> {
                    response.release();
                });
    }

    @Override
    public CompletableFuture<Optional<ConsumerOffsetInfo>> getConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var payload = BytesSerializer.toBytes(consumer);
        payload.writeBytes(BytesSerializer.toBytes(streamId));
        payload.writeBytes(BytesSerializer.toBytes(topicId));
        payload.writeBytes(BytesSerializer.toBytes(partitionId));

        log.debug(
                "Getting consumer offset - Stream: {}, Topic: {}, Partition: {}, Consumer: {}",
                streamId,
                topicId,
                partitionId,
                consumer);

        return connection
                .send(CommandCode.ConsumerOffset.GET.getValue(), payload)
                .thenApply(response -> {
                    try {
                        if (response.isReadable()) {
                            return Optional.of(BytesDeserializer.readConsumerOffsetInfo(response));
                        } else {
                            return Optional.empty();
                        }
                    } finally {
                        response.release();
                    }
                });
    }
}
