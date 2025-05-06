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

import rs.iggy.clients.blocking.ConsumerOffsetsClient;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.consumeroffset.ConsumerOffsetInfo;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.readConsumerOffsetInfo;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytes;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytesAsU64;

class ConsumerOffsetTcpClient implements ConsumerOffsetsClient {

    private static final int GET_CONSUMER_OFFSET_CODE = 120;
    private static final int STORE_CONSUMER_OFFSET_CODE = 121;

    private final InternalTcpClient tcpClient;

    public ConsumerOffsetTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public void storeConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        var payload = toBytes(consumer);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionId.orElse(0L).intValue());
        payload.writeBytes(toBytesAsU64(offset));

        tcpClient.send(STORE_CONSUMER_OFFSET_CODE, payload);
    }

    @Override
    public Optional<ConsumerOffsetInfo> getConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var payload = toBytes(consumer);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionId.orElse(0L).intValue());

        var response = tcpClient.send(GET_CONSUMER_OFFSET_CODE, payload);
        if (response.isReadable()) {
            return Optional.of(readConsumerOffsetInfo(response));
        }
        return Optional.empty();
    }

}
