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
import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;

import java.util.List;
import java.util.Optional;

import static org.apache.iggy.client.blocking.tcp.BytesSerializer.nameToBytes;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytes;

class StreamsTcpClient implements StreamsClient {

    private final InternalTcpClient tcpClient;

    StreamsTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public StreamDetails createStream(String name) {
        var payloadSize = 1 + name.length();
        var payload = Unpooled.buffer(payloadSize);

        payload.writeBytes(nameToBytes(name));
        return tcpClient.exchangeForEntity(CommandCode.Stream.CREATE, payload, BytesDeserializer::readStreamDetails);
    }

    @Override
    public Optional<StreamDetails> getStream(StreamId streamId) {
        var payload = toBytes(streamId);
        return tcpClient.exchangeForOptional(CommandCode.Stream.GET, payload, BytesDeserializer::readStreamDetails);
    }

    @Override
    public List<StreamBase> getStreams() {
        return tcpClient.exchangeForList(CommandCode.Stream.GET_ALL, BytesDeserializer::readStreamBase);
    }

    @Override
    public void updateStream(StreamId streamId, String name) {
        var payloadSize = 1 + name.length();
        var idBytes = toBytes(streamId);
        var payload = Unpooled.buffer(payloadSize + idBytes.capacity());

        payload.writeBytes(idBytes);
        payload.writeBytes(nameToBytes(name));
        tcpClient.send(CommandCode.Stream.UPDATE, payload);
    }

    @Override
    public void deleteStream(StreamId streamId) {
        var payload = toBytes(streamId);
        tcpClient.send(CommandCode.Stream.DELETE, payload);
    }
}
