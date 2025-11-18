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
import org.apache.iggy.client.async.StreamsClient;
import org.apache.iggy.client.blocking.tcp.CommandCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.iggy.client.async.tcp.AsyncBytesDeserializer.readStreamBase;
import static org.apache.iggy.client.async.tcp.AsyncBytesDeserializer.readStreamDetails;
import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.nameToBytes;
import static org.apache.iggy.client.async.tcp.AsyncBytesSerializer.toBytes;

/**
 * Async TCP implementation of StreamsClient using Netty for non-blocking I/O.
 */
public class StreamsTcpClient implements StreamsClient {

    private final AsyncTcpConnection connection;

    public StreamsTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<StreamDetails> createStreamAsync(String name) {
        var payloadSize = 1 + name.length();
        var payload = Unpooled.buffer(payloadSize);

        payload.writeBytes(nameToBytes(name));

        return connection
                .sendAsync(CommandCode.Stream.CREATE.getValue(), payload)
                .thenApply(response -> {
                    StreamDetails details = readStreamDetails(response);
                    response.release();
                    return details;
                });
    }

    @Override
    public CompletableFuture<Optional<StreamDetails>> getStreamAsync(StreamId streamId) {
        var payload = toBytes(streamId);

        return connection.sendAsync(CommandCode.Stream.GET.getValue(), payload).thenApply(response -> {
            Optional<StreamDetails> result;
            if (response.isReadable()) {
                result = Optional.of(readStreamDetails(response));
            } else {
                result = Optional.empty();
            }
            response.release();
            return result;
        });
    }

    @Override
    public CompletableFuture<List<StreamBase>> getStreamsAsync() {
        return connection
                .sendAsync(CommandCode.Stream.GET_ALL.getValue(), Unpooled.EMPTY_BUFFER)
                .thenApply(response -> {
                    List<StreamBase> streams = new ArrayList<>();
                    while (response.isReadable()) {
                        streams.add(readStreamBase(response));
                    }
                    response.release();
                    return streams;
                });
    }

    @Override
    public CompletableFuture<Void> updateStreamAsync(StreamId streamId, String name) {
        var payloadSize = 1 + name.length();
        var idBytes = toBytes(streamId);
        var payload = Unpooled.buffer(payloadSize + idBytes.capacity());

        payload.writeBytes(idBytes);
        payload.writeBytes(nameToBytes(name));

        return connection
                .sendAsync(CommandCode.Stream.UPDATE.getValue(), payload)
                .thenAccept(response -> response.release());
    }

    @Override
    public CompletableFuture<Void> deleteStreamAsync(StreamId streamId) {
        var payload = toBytes(streamId);

        return connection
                .sendAsync(CommandCode.Stream.DELETE.getValue(), payload)
                .thenAccept(response -> response.release());
    }
}
