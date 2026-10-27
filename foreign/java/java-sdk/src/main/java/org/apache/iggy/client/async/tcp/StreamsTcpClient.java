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
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.iggy.serde.BytesDeserializer.readStreamBase;
import static org.apache.iggy.serde.BytesDeserializer.readStreamDetails;
import static org.apache.iggy.serde.BytesSerializer.toBytes;

/**
 * Async TCP implementation of StreamsClient using Netty for non-blocking I/O.
 */
public class StreamsTcpClient implements StreamsClient {

    private final Supplier<AsyncTcpConnection> connectionSupplier;
    private final Runnable topologyChanged;

    public StreamsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this(connectionSupplier, () -> {});
    }

    /**
     * @param topologyChanged runs after this client deletes a stream, when the
     *     partition contexts cached for its topics no longer hold
     */
    StreamsTcpClient(Supplier<AsyncTcpConnection> connectionSupplier, Runnable topologyChanged) {
        this.connectionSupplier = connectionSupplier;
        this.topologyChanged = topologyChanged;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
    }

    @Override
    public CompletableFuture<StreamDetails> createStream(String name, Map<String, HeaderValue> options) {
        var payload = BytesSerializer.toCreateStream(name, options);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Stream.CREATE.getValue(), payload), response -> {
                    StreamDetails details = readStreamDetails(response);
                    response.release();
                    return details;
                });
    }

    @Override
    public CompletableFuture<Optional<StreamDetails>> getStream(StreamId streamId) {
        var payload = toBytes(streamId);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Stream.GET.getValue(), payload), response -> {
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
    public CompletableFuture<List<StreamBase>> getStreams() {
        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.Stream.GET_ALL.getValue(), Unpooled.EMPTY_BUFFER), response -> {
                    List<StreamBase> streams = new ArrayList<>();
                    while (response.isReadable()) {
                        streams.add(readStreamBase(response));
                    }
                    response.release();
                    return streams;
                });
    }

    @Override
    public CompletableFuture<Void> updateStream(StreamId streamId, StreamUpdate update) {
        var payload = BytesSerializer.toUpdateResource(streamId, update);

        return connection().sendAndRelease(CommandCode.System.UPDATE_RESOURCE, payload);
    }

    @Override
    public CompletableFuture<Void> deleteStream(StreamId streamId) {
        var payload = toBytes(streamId);

        return AsyncTcpConnection.mapResponse(connection().send(CommandCode.Stream.DELETE, payload), response -> {
            response.release();
            topologyChanged.run();
            return null;
        });
    }
}
