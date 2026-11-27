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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.SystemClient;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.system.Snapshot;
import org.apache.iggy.system.SnapshotCompression;
import org.apache.iggy.system.Stats;
import org.apache.iggy.system.SystemSnapshotType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Async TCP implementation of system client.
 */
public class SystemTcpClient implements SystemClient {
    private static final Logger log = LoggerFactory.getLogger(SystemTcpClient.class);

    private final Supplier<AsyncTcpConnection> connectionSupplier;

    public SystemTcpClient(Supplier<AsyncTcpConnection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    private AsyncTcpConnection connection() {
        return connectionSupplier.get();
    }

    @Override
    public CompletableFuture<Stats> getStats() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting server statistics");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_STATS.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readStats(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<List<OptionSpec>> describeOptions(OptionsScope scope) {
        var payload = Unpooled.buffer(1).writeByte(scope.asCode());

        log.debug("Describing {} options", scope);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.DESCRIBE_OPTIONS.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readOptionSpecs(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Snapshot> snapshot(
            SnapshotCompression compression, List<SystemSnapshotType> snapshotTypes) {
        ByteBuf payload;
        try {
            payload = BytesSerializer.toGetSnapshot(compression, snapshotTypes);
        } catch (IggyInvalidArgumentException refused) {
            return CompletableFuture.failedFuture(refused);
        }

        log.debug("Getting {} snapshot of {}", compression, snapshotTypes);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_SNAPSHOT.getValue(), payload), response -> {
                    try {
                        var data = new byte[response.readableBytes()];
                        response.readBytes(data);
                        return new Snapshot(data);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<MetadataChanges> getMetadataChanges(MetadataChangesQuery query) {
        try {
            query.validate();
        } catch (IggyValidationException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        var payload = BytesSerializer.toGetMetadataChanges(query);

        log.debug("Reading metadata changes");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_METADATA_CHANGES.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readMetadataChanges(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> retireGeoOrigin(String origin) {
        var payload = BytesSerializer.toBytes(origin, "origin");

        log.debug("Retiring geo origin: {}", origin);

        return connection().sendAndRelease(CommandCode.System.RETIRE_GEO_ORIGIN, payload);
    }

    @Override
    public CompletableFuture<ClusterMetadata> getClusterMetadata() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting cluster metadata");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_CLUSTER_METADATA.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readClusterMetadata(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<ClientInfoDetails> getMe() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting current client info");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_ME.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readClientInfoDetails(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<ClientInfoDetails> getClient(Long clientId) {
        var payload = Unpooled.buffer(4);
        payload.writeIntLE(clientId.intValue());

        log.debug("Getting client info for client ID: {}", clientId);

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_CLIENT.getValue(), payload), response -> {
                    try {
                        return BytesDeserializer.readClientInfoDetails(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<List<ClientInfo>> getClients() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting all clients");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.GET_ALL_CLIENTS.getValue(), payload), response -> {
                    try {
                        List<ClientInfo> clients = new ArrayList<>();
                        while (response.isReadable()) {
                            clients.add(BytesDeserializer.readClientInfo(response));
                        }
                        return clients;
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<String> ping() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Pinging server");

        return AsyncTcpConnection.mapResponse(
                connection().send(CommandCode.System.PING.getValue(), payload), response -> {
                    response.release();
                    return "pong";
                });
    }
}
