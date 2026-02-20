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
import org.apache.iggy.client.async.SystemClient;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of system client.
 */
public class SystemTcpClient implements SystemClient {
    private static final Logger log = LoggerFactory.getLogger(SystemTcpClient.class);

    private final AsyncTcpConnection connection;

    public SystemTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Stats> getStats() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting server statistics");

        return connection.send(CommandCode.System.GET_STATS.getValue(), payload).thenApply(response -> {
            try {
                return BytesDeserializer.readStats(response);
            } finally {
                response.release();
            }
        });
    }

    @Override
    public CompletableFuture<ClientInfoDetails> getMe() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting current client info");

        return connection.send(CommandCode.System.GET_ME.getValue(), payload).thenApply(response -> {
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

        return connection
                .send(CommandCode.System.GET_CLIENT.getValue(), payload)
                .thenApply(response -> {
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

        return connection
                .send(CommandCode.System.GET_ALL_CLIENTS.getValue(), payload)
                .thenApply(response -> {
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

        return connection.send(CommandCode.System.PING.getValue(), payload).thenApply(response -> {
            response.release();
            return "";
        });
    }
}
