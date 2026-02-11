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
import org.apache.iggy.client.async.PersonalAccessTokensClient;
import org.apache.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import org.apache.iggy.personalaccesstoken.RawPersonalAccessToken;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of personal access tokens client.
 */
public class PersonalAccessTokensTcpClient implements PersonalAccessTokensClient {
    private static final Logger log = LoggerFactory.getLogger(PersonalAccessTokensTcpClient.class);

    private final AsyncTcpConnection connection;

    public PersonalAccessTokensTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<RawPersonalAccessToken> createPersonalAccessToken(String name, BigInteger expiry) {
        var payload = Unpooled.buffer();
        payload.writeBytes(BytesSerializer.toBytes(name));
        payload.writeBytes(BytesSerializer.toBytesAsU64(expiry));

        log.debug("Creating personal access token: {}", name);

        return connection
                .send(CommandCode.PersonalAccessToken.CREATE.getValue(), payload)
                .thenApply(response -> {
                    try {
                        return BytesDeserializer.readRawPersonalAccessToken(response);
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<List<PersonalAccessTokenInfo>> getPersonalAccessTokens() {
        var payload = Unpooled.EMPTY_BUFFER;

        log.debug("Getting all personal access tokens");

        return connection
                .send(CommandCode.PersonalAccessToken.GET_ALL.getValue(), payload)
                .thenApply(response -> {
                    try {
                        List<PersonalAccessTokenInfo> tokens = new ArrayList<>();
                        while (response.isReadable()) {
                            tokens.add(BytesDeserializer.readPersonalAccessTokenInfo(response));
                        }
                        return tokens;
                    } finally {
                        response.release();
                    }
                });
    }

    @Override
    public CompletableFuture<Void> deletePersonalAccessToken(String name) {
        var payload = BytesSerializer.toBytes(name);

        log.debug("Deleting personal access token: {}", name);

        return connection
                .send(CommandCode.PersonalAccessToken.DELETE.getValue(), payload)
                .thenAccept(response -> {
                    response.release();
                });
    }

    @Override
    public CompletableFuture<IdentityInfo> loginWithPersonalAccessToken(String token) {
        var payload = BytesSerializer.toBytes(token);

        log.debug("Logging in with personal access token");

        return connection
                .send(CommandCode.PersonalAccessToken.LOGIN.getValue(), payload)
                .thenApply(response -> {
                    try {
                        var userId = response.readUnsignedIntLE();
                        return new IdentityInfo(userId, Optional.empty());
                    } finally {
                        response.release();
                    }
                });
    }
}
