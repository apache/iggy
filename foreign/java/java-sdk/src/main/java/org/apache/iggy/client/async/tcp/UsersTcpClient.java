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
import org.apache.iggy.client.async.UsersClient;
import org.apache.iggy.client.blocking.tcp.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP implementation of users client.
 */
public class UsersTcpClient implements UsersClient {
    private static final Logger log = LoggerFactory.getLogger(UsersTcpClient.class);

    private final AsyncTcpConnection connection;

    public UsersTcpClient(AsyncTcpConnection connection) {
        this.connection = connection;
    }

    @Override
    public CompletableFuture<IdentityInfo> loginAsync(String username, String password) {
        String version = "0.6.30";
        String context = "java-sdk";

        var payload = Unpooled.buffer();
        var usernameBytes = AsyncBytesSerializer.toBytes(username);
        var passwordBytes = AsyncBytesSerializer.toBytes(password);

        payload.writeBytes(usernameBytes);
        payload.writeBytes(passwordBytes);
        payload.writeIntLE(version.length());
        payload.writeBytes(version.getBytes());
        payload.writeIntLE(context.length());
        payload.writeBytes(context.getBytes());

        log.debug("Logging in user: {}", username);

        return connection.sendAsync(CommandCode.User.LOGIN.getValue(), payload).thenApply(response -> {
            try {
                // Read the user ID from response (4-byte unsigned int LE)
                var userId = response.readUnsignedIntLE();
                return new IdentityInfo(userId, Optional.empty());
            } finally {
                response.release();
            }
        });
    }

    @Override
    public CompletableFuture<Void> logoutAsync() {
        var payload = Unpooled.buffer(0); // Empty payload for logout

        log.debug("Logging out");

        return connection.sendAsync(CommandCode.User.LOGOUT.getValue(), payload).thenAccept(response -> {
            response.release();
            log.debug("Logged out successfully");
        });
    }
}
