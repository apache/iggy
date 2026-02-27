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
import org.apache.iggy.IggyVersion;
import org.apache.iggy.client.async.UsersClient;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.iggy.serde.BytesSerializer.toBytes;

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
    public CompletableFuture<Optional<UserInfoDetails>> getUser(UserId userId) {
        var payload = toBytes(userId);
        return connection.exchangeForOptional(CommandCode.User.GET, payload, BytesDeserializer::readUserInfoDetails);
    }

    @Override
    public CompletableFuture<List<UserInfo>> getUsers() {
        var payload = Unpooled.EMPTY_BUFFER;
        return connection.exchangeForList(CommandCode.User.GET_ALL, payload, BytesDeserializer::readUserInfo);
    }

    @Override
    public CompletableFuture<UserInfoDetails> createUser(
            String username, String password, UserStatus status, Optional<Permissions> permissions) {
        var payload = Unpooled.buffer();
        payload.writeBytes(toBytes(username));
        payload.writeBytes(toBytes(password));
        payload.writeByte(status.asCode());
        permissions.ifPresentOrElse(
                perms -> {
                    payload.writeByte(1);
                    var permissionBytes = toBytes(perms);
                    payload.writeIntLE(permissionBytes.readableBytes());
                    payload.writeBytes(permissionBytes);
                },
                () -> payload.writeByte(0));

        return connection.exchangeForEntity(CommandCode.User.CREATE, payload, BytesDeserializer::readUserInfoDetails);
    }

    @Override
    public CompletableFuture<Void> deleteUser(UserId userId) {
        var payload = toBytes(userId);
        return connection.sendAndRelease(CommandCode.User.DELETE, payload);
    }

    @Override
    public CompletableFuture<Void> updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status) {
        var payload = toBytes(userId);
        username.ifPresentOrElse(
                un -> {
                    payload.writeByte(1);
                    payload.writeBytes(toBytes(un));
                },
                () -> payload.writeByte(0));
        status.ifPresentOrElse(
                s -> {
                    payload.writeByte(1);
                    payload.writeByte(s.asCode());
                },
                () -> payload.writeByte(0));

        return connection.sendAndRelease(CommandCode.User.UPDATE, payload);
    }

    @Override
    public CompletableFuture<Void> updatePermissions(UserId userId, Optional<Permissions> permissions) {
        var payload = toBytes(userId);

        permissions.ifPresentOrElse(
                perms -> {
                    payload.writeByte(1);
                    var permissionBytes = toBytes(perms);
                    payload.writeIntLE(permissionBytes.readableBytes());
                    payload.writeBytes(permissionBytes);
                },
                () -> payload.writeByte(0));

        return connection.sendAndRelease(CommandCode.User.UPDATE_PERMISSIONS, payload);
    }

    @Override
    public CompletableFuture<Void> changePassword(UserId userId, String currentPassword, String newPassword) {
        var payload = toBytes(userId);
        payload.writeBytes(toBytes(currentPassword));
        payload.writeBytes(toBytes(newPassword));

        return connection.sendAndRelease(CommandCode.User.CHANGE_PASSWORD, payload);
    }

    @Override
    public CompletableFuture<IdentityInfo> login(String username, String password) {
        String version = IggyVersion.getInstance().getUserAgent();
        String context = IggyVersion.getInstance().toString();

        var payload = Unpooled.buffer();
        var usernameBytes = toBytes(username);
        var passwordBytes = toBytes(password);

        payload.writeBytes(usernameBytes);
        payload.writeBytes(passwordBytes);
        payload.writeIntLE(version.length());
        payload.writeBytes(version.getBytes());
        payload.writeIntLE(context.length());
        payload.writeBytes(context.getBytes());

        log.debug("Logging in user: {}", username);

        return connection.send(CommandCode.User.LOGIN.getValue(), payload).thenApply(response -> {
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
    public CompletableFuture<Void> logout() {
        var payload = Unpooled.buffer(0); // Empty payload for logout

        log.debug("Logging out");

        return connection.send(CommandCode.User.LOGOUT.getValue(), payload).thenAccept(response -> {
            response.release();
            log.debug("Logged out successfully");
        });
    }
}
