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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.blocking.UsersClient;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;

import java.util.List;
import java.util.Optional;

import static org.apache.iggy.client.blocking.tcp.BytesSerializer.nameToBytes;
import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytes;

class UsersTcpClient implements UsersClient {

    private final InternalTcpClient tcpClient;

    UsersTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Optional<UserInfoDetails> getUser(UserId userId) {
        var payload = toBytes(userId);
        return tcpClient.exchangeForOptional(CommandCode.User.GET, payload, BytesDeserializer::readUserInfoDetails);
    }

    @Override
    public List<UserInfo> getUsers() {
        return tcpClient.exchangeForList(CommandCode.User.GET_ALL, BytesDeserializer::readUserInfo);
    }

    @Override
    public UserInfoDetails createUser(
            String username, String password, UserStatus status, Optional<Permissions> permissions) {
        var payload = Unpooled.buffer();
        payload.writeBytes(nameToBytes(username));
        payload.writeBytes(nameToBytes(password));
        payload.writeByte(status.asCode());
        permissions.ifPresentOrElse(
                perms -> {
                    payload.writeByte(1);
                    var permissionBytes = toBytes(perms);
                    payload.writeIntLE(permissionBytes.readableBytes());
                    payload.writeBytes(permissionBytes);
                },
                () -> payload.writeByte(0));

        return tcpClient.exchangeForEntity(CommandCode.User.CREATE, payload, BytesDeserializer::readUserInfoDetails);
    }

    @Override
    public void deleteUser(UserId userId) {
        var payload = toBytes(userId);
        tcpClient.send(CommandCode.User.DELETE, payload);
    }

    @Override
    public void updateUser(UserId userId, Optional<String> usernameOptional, Optional<UserStatus> statusOptional) {
        var payload = toBytes(userId);
        usernameOptional.ifPresentOrElse(
                (username) -> {
                    payload.writeByte(1);
                    payload.writeBytes(nameToBytes(username));
                },
                () -> payload.writeByte(0));
        statusOptional.ifPresentOrElse(
                (status) -> {
                    payload.writeByte(1);
                    payload.writeByte(status.asCode());
                },
                () -> payload.writeByte(0));

        tcpClient.send(CommandCode.User.UPDATE, payload);
    }

    @Override
    public void updatePermissions(UserId userId, Optional<Permissions> permissionsOptional) {
        var payload = toBytes(userId);

        permissionsOptional.ifPresentOrElse(
                permissions -> {
                    payload.writeByte(1);
                    var permissionBytes = toBytes(permissions);
                    payload.writeIntLE(permissionBytes.readableBytes());
                    payload.writeBytes(permissionBytes);
                },
                () -> payload.writeByte(0));

        tcpClient.send(CommandCode.User.UPDATE_PERMISSIONS, payload);
    }

    @Override
    public void changePassword(UserId userId, String currentPassword, String newPassword) {
        var payload = toBytes(userId);
        payload.writeBytes(nameToBytes(currentPassword));
        payload.writeBytes(nameToBytes(newPassword));

        tcpClient.send(CommandCode.User.CHANGE_PASSWORD, payload);
    }

    @Override
    public IdentityInfo login(String username, String password) {
        String version = "0.6.30";
        String context = "java-sdk";
        var payloadSize = 2 + username.length() + password.length() + 4 + version.length() + 4 + context.length();
        var payload = Unpooled.buffer(payloadSize);

        payload.writeBytes(nameToBytes(username));
        payload.writeBytes(nameToBytes(password));
        payload.writeIntLE(version.length());
        payload.writeBytes(version.getBytes());
        payload.writeIntLE(context.length());
        payload.writeBytes(context.getBytes());

        var userId = tcpClient.exchangeForEntity(CommandCode.User.LOGIN, payload, ByteBuf::readUnsignedIntLE);
        return new IdentityInfo(userId, Optional.empty());
    }

    @Override
    public void logout() {
        tcpClient.send(CommandCode.User.LOGOUT);
    }
}
