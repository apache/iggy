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

import io.netty.buffer.Unpooled;
import rs.iggy.clients.blocking.UsersClient;
import rs.iggy.identifier.UserId;
import rs.iggy.user.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.readUserInfoDetails;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.nameToBytes;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytes;

class UsersTcpClient implements UsersClient {

    private static final int GET_USER_CODE = 31;
    private static final int GET_USERS_CODE = 32;
    private static final int CREATE_USER_CODE = 33;
    private static final int DELETE_USER_CODE = 34;
    private static final int UPDATE_USER_CODE = 35;
    private static final int UPDATE_PERMISSIONS_CODE = 36;
    private static final int CHANGE_PASSWORD_CODE = 37;
    private static final int LOGIN_USER_CODE = 38;
    private static final int LOGOUT_USER_CODE = 39;

    private final InternalTcpClient tcpClient;

    UsersTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Optional<UserInfoDetails> getUser(UserId userId) {
        var payload = toBytes(userId);
        var response = tcpClient.send(GET_USER_CODE, payload);
        if (response.isReadable()) {
            return Optional.of(readUserInfoDetails(response));
        }
        return Optional.empty();
    }

    @Override
    public List<UserInfo> getUsers() {
        var response = tcpClient.send(GET_USERS_CODE);
        List<UserInfo> users = new ArrayList<>();
        while (response.isReadable()) {
            users.add(BytesDeserializer.readUserInfo(response));
        }
        return users;
    }

    @Override
    public UserInfoDetails createUser(String username, String password, UserStatus status, Optional<Permissions> permissions) {
        var payload = Unpooled.buffer();
        payload.writeBytes(nameToBytes(username));
        payload.writeBytes(nameToBytes(password));
        payload.writeByte(status.asCode());
        permissions.ifPresentOrElse(perms -> {
            payload.writeByte(1);
            var permissionBytes = toBytes(perms);
            payload.writeIntLE(permissionBytes.readableBytes());
            payload.writeBytes(permissionBytes);
        }, () -> payload.writeByte(0));

        var response = tcpClient.send(CREATE_USER_CODE, payload);
        return readUserInfoDetails(response);
    }

    @Override
    public void deleteUser(UserId userId) {
        var payload = toBytes(userId);
        tcpClient.send(DELETE_USER_CODE, payload);
    }

    @Override
    public void updateUser(UserId userId, Optional<String> usernameOptional, Optional<UserStatus> statusOptional) {
        var payload = toBytes(userId);
        usernameOptional.ifPresentOrElse((username) -> {
            payload.writeByte(1);
            payload.writeBytes(nameToBytes(username));
        }, () -> payload.writeByte(0));
        statusOptional.ifPresentOrElse((status) -> {
            payload.writeByte(1);
            payload.writeByte(status.asCode());
        }, () -> payload.writeByte(0));

        tcpClient.send(UPDATE_USER_CODE, payload);
    }

    @Override
    public void updatePermissions(UserId userId, Optional<Permissions> permissionsOptional) {
        var payload = toBytes(userId);

        permissionsOptional.ifPresentOrElse(permissions -> {
            payload.writeByte(1);
            var permissionBytes = toBytes(permissions);
            payload.writeIntLE(permissionBytes.readableBytes());
            payload.writeBytes(permissionBytes);
        }, () -> payload.writeByte(0));

        tcpClient.send(UPDATE_PERMISSIONS_CODE, payload);
    }

    @Override
    public void changePassword(UserId userId, String currentPassword, String newPassword) {
        var payload = toBytes(userId);
        payload.writeBytes(nameToBytes(currentPassword));
        payload.writeBytes(nameToBytes(newPassword));

        tcpClient.send(CHANGE_PASSWORD_CODE, payload);
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

        var response = tcpClient.send(LOGIN_USER_CODE, payload);

        var userId = response.readUnsignedIntLE();
        return new IdentityInfo(userId, Optional.empty());
    }

    @Override
    public void logout() {
        tcpClient.send(LOGOUT_USER_CODE);
    }
}
