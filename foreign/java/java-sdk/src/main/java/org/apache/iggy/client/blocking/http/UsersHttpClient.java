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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.IggyVersion;
import org.apache.iggy.client.blocking.UsersClient;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.TokenInfo;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;
import tools.jackson.core.type.TypeReference;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class UsersHttpClient implements UsersClient {

    private static final String USERS = "/users";
    private final InternalHttpClient httpClient;

    public UsersHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<UserInfoDetails> getUser(UserId userId) {
        var request = httpClient.prepareGetRequest(USERS + "/" + userId);
        return httpClient
                .executeWithOptionalResponse(request, HttpUserDetails.class)
                .map(HttpUserDetails::toUserInfoDetails);
    }

    @Override
    public List<UserInfo> getUsers() {
        var request = httpClient.prepareGetRequest(USERS);
        List<HttpUser> users = httpClient.execute(request, new TypeReference<>() {});
        return users.stream().map(HttpUser::toUserInfo).toList();
    }

    @Override
    public UserInfoDetails createUser(
            String username,
            String password,
            UserStatus status,
            Optional<Permissions> permissions,
            UserOptions options) {
        var request = httpClient.preparePostRequest(
                USERS,
                new CreateUser(
                        username, password, status, permissions, HttpOptionValue.requestValues(options.entries())));
        return httpClient.execute(request, HttpUserDetails.class).toUserInfoDetails();
    }

    @Override
    public void deleteUser(UserId userId) {
        var request = httpClient.prepareDeleteRequest(USERS + "/" + userId);
        httpClient.execute(request);
    }

    @Override
    public void updateUser(UserId userId, UserUpdate update) {
        var request = httpClient.preparePutRequest(
                USERS + "/" + userId, HttpOptionValue.updateBody(update.entries(), update.resetKeys()));
        httpClient.execute(request);
    }

    @Override
    public void updatePermissions(UserId userId, Optional<Permissions> permissions) {
        var request =
                httpClient.preparePutRequest(USERS + "/" + userId + "/permissions", new UpdatePermissions(permissions));
        httpClient.execute(request);
    }

    @Override
    public void changePassword(UserId userId, String currentPassword, String newPassword) {
        var request = httpClient.preparePutRequest(
                USERS + "/" + userId + "/password", new ChangePassword(currentPassword, newPassword));
        httpClient.execute(request);
    }

    @Override
    public IdentityInfo login(String username, String password) {
        String version = IggyVersion.getInstance().getUserAgent();
        String context = IggyVersion.getInstance().toString();
        var request = httpClient.preparePostRequest(USERS + "/login", new Login(username, password, version, context));
        var response = httpClient.execute(request, IdentityInfo.class);
        httpClient.setToken(response.accessToken().map(TokenInfo::token));
        return response;
    }

    @Override
    public void logout() {
        var request = httpClient.prepareDeleteRequest(USERS + "/logout");
        httpClient.execute(request);
        httpClient.setToken(Optional.empty());
    }

    record Login(String username, String password, String version, String context) {}

    private record CreateUser(
            String username,
            String password,
            UserStatus status,
            Optional<Permissions> permissions,
            Map<String, Object> options) {}

    private record UpdatePermissions(Optional<Permissions> permissions) {}

    private record ChangePassword(String currentPassword, String newPassword) {}

    /**
     * The REST shape of a user: one option map with a per-entry provenance flag, split here into the two
     * blocks the binary protocol sends.
     */
    record HttpUser(
            Long id, BigInteger createdAt, UserStatus status, String username, Map<String, HttpOptionValue> options) {

        UserInfo toUserInfo() {
            return new UserInfo(
                    id,
                    createdAt,
                    status,
                    username,
                    HttpOptionValue.select(options, true),
                    HttpOptionValue.select(options, false));
        }
    }

    /** The REST shape of a user with its permissions. See {@link HttpUser}. */
    record HttpUserDetails(
            Long id,
            BigInteger createdAt,
            UserStatus status,
            String username,
            Optional<Permissions> permissions,
            Map<String, HttpOptionValue> options) {

        UserInfoDetails toUserInfoDetails() {
            return new UserInfoDetails(
                    id,
                    createdAt,
                    status,
                    username,
                    permissions,
                    HttpOptionValue.select(options, true),
                    HttpOptionValue.select(options, false));
        }
    }
}
