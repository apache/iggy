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

import org.apache.iggy.client.blocking.UsersClient;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;

import java.util.List;
import java.util.Optional;

final class UsersTcpClient implements UsersClient {

    private final org.apache.iggy.client.async.UsersClient delegate;

    UsersTcpClient(org.apache.iggy.client.async.UsersClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<UserInfoDetails> getUser(UserId userId) {
        return FutureUtil.resolve(delegate.getUser(userId));
    }

    @Override
    public List<UserInfo> getUsers() {
        return FutureUtil.resolve(delegate.getUsers());
    }

    @Override
    public UserInfoDetails createUser(
            String username, String password, UserStatus status, Optional<Permissions> permissions) {
        return FutureUtil.resolve(delegate.createUser(username, password, status, permissions));
    }

    @Override
    public void deleteUser(UserId userId) {
        FutureUtil.resolve(delegate.deleteUser(userId));
    }

    @Override
    public void updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status) {
        FutureUtil.resolve(delegate.updateUser(userId, username, status));
    }

    @Override
    public void updatePermissions(UserId userId, Optional<Permissions> permissions) {
        FutureUtil.resolve(delegate.updatePermissions(userId, permissions));
    }

    @Override
    public void changePassword(UserId userId, String currentPassword, String newPassword) {
        FutureUtil.resolve(delegate.changePassword(userId, currentPassword, newPassword));
    }

    @Override
    public IdentityInfo login(String username, String password) {
        return FutureUtil.resolve(delegate.login(username, password));
    }

    @Override
    public void logout() {
        FutureUtil.resolve(delegate.logout());
    }
}
