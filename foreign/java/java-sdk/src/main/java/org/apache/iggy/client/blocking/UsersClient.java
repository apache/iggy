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

package org.apache.iggy.client.blocking;

import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;

import java.util.List;
import java.util.Optional;

public interface UsersClient {

    default Optional<UserInfoDetails> getUser(Long userId) {
        return getUser(UserId.of(userId));
    }

    Optional<UserInfoDetails> getUser(UserId userId);

    List<UserInfo> getUsers();

    UserInfoDetails createUser(String username, String password, UserStatus status, Optional<Permissions> permissions);

    default void deleteUser(Long userId) {
        deleteUser(UserId.of(userId));
    }

    void deleteUser(UserId userId);

    default void updateUser(Long userId, Optional<String> username, Optional<UserStatus> status) {
        updateUser(UserId.of(userId), username, status);
    }

    void updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status);

    default void updatePermissions(Long userId, Optional<Permissions> permissions) {
        updatePermissions(UserId.of(userId), permissions);
    }

    void updatePermissions(UserId userId, Optional<Permissions> permissions);

    default void changePassword(Long userId, String currentPassword, String newPassword) {
        changePassword(UserId.of(userId), currentPassword, newPassword);
    }

    void changePassword(UserId userId, String currentPassword, String newPassword);

    IdentityInfo login(String username, String password);

    void logout();
}
