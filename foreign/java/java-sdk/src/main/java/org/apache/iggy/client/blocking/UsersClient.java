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
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;

import java.util.List;
import java.util.Optional;

public interface UsersClient {

    default Optional<UserInfoDetails> getUser(Long userId) {
        return getUser(UserId.of(userId));
    }

    Optional<UserInfoDetails> getUser(UserId userId);

    /**
     * Over TCP, reads the list in pages of up to 1000 users, so the list is not one consistent read. All pages share
     * the sign-in and the request timeout of the call, so a new sign-in during the read fails it. Over TCP, the call
     * fails with {@link org.apache.iggy.exception.IggyMalformedResponseException} when the server answers the page
     * cursor it was sent. Over HTTP, reads the whole list in one reply, which the server builds from pages of 1000, so
     * it is not one consistent read either.
     */
    List<UserInfo> getUsers();

    /**
     * Gets one page of users. The name filter applies to the username. A query that breaks the page rules fails with
     * an invalid command error.
     */
    Page<UserInfo> getUsersPage(NamedPageQuery query);

    default UserInfoDetails createUser(
            String username, String password, UserStatus status, Optional<Permissions> permissions) {
        return createUser(username, password, status, permissions, UserOptions.empty());
    }

    UserInfoDetails createUser(
            String username,
            String password,
            UserStatus status,
            Optional<Permissions> permissions,
            UserOptions options);

    default void deleteUser(Long userId) {
        deleteUser(UserId.of(userId));
    }

    void deleteUser(UserId userId);

    default void updateUser(Long userId, UserUpdate update) {
        updateUser(UserId.of(userId), update);
    }

    /** Changes a user's username, status or both. Every key left out keeps its value. */
    void updateUser(UserId userId, UserUpdate update);

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
