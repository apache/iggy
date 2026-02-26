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

package org.apache.iggy.client.async;

import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async client interface for user authentication operations.
 *
 * <p>Authentication is required before performing any data operations on the server.
 * The client must successfully log in before creating streams, sending messages, or
 * consuming data.
 *
 * <p>Usage example:
 * <pre>{@code
 * UsersClient users = client.users();
 *
 * // Login and chain subsequent operations
 * users.login("iggy", "iggy")
 *     .thenAccept(identity -> System.out.println("Logged in as user: " + identity.userId()))
 *     .exceptionally(ex -> {
 *         System.err.println("Login failed: " + ex.getMessage());
 *         return null;
 *     });
 * }</pre>
 *
 * <p>For convenience, credentials can be provided at client construction time and used
 * with {@link org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#login()}, or the
 * builder's {@link org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder#buildAndLogin()}
 * method can handle connection and login in a single step.
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#users()
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder#buildAndLogin()
 */
public interface UsersClient {

    /**
     * Get the details of a user by the ID provided.
     *
     * @see #getUser(UserId)
     */
    default CompletableFuture<Optional<UserInfoDetails>> getUser(Long userId) {
        return getUser(UserId.of(userId));
    }

    /**
     * Get the details of a user by the ID provided.
     *
     * @param userId the ID of the user to retrieve information about.
     * @return a {@link CompletableFuture} that completes with the user's {@link UserInfoDetails} on success
     */
    CompletableFuture<Optional<UserInfoDetails>> getUser(UserId userId);

    /**
     * Get a list of the users currently registered.
     *
     * @return A {@link CompletableFuture} that completes with a list of {@link UserInfo} about each of the registered users.
     */
    CompletableFuture<List<UserInfo>> getUsers();

    /**
     * Create a user with the details provided.
     *
     * @param username The username of the new user.
     * @param password The password of the new user.
     * @param status The status of the new user.
     * @param permissions An optional set of permissions for the new user.
     * @return A {@link CompletableFuture} that completes with a {@link UserInfoDetails} that gives information about the newly created user.
     */
    CompletableFuture<UserInfoDetails> createUser(
            String username, String password, UserStatus status, Optional<Permissions> permissions);

    /**
     * Delete the user with the given ID.
     *
     * @see #deleteUser(UserId)
     */
    default CompletableFuture<Void> deleteUser(Long userId) {
        return deleteUser(UserId.of(userId));
    }

    /**
     * Delete the user with the given ID.
     *
     * @param userId The ID of the user to delete.
     * @return A {@link CompletableFuture} that completes but yields no value.
     */
    CompletableFuture<Void> deleteUser(UserId userId);

    /**
     * Update the user identified by the given userId, setting (if provided) their username and or status.
     *
     * @see #updateUser(UserId, Optional, Optional)
     */
    default CompletableFuture<Void> updateUser(Long userId, Optional<String> username, Optional<UserStatus> status) {
        return updateUser(UserId.of(userId), username, status);
    }

    /**
     * Update the user identified by the given userId, setting (if provided) their username and or status.
     * @param userId The ID of the user to update.
     * @param username The new username of the user, or an empty optional if no update is required.
     * @param status The new status of the user, or an empty optional if no update is required.
     * @return A {@link CompletableFuture} that completes but yields no value.
     */
    CompletableFuture<Void> updateUser(UserId userId, Optional<String> username, Optional<UserStatus> status);

    /**
     * Update the permissions of the user identified by the provided userId.
     *
     * @see #updatePermissions(UserId, Optional)
     */
    default CompletableFuture<Void> updatePermissions(Long userId, Optional<Permissions> permissions) {
        return updatePermissions(UserId.of(userId), permissions);
    }

    /**
     * Update the permissions of the user identified by the provided userId.
     *
     * @param userId The ID of the user of which to update permissions
     * @param permissions The new permissions of the user
     * @return A {@link CompletableFuture} that completes but yields no value.
     */
    CompletableFuture<Void> updatePermissions(UserId userId, Optional<Permissions> permissions);

    /**
     * Change the password of the user identifier by the given userId.
     *
     * @see #changePassword(UserId, String, String)
     */
    default CompletableFuture<Void> changePassword(Long userId, String currentPassword, String newPassword) {
        return changePassword(UserId.of(userId), currentPassword, newPassword);
    }

    /**
     * Change the password of the user identifier by the given userId.
     *
     * @param userId The ID of the user whose password should be changed.
     * @param currentPassword The current password of the user
     * @param newPassword The new password of the user
     * @return A {@link CompletableFuture} that completes but yields no value.
     */
    CompletableFuture<Void> changePassword(UserId userId, String currentPassword, String newPassword);

    /**
     * Logs in to the Iggy server with the specified credentials.
     *
     * <p>A successful login returns the authenticated user's identity information
     * and authorizes the connection for subsequent operations. Each TCP connection
     * maintains its own authentication state.
     *
     * @param username the username to authenticate with
     * @param password the password to authenticate with
     * @return a {@link CompletableFuture} that completes with the user's
     *         {@link IdentityInfo} on success
     * @throws org.apache.iggy.exception.IggyException if the credentials are invalid
     */
    CompletableFuture<IdentityInfo> login(String username, String password);

    /**
     * Logs out from the Iggy server and invalidates the current session.
     *
     * <p>After logout, the connection remains open but no data operations can be
     * performed until the client logs in again.
     *
     * @return a {@link CompletableFuture} that completes when logout is successful
     */
    CompletableFuture<Void> logout();
}
