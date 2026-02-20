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

import org.apache.iggy.user.IdentityInfo;

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
