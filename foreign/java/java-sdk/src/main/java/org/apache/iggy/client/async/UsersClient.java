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
 * Async client for user management operations.
 */
public interface UsersClient {

    /**
     * Logs in to the server with the specified username and password.
     *
     * @param username The username to login with
     * @param password The password to login with
     * @return A CompletableFuture that completes with the user's identity information
     */
    CompletableFuture<IdentityInfo> login(String username, String password);

    /**
     * Logs out from the server.
     *
     * @return A CompletableFuture that completes when logout is successful
     */
    CompletableFuture<Void> logout();
}
