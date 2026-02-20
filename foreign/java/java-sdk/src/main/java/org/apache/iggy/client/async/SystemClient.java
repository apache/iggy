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

import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.Stats;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Async interface for system operations.
 */
public interface SystemClient {

    /**
     * Gets server statistics asynchronously.
     *
     * @return A CompletableFuture containing server statistics
     */
    CompletableFuture<Stats> getStats();

    /**
     * Gets information about the current client asynchronously.
     *
     * @return A CompletableFuture containing current client details
     */
    CompletableFuture<ClientInfoDetails> getMe();

    /**
     * Gets information about a specific client asynchronously.
     *
     * @param clientId The ID of the client to retrieve
     * @return A CompletableFuture containing client details
     */
    CompletableFuture<ClientInfoDetails> getClient(Long clientId);

    /**
     * Gets a list of all connected clients asynchronously.
     *
     * @return A CompletableFuture containing list of client information
     */
    CompletableFuture<List<ClientInfo>> getClients();

    /**
     * Pings the server asynchronously.
     *
     * @return A CompletableFuture that completes when ping succeeds
     */
    CompletableFuture<String> ping();
}
