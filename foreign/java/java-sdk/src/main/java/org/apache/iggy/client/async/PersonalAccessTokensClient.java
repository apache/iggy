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

import org.apache.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import org.apache.iggy.personalaccesstoken.RawPersonalAccessToken;
import org.apache.iggy.user.IdentityInfo;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Async interface for personal access token operations.
 */
public interface PersonalAccessTokensClient {

    /**
     * Creates a new personal access token asynchronously.
     *
     * @param name The name of the token
     * @param expiry The expiration time in seconds (0 for no expiration)
     * @return A CompletableFuture containing the created raw personal access token
     */
    CompletableFuture<RawPersonalAccessToken> createPersonalAccessToken(String name, BigInteger expiry);

    /**
     * Gets all personal access tokens asynchronously.
     *
     * @return A CompletableFuture containing list of personal access tokens
     */
    CompletableFuture<List<PersonalAccessTokenInfo>> getPersonalAccessTokens();

    /**
     * Deletes a personal access token asynchronously.
     *
     * @param name The name of the token to delete
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> deletePersonalAccessToken(String name);

    /**
     * Logs in using a personal access token asynchronously.
     *
     * @param token The personal access token
     * @return A CompletableFuture containing identity information
     */
    CompletableFuture<IdentityInfo> loginWithPersonalAccessToken(String token);
}
