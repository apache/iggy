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

package org.apache.iggy.client.async.tcp;

import org.apache.iggy.user.IdentityInfo;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Runs after a successful login to redirect the client to the cluster
 * leader when it is connected to a follower.
 */
@FunctionalInterface
interface LoginRedirectionHook {

    LoginRedirectionHook NONE = reLogin -> CompletableFuture.completedFuture(null);

    /**
     * Checks the cluster roster and, while the current node is not the
     * leader, retargets the connection and re-runs the login.
     *
     * @param reLogin replays the just-completed login against the current
     * target; it must not trigger another redirection check itself, since
     * the hook drives any further hops and serializes concurrent checks
     * @return the redirected login's identity, or {@code null} when the client
     * stays on the current node
     */
    CompletableFuture<IdentityInfo> afterLogin(Supplier<CompletableFuture<IdentityInfo>> reLogin);
}
