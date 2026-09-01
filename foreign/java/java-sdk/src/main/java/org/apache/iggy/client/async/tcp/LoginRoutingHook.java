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
 * Routes a login to the cluster leader before executing its Register
 * operation.
 */
@FunctionalInterface
interface LoginRoutingHook {

    LoginRoutingHook NONE = loginAttempt -> loginAttempt.get();

    /**
     * Discovers and selects the current leader, then executes the supplied
     * login exactly once against the selected connection.
     *
     * @param loginAttempt sends Register against the active connection
     * @return the identity returned by the successful Register response
     */
    CompletableFuture<IdentityInfo> loginOnLeader(Supplier<CompletableFuture<IdentityInfo>> loginAttempt);

    /**
     * Drops any login kept for replay. Called on an explicit sign-out: there
     * is no session left to restore, and a redial must not resurrect one.
     */
    default void forgetLogin() {}
}
