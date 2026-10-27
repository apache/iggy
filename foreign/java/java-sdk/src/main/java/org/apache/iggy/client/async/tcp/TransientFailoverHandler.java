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

import java.util.concurrent.CompletableFuture;

@FunctionalInterface
interface TransientFailoverHandler {
    /**
     * Rechecks the leader after {@code source} lost the link of a call or saw it refused, and returns the
     * connection that carries the next attempt of the call.
     */
    CompletableFuture<AsyncTcpConnection> recheck(
            AsyncTcpConnection source,
            long requestDeadlineNanos,
            Throwable rejection,
            AsyncTcpConnection.TransientFailoverState failoverState);
}
