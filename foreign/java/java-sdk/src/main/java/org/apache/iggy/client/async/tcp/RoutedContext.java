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

import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;

/**
 * The partition context in the header of a request that the router sends on a route. The server refuses a poll, an
 * offset write, or a lookup whose header names incarnation 0, so the client fills it from the route.
 */
final class RoutedContext {
    private static final int FETCH = CommandCode.Messages.FETCH_MESSAGES.getValue();

    private RoutedContext() {}

    /**
     * The context that a request of {@code command} sends on a route with context {@code route}. A fetch names the
     * history of each partition in its targets, so it keeps the context of the caller for them, and the encoder names
     * only the session watermark in its header. Any other request without a caller context takes the context of the
     * route. So does a context that is all zero, since its op 0 would bind owner 0 to no owner that the server
     * installed. Any other caller context with incarnation 0 takes the incarnation of the route and keeps its owner
     * and op. The router keeps the result for every attempt, so they all name the same history.
     */
    static PartitionContext of(int command, PartitionContext caller, PartitionContext route) {
        if (caller == null) {
            return route;
        }
        if (command == FETCH || caller.incarnation().signum() != 0) {
            return caller;
        }
        return caller.equals(PartitionContext.EMPTY)
                ? route
                : new PartitionContext(route.incarnation(), caller.ownerGeneration(), caller.metadataOp());
    }
}
