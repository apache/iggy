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

import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.config.RetryPolicy;

import java.time.Duration;

/**
 * Pure redial planning: which address to dial on a given reconnect attempt
 * and how long to wait before it.
 */
final class ReconnectPlan {

    private ReconnectPlan() {}

    /**
     * Alternates reconnect dials between the current endpoint and the
     * configured seed. After a leader redirect the current endpoint may die
     * with the leader, and the seed is the way back to the rest of the
     * cluster. Attempts are 1-based; odd attempts dial the current endpoint.
     */
    static ConnectionInfo target(ConnectionInfo current, ConnectionInfo seed, int attempt) {
        if (current.equals(seed)) {
            return current;
        }
        return attempt % 2 == 1 ? current : seed;
    }

    /**
     * The delay before the given 1-based attempt: the policy's initial delay
     * scaled by its multiplier per prior attempt, capped at its max delay.
     */
    static Duration delay(RetryPolicy policy, int attempt) {
        double scaled = policy.getInitialDelay().toMillis() * Math.pow(policy.getMultiplier(), attempt - 1L);
        long millis = (long) Math.min(scaled, policy.getMaxDelay().toMillis());
        return Duration.ofMillis(millis);
    }
}
