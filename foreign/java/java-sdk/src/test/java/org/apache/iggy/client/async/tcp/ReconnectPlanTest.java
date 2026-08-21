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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconnectPlanTest {

    private final ConnectionInfo seed = new ConnectionInfo("seed-node", 8090);
    private final ConnectionInfo current = new ConnectionInfo("leader-node", 8090);
    private final ConnectionInfo survivor = new ConnectionInfo("survivor-node", 8090);

    @Test
    void shouldRotateThroughEveryKnownEndpoint() {
        var candidates = List.of(current, seed, survivor);

        assertThat(ReconnectPlan.target(candidates, 1)).isEqualTo(current);
        assertThat(ReconnectPlan.target(candidates, 2)).isEqualTo(seed);
        assertThat(ReconnectPlan.target(candidates, 3)).isEqualTo(survivor);
        assertThat(ReconnectPlan.target(candidates, 4)).isEqualTo(current);
    }

    @Test
    void shouldDialOnlyOneAddressWhenNeverRedirected() {
        assertThat(ReconnectPlan.target(List.of(seed), 1)).isEqualTo(seed);
        assertThat(ReconnectPlan.target(List.of(seed), 2)).isEqualTo(seed);
    }

    @Test
    void shouldRefuseToPlanARedialWithoutCandidates() {
        assertThatThrownBy(() -> ReconnectPlan.target(List.of(), 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldKeepFixedDelayConstant() {
        var policy = RetryPolicy.fixedDelay(12, Duration.ofSeconds(5));

        assertThat(ReconnectPlan.delay(policy, 1)).isEqualTo(Duration.ofSeconds(5));
        assertThat(ReconnectPlan.delay(policy, 12)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void shouldScaleExponentialDelayUpToTheCap() {
        var policy = RetryPolicy.exponentialBackoff(5, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0);

        assertThat(ReconnectPlan.delay(policy, 1)).isEqualTo(Duration.ofMillis(100));
        assertThat(ReconnectPlan.delay(policy, 2)).isEqualTo(Duration.ofMillis(200));
        assertThat(ReconnectPlan.delay(policy, 3)).isEqualTo(Duration.ofMillis(400));
        assertThat(ReconnectPlan.delay(policy, 5)).isEqualTo(Duration.ofSeconds(1));
    }
}
