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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RetryBackoffTest {
    @Test
    void shouldDoubleTheCeilingFromTheBaseAndHoldItAtTheMaximum() {
        RetryBackoff backoff = new RetryBackoff(() -> 1.0);

        assertThat(pauses(backoff, 8)).containsExactly(50L, 100L, 200L, 400L, 800L, 1000L, 1000L, 1000L);
    }

    @Test
    void shouldTakeTheDrawnFractionOfTheCeiling() {
        assertThat(pauses(new RetryBackoff(() -> 0.5), 6)).containsExactly(25L, 50L, 100L, 200L, 400L, 500L);
        assertThat(pauses(new RetryBackoff(() -> 0.0), 6)).containsOnly(0L);
    }

    @Test
    void shouldStartAtTheBaseInANewCall() {
        pauses(new RetryBackoff(() -> 1.0), 8);

        assertThat(new RetryBackoff(() -> 1.0).nextMillis()).isEqualTo(RetryBackoff.BASE_MILLIS);
    }

    @Test
    void shouldDrawEveryPauseWithinItsCeiling() {
        RetryBackoff backoff = new RetryBackoff();

        List<Long> pauses = pauses(backoff, 64);

        for (int retry = 0; retry < pauses.size(); retry++) {
            assertThat(pauses.get(retry))
                    .isBetween(0L, Math.min(RetryBackoff.MAX_MILLIS, RetryBackoff.BASE_MILLIS << Math.min(retry, 5)));
        }
    }

    private static List<Long> pauses(RetryBackoff backoff, int count) {
        List<Long> pauses = new ArrayList<>();
        for (int retry = 0; retry < count; retry++) {
            pauses.add(backoff.nextMillis());
        }
        return pauses;
    }
}
