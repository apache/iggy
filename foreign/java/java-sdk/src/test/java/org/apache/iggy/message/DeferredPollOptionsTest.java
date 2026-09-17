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

package org.apache.iggy.message;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeferredPollOptionsTest {

    @Test
    void shouldMatchTheServerDefaults() {
        var defaults = DeferredPollOptions.defaults();

        assertThat(defaults.maxWait()).isEqualTo(Duration.ofSeconds(1));
        assertThat(defaults.minCount()).isEqualTo(1);
        assertThat(defaults.maxBytes()).isEqualTo(1024 * 1024);
        assertThat(defaults.requestTimeout()).isEqualTo(Duration.ofSeconds(11));
        assertThat(defaults.maxWaitMicros()).isEqualTo(1_000_000);
        assertThat(defaults.requestTimeoutMicros()).isEqualTo(11_000_000);
    }

    @Test
    void shouldAcceptTheProtocolBoundaries() {
        var atLimit = new DeferredPollOptions(
                DeferredPollOptions.MAX_WAIT,
                0xFFFF_FFFFL,
                DeferredPollOptions.MIN_MAX_BYTES,
                DeferredPollOptions.MAX_REQUEST_TIMEOUT);

        assertThat(atLimit.maxWaitMicros()).isEqualTo(600_000_000L);
        assertThat(atLimit.requestTimeoutMicros()).isEqualTo(630_000_000L);
    }

    @Test
    void shouldAcceptAZeroWaitAsAnImmediateBoundedRead() {
        var immediate = DeferredPollOptions.defaults().withMaxWait(Duration.ZERO);

        assertThat(immediate.maxWaitMicros()).isZero();
    }

    @Test
    void shouldRejectLimitsTheServerWouldRefuse() {
        var defaults = DeferredPollOptions.defaults();

        assertThatThrownBy(() -> defaults.withMaxWait(DeferredPollOptions.MAX_WAIT.plusMillis(1)))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withMaxWait(Duration.ofSeconds(-1)))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withRequestTimeout(Duration.ZERO))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withRequestTimeout(DeferredPollOptions.MAX_REQUEST_TIMEOUT.plusMillis(1)))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withMinCount(0)).isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withMinCount(0x1_0000_0000L))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withMaxBytes(DeferredPollOptions.MIN_MAX_BYTES - 1))
                .isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> defaults.withMaxBytes(0x1_0000_0000L))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldRejectAWaitLongerThanTheTotalBudget() {
        assertThatThrownBy(() -> new DeferredPollOptions(Duration.ofSeconds(12), 1, 1024, Duration.ofSeconds(11)))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("shorter than maxWait");
    }

    @Test
    void shouldAcceptAWaitEqualToTheTotalBudget() {
        var equal = new DeferredPollOptions(Duration.ofSeconds(11), 1, 1024, Duration.ofSeconds(11));

        assertThat(equal.maxWaitMicros()).isEqualTo(equal.requestTimeoutMicros());
    }

    @Test
    void shouldRejectFractionalMicrosecondsInsteadOfTruncatingThem() {
        assertThatThrownBy(() -> DeferredPollOptions.defaults().withMaxWait(Duration.ofNanos(1500)))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("whole number of microseconds");
        assertThatThrownBy(() -> DeferredPollOptions.defaults().withRequestTimeout(Duration.ofNanos(11_000_000_001L)))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("whole number of microseconds");
    }

    @Test
    void shouldRejectAReadinessTargetTheCountCannotMeet() {
        var options = DeferredPollOptions.defaults().withMinCount(10);

        options.validate(10);

        assertThatThrownBy(() -> options.validate(9))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("exceeds the requested count");
        assertThatThrownBy(() -> options.validate(0)).isInstanceOf(IggyInvalidArgumentException.class);
        assertThatThrownBy(() -> options.validate(0x1_0000_0000L)).isInstanceOf(IggyInvalidArgumentException.class);
    }
}
