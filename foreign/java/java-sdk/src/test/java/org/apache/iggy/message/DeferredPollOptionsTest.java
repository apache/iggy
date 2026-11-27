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
import java.time.temporal.ChronoUnit;

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
    void shouldNeedRoomForOneMessageInTheByteLimit() {
        // The server refuses a limit below its poll prefix, one batch header and one message header.
        assertThat(DeferredPollOptions.defaults().withMaxBytes(344).maxBytes()).isEqualTo(344);
        assertThatThrownBy(() -> DeferredPollOptions.defaults().withMaxBytes(343))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("must be between 344");
    }

    @Test
    void shouldAcceptABudgetThatCoversTheWaitAndExactlyTheReplyMargin() {
        var exact = new DeferredPollOptions(Duration.ofSeconds(10), 1, 1024, Duration.ofSeconds(11));

        assertThat(exact.requestTimeout().minus(exact.maxWait())).isEqualTo(DeferredPollOptions.REPLY_MARGIN);
    }

    @Test
    void shouldRejectABudgetOneMicrosecondShortOfTheReplyMargin() {
        Duration oneMicroShort = Duration.ofSeconds(11).minus(1, ChronoUnit.MICROS);

        assertThatThrownBy(() -> new DeferredPollOptions(Duration.ofSeconds(10), 1, 1024, oneMicroShort))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("reply margin");
        assertThatThrownBy(() -> new DeferredPollOptions(Duration.ofSeconds(11), 1, 1024, Duration.ofSeconds(11)))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("reply margin");
    }

    @Test
    void shouldNeedNoReplyMarginForAZeroWait() {
        var immediate = new DeferredPollOptions(Duration.ZERO, 1, 1024, Duration.of(1, ChronoUnit.MICROS));

        assertThat(immediate.requestTimeoutMicros()).isEqualTo(1);
    }

    @Test
    void shouldNeedTheRequestTimeoutRaisedBeforeAWaitAboveTheDefaultMargin() {
        assertThatThrownBy(() -> DeferredPollOptions.defaults().withMaxWait(Duration.ofSeconds(20)))
                .isInstanceOf(IggyInvalidArgumentException.class);

        var raised = DeferredPollOptions.defaults()
                .withRequestTimeout(Duration.ofSeconds(21))
                .withMaxWait(Duration.ofSeconds(20));

        assertThat(raised.maxWait()).isEqualTo(Duration.ofSeconds(20));
    }

    @Test
    void shouldShrinkBothBudgetsByTheTimeAlreadySpent() {
        var options = DeferredPollOptions.defaults().withMinCount(3).withMaxBytes(2048);

        var remaining = options.remaining(Duration.ofMillis(400)).orElseThrow();

        assertThat(remaining.maxWait()).isEqualTo(Duration.ofMillis(600));
        assertThat(remaining.requestTimeout()).isEqualTo(Duration.ofMillis(10_600));
        assertThat(remaining.minCount()).isEqualTo(3);
        assertThat(remaining.maxBytes()).isEqualTo(2048);
    }

    @Test
    void shouldStopTheWaitAtZeroAndCountAPartialMicrosecondAsSpent() {
        var remaining = DeferredPollOptions.defaults()
                .remaining(Duration.ofSeconds(2).plusNanos(1))
                .orElseThrow();

        assertThat(remaining.maxWait()).isZero();
        assertThat(remaining.requestTimeoutMicros()).isEqualTo(8_999_999);
    }

    @Test
    void shouldReportASpentRequestBudget() {
        var options = DeferredPollOptions.defaults();

        assertThat(options.remaining(options.requestTimeout())).isEmpty();
        assertThat(options.remaining(options.requestTimeout().minusNanos(1)))
                .as("the last partial microsecond counts as spent")
                .isEmpty();
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
