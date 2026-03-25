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

package org.apache.iggy.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RetryPolicyTest {
    @Test
    void exponentialBackoffWithNoArgsReturnsExpectedRetryPolicy() {
        var retryPolicy = RetryPolicy.exponentialBackoff();

        assertThat(retryPolicy.getMaxRetries()).isEqualTo(3);
        assertThat(retryPolicy.getInitialDelay()).isEqualTo(Duration.ofMillis(100));
        assertThat(retryPolicy.getMaxDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(retryPolicy.getMultiplier()).isEqualTo(2.0);
    }

    @Test
    void exponentialBackoffWithAllArgsReturnsExpectedRetryPolicy() {
        var retryPolicy = RetryPolicy.exponentialBackoff(30, Duration.ofMillis(50), Duration.ofMinutes(60), 1.5);

        assertThat(retryPolicy.getMaxRetries()).isEqualTo(30);
        assertThat(retryPolicy.getInitialDelay()).isEqualTo(Duration.ofMillis(50));
        assertThat(retryPolicy.getMaxDelay()).isEqualTo(Duration.ofMinutes(60));
        assertThat(retryPolicy.getMultiplier()).isEqualTo(1.5);
    }

    @Test
    void fixedDelayReturnsExpectedRetryPolicy() {
        var retryPolicy = RetryPolicy.fixedDelay(50, Duration.ofMillis(500));

        assertThat(retryPolicy.getMaxRetries()).isEqualTo(50);
        assertThat(retryPolicy.getInitialDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(retryPolicy.getMaxDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(retryPolicy.getMultiplier()).isEqualTo(1.0);
    }

    @Test
    void noRetryReturnsExpectedRetryPolicy() {
        var retryPolicy = RetryPolicy.noRetry();

        assertThat(retryPolicy.getMaxRetries()).isEqualTo(0);
        assertThat(retryPolicy.getInitialDelay()).isEqualTo(Duration.ZERO);
        assertThat(retryPolicy.getMaxDelay()).isEqualTo(Duration.ZERO);
        assertThat(retryPolicy.getMultiplier()).isEqualTo(1.0);
    }

    @Test
    void getMaxRetriesReturnsMaxRetries() {
        var retryPolicy = RetryPolicy.exponentialBackoff(2, Duration.ofMillis(200), Duration.ofMillis(1000), 1.0);

        assertThat(retryPolicy.getMaxRetries()).isEqualTo(2);
    }

    @Test
    void getInitialDelayReturnsInitialDelay() {
        var retryPolicy = RetryPolicy.exponentialBackoff(2, Duration.ofMillis(200), Duration.ofMillis(1000), 1.0);

        assertThat(retryPolicy.getInitialDelay()).isEqualTo(Duration.ofMillis(200));
    }

    @Test
    void getMaxDelayReturnsMaxDelay() {
        var retryPolicy = RetryPolicy.exponentialBackoff(2, Duration.ofMillis(200), Duration.ofMillis(1000), 1.0);

        assertThat(retryPolicy.getMaxDelay()).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void getMultiplierReturnsMultiplier() {
        var retryPolicy = RetryPolicy.exponentialBackoff(2, Duration.ofMillis(200), Duration.ofMillis(1000), 1.0);

        assertThat(retryPolicy.getMultiplier()).isEqualTo(1.0);
    }
}
