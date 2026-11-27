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

import io.netty.channel.IoEventLoopGroup;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.apache.iggy.client.async.tcp.EventLoopShutdown.QUIET_PERIOD_PROPERTY;
import static org.apache.iggy.client.async.tcp.EventLoopShutdown.TIMEOUT_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventLoopShutdownTest {

    private static final UnaryOperator<String> NO_PROPERTIES = name -> null;

    @Test
    void shouldUseTheNettyDefaultsWithoutBuilderValuesOrProperties() {
        var shutdown = EventLoopShutdown.resolve(null, null, NO_PROPERTIES);

        assertThat(shutdown.quietPeriod()).isEqualTo(Duration.ofSeconds(2));
        assertThat(shutdown.timeout()).isEqualTo(Duration.ofSeconds(15));
    }

    @Test
    void shouldTakeUnsetValuesFromTheSystemProperties() {
        var shutdown = EventLoopShutdown.resolve(
                null, null, properties(Map.of(QUIET_PERIOD_PROPERTY, "0", TIMEOUT_PROPERTY, " 250 ")));

        assertThat(shutdown.quietPeriod()).isZero();
        assertThat(shutdown.timeout()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void shouldPreferBuilderValuesOverTheSystemProperties() {
        var shutdown = EventLoopShutdown.resolve(
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                properties(Map.of(QUIET_PERIOD_PROPERTY, "0", TIMEOUT_PROPERTY, "0")));

        assertThat(shutdown.quietPeriod()).isEqualTo(Duration.ofSeconds(1));
        assertThat(shutdown.timeout()).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    void shouldResolveEachValueOnItsOwn() {
        var shutdown =
                EventLoopShutdown.resolve(null, Duration.ofSeconds(1), properties(Map.of(QUIET_PERIOD_PROPERTY, "0")));

        assertThat(shutdown.quietPeriod()).isZero();
        assertThat(shutdown.timeout()).isEqualTo(Duration.ofSeconds(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "2s", "PT2S", "1.5"})
    void shouldRejectAPropertyThatIsNotAWholeNumberOfMilliseconds(String value) {
        assertThatThrownBy(
                        () -> EventLoopShutdown.resolve(null, null, properties(Map.of(QUIET_PERIOD_PROPERTY, value))))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining(QUIET_PERIOD_PROPERTY);
    }

    @Test
    void shouldRejectNegativeBuilderValues() {
        assertThatThrownBy(() -> EventLoopShutdown.resolve(Duration.ofMillis(-1), null, NO_PROPERTIES))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("ShutdownQuietPeriod cannot be negative");
        assertThatThrownBy(() -> EventLoopShutdown.resolve(Duration.ZERO, Duration.ofMillis(-1), NO_PROPERTIES))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("ShutdownTimeout cannot be negative");
    }

    @Test
    void shouldRejectATimeoutShorterThanTheQuietPeriod() {
        assertThatThrownBy(() -> EventLoopShutdown.resolve(Duration.ofSeconds(20), null, NO_PROPERTIES))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("ShutdownTimeout cannot be shorter than ShutdownQuietPeriod");
    }

    @Test
    void shouldRejectAnInvalidSettingWhenTheBuilderBuilds() {
        var builder = AsyncIggyTcpClient.builder().shutdownQuietPeriod(Duration.ofMillis(-1));

        assertThatThrownBy(builder::build)
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("ShutdownQuietPeriod cannot be negative");
    }

    @Test
    void shouldSaturateDurationsTooLongForNetty() throws Exception {
        IoEventLoopGroup group = AsyncTcpConnection.newEventLoopGroup(1);

        var shutdown = new EventLoopShutdown(Duration.ZERO, Duration.ofSeconds(Long.MAX_VALUE));

        shutdown.shutdown(group).get(5, TimeUnit.SECONDS);
        assertThat(group.isTerminated()).isTrue();
    }

    private static UnaryOperator<String> properties(Map<String, String> values) {
        return values::get;
    }
}
