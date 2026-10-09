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

import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PollingStrategyTest {
    @Test
    void offsetReturnsStrategyWithOffsetKindAndProvidedValue() {
        var strategy = PollingStrategy.offset(BigInteger.ONE);

        assertThat(strategy.value()).isEqualTo(BigInteger.ONE);
        assertThat(strategy.kind()).isEqualTo(PollingKind.Offset);
    }

    @Test
    void timestampReturnsStrategyWithTimestampKindAndProvidedValue() {
        var strategy = PollingStrategy.timestamp(new BigInteger("123456789"));

        assertThat(strategy.value()).isEqualTo(new BigInteger("123456789"));
        assertThat(strategy.kind()).isEqualTo(PollingKind.Timestamp);
    }

    @Test
    void firstReturnsStrategyWithFirstKindAndZeroAsValue() {
        var strategy = PollingStrategy.first();

        assertThat(strategy.value()).isEqualTo(BigInteger.ZERO);
        assertThat(strategy.kind()).isEqualTo(PollingKind.First);
    }

    @Test
    void lastReturnsStrategyWithLastKindAndZeroAsValue() {
        var strategy = PollingStrategy.last();

        assertThat(strategy.value()).isEqualTo(BigInteger.ZERO);
        assertThat(strategy.kind()).isEqualTo(PollingKind.Last);
    }

    @Test
    void nextReturnsStrategyWithNextKindAndZeroAsValue() {
        var strategy = PollingStrategy.next();

        assertThat(strategy.value()).isEqualTo(BigInteger.ZERO);
        assertThat(strategy.kind()).isEqualTo(PollingKind.Next);
    }

    @Test
    void factoriesAndConstructorReturnStrategiesWithoutContext() {
        var strategies = List.of(
                PollingStrategy.offset(BigInteger.ONE),
                PollingStrategy.timestamp(BigInteger.TWO),
                PollingStrategy.first(),
                PollingStrategy.last(),
                PollingStrategy.next(),
                new PollingStrategy(PollingKind.Offset, BigInteger.TEN));

        assertThat(strategies)
                .allSatisfy(strategy -> assertThat(strategy.context()).isEmpty());
    }

    @Test
    void withContextReturnsCopyWithContext() {
        var context = new PartitionContext(BigInteger.valueOf(7), BigInteger.valueOf(8), BigInteger.valueOf(9));
        var strategy = PollingStrategy.offset(BigInteger.TEN);

        var continued = strategy.withContext(context);

        assertThat(continued.kind()).isEqualTo(PollingKind.Offset);
        assertThat(continued.value()).isEqualTo(BigInteger.TEN);
        assertThat(continued.context()).contains(context);
        assertThat(strategy.context()).isEmpty();
    }

    @Test
    void constructorRejectsNullContext() {
        assertThatThrownBy(() -> new PollingStrategy(PollingKind.Offset, BigInteger.ONE, null))
                .isInstanceOf(NullPointerException.class);
    }
}
