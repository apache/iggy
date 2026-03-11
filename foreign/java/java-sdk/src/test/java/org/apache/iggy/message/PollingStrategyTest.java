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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PollingStrategyTest {
    @Test
    void offsetReturnsStrategyWithOffsetKindAndProvidedValue() {
        var strategy = PollingStrategy.offset(BigInteger.ONE);

        assertEquals(BigInteger.ONE, strategy.value());
        assertEquals(PollingKind.Offset, strategy.kind());
    }

    @Test
    void timestampReturnsStrategyWithTimestampKindAndProvidedValue() {
        var strategy = PollingStrategy.timestamp(new BigInteger("123456789"));

        assertEquals(new BigInteger("123456789"), strategy.value());
        assertEquals(PollingKind.Timestamp, strategy.kind());
    }

    @Test
    void firstReturnsStrategyWithFirstKindAndZeroAsValue() {
        var strategy = PollingStrategy.first();

        assertEquals(BigInteger.ZERO, strategy.value());
        assertEquals(PollingKind.First, strategy.kind());
    }

    @Test
    void lastReturnsStrategyWithLastKindAndZeroAsValue() {
        var strategy = PollingStrategy.last();

        assertEquals(BigInteger.ZERO, strategy.value());
        assertEquals(PollingKind.Last, strategy.kind());
    }

    @Test
    void nextReturnsStrategyWithNextKindAndZeroAsValue() {
        var strategy = PollingStrategy.next();

        assertEquals(BigInteger.ZERO, strategy.value());
        assertEquals(PollingKind.Next, strategy.kind());
    }
}
