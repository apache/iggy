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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class BigIntegerMessageIdTest {

    @Test
    void defaultIdReturnsInstanceContainingZero() {
        var messageId = BigIntegerMessageId.defaultId();

        assertEquals(BigInteger.ZERO, messageId.toBigInteger());
    }

    @Test
    void defaultIdReturnsSameInstanceContainingZero() {
        var first = BigIntegerMessageId.defaultId();
        var second = BigIntegerMessageId.defaultId();

        assertSame(first, second);
        assertEquals(BigInteger.ZERO, first.toBigInteger());
    }

    @Test
    void toBigIntegerReturnsValueWithinInstance() {
        var value = new BigInteger("123");
        var messageId = new BigIntegerMessageId(value);

        assertEquals(value, messageId.toBigInteger());
    }

    @Test
    void toBytesReturnsValueAsExpected128bitNumber() {
        var messageId = new BigIntegerMessageId(BigInteger.TEN);

        var result = messageId.toBytes();

        assertArrayEquals(new byte[] {10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, result.array());
    }

    @Test
    void toStringReturnsStringRepresentationOfValue() {
        var value = new BigInteger("123456");
        var messageId = new BigIntegerMessageId(value);

        var result = messageId.toString();

        assertEquals("123456", result);
    }
}
