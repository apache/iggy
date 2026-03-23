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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class UuidMessageIdTest {

    @ParameterizedTest
    @CsvSource({
        "00000000-0000-0000-0000-000000000000, 0",
        "ffffffff-ffff-ffff-ffff-ffffffffffff, 340282366920938463463374607431768211455" // 2^128-1
    })
    void toBigIntegerReturnsBigIntegerRepresentation(UUID id, BigInteger expected) {
        var messageId = new UuidMessageId(id);

        var result = messageId.toBigInteger();

        assertThat(result).isEqualTo(expected);
    }

    public static Stream<Arguments> toBytesArgumentProvider() {
        return Stream.of(
                Arguments.of(
                        UUID.fromString("00000000-0000-0000-0000-000000000000"),
                        new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                Arguments.of(
                        UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                        new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1}));
    }

    @ParameterizedTest
    @MethodSource("toBytesArgumentProvider")
    void toBytesReturnsExpectedBytes(UUID id, byte[] expected) {
        var messageId = new UuidMessageId(id);

        var result = messageId.toBytes();

        assertThat(result.array()).isEqualTo(expected);
    }

    @Test
    void toStringReturnsUUIDStringRepresentation() {
        var id = "c0d43e67-54d0-4671-8715-dfb3832cd367";
        var messageId = new UuidMessageId(UUID.fromString(id));

        assertThat(messageId.toString()).isEqualTo(id);
    }
}
