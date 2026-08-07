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

package org.apache.iggy.hash;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class XxHash32Test {

    /**
     * Golden vectors for XXH32 with seed 0, matching the canonical xxHash
     * reference implementation and {@code twox_hash::XxHash32::oneshot(0, ..)}
     * used by the Rust SDK for message-key partitioning.
     */
    @ParameterizedTest
    @CsvSource({
        "'', 02CC5D05",
        "a, 550D7456",
        "abc, 32D153FF",
        "message digest, 7C948494",
        "abcdefghijklmnopqrstuvwxyz, 63A14D5F",
        "user-123, 0CC9EC8C",
        "order-key, 43A01006",
    })
    void shouldMatchReferenceVectors(String input, String expectedHex) {
        long hash = XxHash32.hashUnsigned(input.getBytes(StandardCharsets.UTF_8));

        assertThat(hash).isEqualTo(Long.parseLong(expectedHex, 16));
    }

    @Test
    void shouldHashAllByteValuesAcrossEveryLoopShape() {
        // 256 bytes exercises the 16-byte stripes, the 4-byte tail chunks and
        // the single-byte tail in one input
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        assertThat(XxHash32.hashUnsigned(data)).isEqualTo(0x59441253L);
    }

    @Test
    void shouldReturnUnsignedValueForHashesAboveIntegerMax() {
        // "Nobody inspects the spammish repetition" hashes to 0xE2293B2F,
        // which is negative as a signed int
        long hash = XxHash32.hashUnsigned("Nobody inspects the spammish repetition".getBytes(StandardCharsets.UTF_8));

        assertThat(hash).isEqualTo(0xE2293B2FL);
    }
}
