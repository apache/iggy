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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.HexFormat;

import static org.apache.iggy.serde.BytesDeserializer.readU64AsBigInteger;
import static org.assertj.core.api.Assertions.assertThat;

class BytesDeserializerTest {

    @Nested
    class U64 {

        @Test
        void shouldDeserializeMaxValue() {
            // given
            long maxLong = 0xFFFF_FFFF_FFFF_FFFFL;
            ByteBuf buffer = Unpooled.copyLong(maxLong);
            var expectedMaxU64 = new BigInteger(Long.toUnsignedString(maxLong));

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(expectedMaxU64);
        }

        @Test
        void shouldDeserializeZero() {
            // given
            ByteBuf buffer = Unpooled.buffer(8);
            buffer.writeZero(8);

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(BigInteger.ZERO);
        }

        @Test
        void shouldDeserializeArbitraryValue() {
            // given
            byte[] bytes = HexFormat.of().parseHex("8000000000000000");
            var expected = new BigInteger(1, bytes);
            ArrayUtils.reverse(bytes);
            ByteBuf buffer = Unpooled.wrappedBuffer(bytes);

            // when
            BigInteger result = readU64AsBigInteger(buffer);

            // then
            assertThat(result).isEqualTo(expected);
        }
    }
}
