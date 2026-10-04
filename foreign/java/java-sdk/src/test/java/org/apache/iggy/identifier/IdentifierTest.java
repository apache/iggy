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

package org.apache.iggy.identifier;

import io.netty.buffer.ByteBuf;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class IdentifierTest {
    @Test
    void constructorThrowsIggyInvalidArgumentExceptionWhenBothNameAndIdAreProvided() {
        assertThatThrownBy(() -> new FakeIdentifier("foo", 123L)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void getSizeCountsEncodedBytesOfName() {
        assertThat(new FakeIdentifier("世界", null).getSize()).isEqualTo(2 + 6);
    }

    @Test
    void toBytesEncodesNameIdentifierAsKindLengthAndUtf8Bytes() {
        byte[] expected = "世界".getBytes(StandardCharsets.UTF_8);

        ByteBuf result = new FakeIdentifier("世界", null).toBytes();

        assertThat(result.readableBytes()).isEqualTo(2 + expected.length);
        assertThat(result.readByte()).isEqualTo((byte) 2);
        assertThat(result.readUnsignedByte()).isEqualTo((short) expected.length);
        byte[] name = new byte[expected.length];
        result.readBytes(name);
        assertThat(name).isEqualTo(expected);
    }

    @Test
    void toBytesEncodesNumericIdentifierAsKindLengthAndLittleEndianId() {
        ByteBuf result = new FakeIdentifier(null, 7L).toBytes();

        assertThat(result.readableBytes()).isEqualTo(6);
        assertThat(result.readByte()).isEqualTo((byte) 1);
        assertThat(result.readByte()).isEqualTo((byte) 4);
        assertThat(result.readIntLE()).isEqualTo(7);
    }

    @Test
    void constructorAcceptsNameOfExactly255EncodedBytes() {
        String name = "世".repeat(85);
        assertThat(name.getBytes(StandardCharsets.UTF_8)).hasSize(255);
        assertThat(new FakeIdentifier(name, null).getName()).isEqualTo(name);
    }

    @Test
    void constructorThrowsWhenNameExceeds255EncodedBytesEvenIfUnder255Chars() {
        String name = "あ".repeat(200);
        assertThat(name.length()).isLessThan(255);
        assertThatThrownBy(() -> new FakeIdentifier(name, null))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("600");
    }

    static class FakeIdentifier extends Identifier {
        protected FakeIdentifier(@Nullable String name, @Nullable Long id) {
            super(name, id);
        }
    }
}
