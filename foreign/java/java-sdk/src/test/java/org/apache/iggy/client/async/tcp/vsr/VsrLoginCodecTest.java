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

package org.apache.iggy.client.async.tcp.vsr;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.serde.BytesSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrLoginCodecTest {

    @Test
    void sdkVersionFieldEncodesVersionAsUtf8() {
        assertThat(VsrLoginCodec.sdkVersionField("0.9.0-SNAPSHOT"))
                .isEqualTo("0.9.0-SNAPSHOT".getBytes(StandardCharsets.UTF_8));
    }

    @ParameterizedTest
    @NullSource
    @EmptySource
    void sdkVersionFieldFallsBackToUnknownWhenVersionIsMissing(String version) {
        assertThat(VsrLoginCodec.sdkVersionField(version)).isEqualTo("unknown".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sdkVersionFieldTruncatesOnCodePointBoundaryWithinU8Prefix() {
        String version = "é".repeat(300);

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(254);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo("é".repeat(127));
    }

    @Test
    void sdkVersionFieldKeepsSurrogatePairThatEndsExactlyAtU8Prefix() {
        String version = "a".repeat(251) + "\uD83D\uDE00";
        assertThat(version.getBytes(StandardCharsets.UTF_8)).hasSize(255);

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(255);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo(version);
    }

    @Test
    void sdkVersionFieldDropsWholeSurrogatePairThatWouldStraddleU8Prefix() {
        String version = "a".repeat(252) + "\uD83D\uDE00";

        byte[] field = VsrLoginCodec.sdkVersionField(version);

        assertThat(field).hasSize(252);
        assertThat(new String(field, StandardCharsets.UTF_8)).isEqualTo("a".repeat(252));
    }

    @Test
    void rewriteUserLoginPrefixesCredentialsWithUtf8ByteLength() {
        String username = "użytkownik";
        String password = "hasło";
        ByteBuf loginPayload = BytesSerializer.toBytes(username, "username");
        loginPayload.writeBytes(BytesSerializer.toBytes(password, "password"));

        ByteBuf body = VsrLoginCodec.rewriteUserLogin(UnpooledByteBufAllocator.DEFAULT, loginPayload);
        try {
            assertThat(body.readIntLE()).isEqualTo(VsrLoginCodec.PROTOCOL_VERSION);
            assertThat(readShortField(body)).isEqualTo(VsrLoginCodec.SDK_NAME);
            assertThat(readShortField(body)).isNotEmpty();
            assertThat(readShortField(body)).isEqualTo(username);
            assertThat(readShortField(body)).isEqualTo(password);
            assertThat(body.readIntLE()).isZero();
            assertThat(body.isReadable()).isFalse();
        } finally {
            body.release();
            loginPayload.release();
        }
    }

    @Test
    void rewriteUserLoginRejectsEmptyUsernameBeforeAllocating() {
        ByteBuf loginPayload = Unpooled.buffer();
        loginPayload.writeByte(0);
        loginPayload.writeBytes(BytesSerializer.toBytes("secret", "password"));
        CountingAllocator alloc = new CountingAllocator();

        assertThatThrownBy(() -> VsrLoginCodec.rewriteUserLogin(alloc, loginPayload))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("username");
        assertThat(alloc.allocations).isZero();
        loginPayload.release();
    }

    @Test
    void rewritePatLoginRejectsEmptyTokenBeforeAllocating() {
        ByteBuf loginPayload = Unpooled.buffer();
        loginPayload.writeByte(0);
        CountingAllocator alloc = new CountingAllocator();

        assertThatThrownBy(() -> VsrLoginCodec.rewritePatLogin(alloc, loginPayload))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageContaining("token");
        assertThat(alloc.allocations).isZero();
        loginPayload.release();
    }

    private static String readShortField(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readUnsignedByte()];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class CountingAllocator extends AbstractByteBufAllocator {
        private int allocations;

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            allocations++;
            return Unpooled.buffer(initialCapacity, maxCapacity);
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            allocations++;
            return Unpooled.directBuffer(initialCapacity, maxCapacity);
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }
    }
}
