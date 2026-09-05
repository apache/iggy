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

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * LoginUser wire format:
 * [username_len:u8][username:N][password_len:u8][password:N]
 * [version_len:u32_le][version:N][context_len:u32_le][context:N]
 */
class UsersTcpClientPayloadTest {

    @Test
    void shouldPrefixEveryFieldWithItsUtf8ByteLength() {
        String username = "użytkownik";
        String password = "hasło";
        String version = "iggy-java-sdk/0.9.0";
        String context = "build: 2026-09-05, commit: ąęó";

        ByteBuf payload = UsersTcpClient.loginPayload(username, password, version, context);

        assertThat(readU8String(payload)).isEqualTo(username);
        assertThat(readU8String(payload)).isEqualTo(password);
        assertThat(readU32String(payload)).isEqualTo(version);
        assertThat(readU32String(payload)).isEqualTo(context);
        assertThat(payload.isReadable()).isFalse();
    }

    private static String readU8String(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readUnsignedByte()];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String readU32String(ByteBuf buffer) {
        byte[] bytes = new byte[Math.toIntExact(buffer.readUnsignedIntLE())];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
