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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.iggy.IggyVersion;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.nio.charset.StandardCharsets;

/**
 * Rewrites serialized login payloads into the
 * {@code LoginRegister} / {@code LoginRegisterWithPat} bodies, mirroring
 * {@code core/binary_protocol/src/requests/users/login_register.rs} and
 * {@code login_register_with_pat.rs}. Both bodies start with a
 * {@code ClientVersionInfo} prefix carrying the packed protocol version and
 * the SDK identity ({@code core/binary_protocol/src/version.rs}).
 */
final class VsrLoginCodec {

    /**
     * Packed semver of the {@code iggy_binary_protocol} crate this codec
     * targets: {@code major << 20 | minor << 10 | patch}, 10 bits per field.
     * Keep in sync with {@code core/binary_protocol/Cargo.toml}; the server
     * accepts any client whose major.minor is not newer than its own.
     */
    static final int PROTOCOL_VERSION = (11 << 10); // 0.11.0

    static final String SDK_NAME = "java-sdk";

    private VsrLoginCodec() {}

    /**
     * {@code LoginUser} (code 38) payload in:
     * {@code [username:u8-len][password:u8-len][version:u32-len][context:u32-len]}.
     * The trailing version/context strings are superseded by the
     * {@code ClientVersionInfo} prefix and dropped.
     */
    static ByteBuf rewriteUserLogin(ByteBufAllocator alloc, ByteBuf loginPayload) {
        ByteBuf in = loginPayload.slice();
        byte[] username = readShortField(in, "username");
        byte[] password = readShortField(in, "password");

        ByteBuf body = alloc.buffer();
        writeVersionInfo(body);
        writeShortField(body, username);
        body.writeByte(password.length);
        body.writeBytes(password);
        body.writeIntLE(0);
        return body;
    }

    /**
     * {@code LoginWithPersonalAccessToken} (code 44) payload in:
     * {@code [token:u8-len]}.
     */
    static ByteBuf rewritePatLogin(ByteBufAllocator alloc, ByteBuf loginPayload) {
        ByteBuf in = loginPayload.slice();
        byte[] token = readShortField(in, "token");

        ByteBuf body = alloc.buffer();
        writeVersionInfo(body);
        writeShortField(body, token);
        body.writeIntLE(0);
        return body;
    }

    /**
     * Register reply body after result-section stripping:
     * {@code [user_id:u32][session:u64][server_protocol_version:u32][server_version:u8-len]}.
     */
    static long readSessionEpoch(ByteBuf registerBody) {
        return registerBody.getLongLE(registerBody.readerIndex() + 4);
    }

    private static void writeVersionInfo(ByteBuf body) {
        body.writeIntLE(PROTOCOL_VERSION);
        writeShortField(body, SDK_NAME.getBytes(StandardCharsets.UTF_8));
        writeShortField(body, sdkVersion().getBytes(StandardCharsets.UTF_8));
    }

    private static String sdkVersion() {
        String version = IggyVersion.getInstance().getVersion();
        if (version == null || version.isEmpty()) {
            return "unknown";
        }
        return version.length() > 255 ? version.substring(0, 255) : version;
    }

    private static byte[] readShortField(ByteBuf in, String field) {
        if (!in.isReadable()) {
            throw new IggyInvalidArgumentException("Login payload is missing the " + field + " field");
        }
        int length = in.readUnsignedByte();
        if (in.readableBytes() < length) {
            throw new IggyInvalidArgumentException("Login payload " + field + " field is truncated");
        }
        byte[] value = new byte[length];
        in.readBytes(value);
        return value;
    }

    private static void writeShortField(ByteBuf out, byte[] value) {
        if (value.length == 0 || value.length > 255) {
            throw new IggyInvalidArgumentException("Wire name fields must be 1..255 bytes, got " + value.length);
        }
        out.writeByte(value.length);
        out.writeBytes(value);
    }
}
