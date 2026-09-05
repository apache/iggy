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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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

    /** Bound on a u8-length-prefixed field, in encoded bytes. */
    static final int MAX_SHORT_FIELD_LENGTH = 255;

    private static final String UNKNOWN_SDK_VERSION = "unknown";

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
        writeShortField(body, sdkVersionField(IggyVersion.getInstance().getVersion()));
    }

    /**
     * An over-long version is cut on the encoded bytes at a code point boundary, so the field
     * always fits its u8 prefix and still decodes as UTF-8 on the server.
     */
    static byte[] sdkVersionField(String version) {
        String value = version == null || version.isEmpty() ? UNKNOWN_SDK_VERSION : version;
        ByteBuffer encoded = ByteBuffer.allocate(MAX_SHORT_FIELD_LENGTH);
        StandardCharsets.UTF_8
                .newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .encode(CharBuffer.wrap(value), encoded, true);
        return Arrays.copyOf(encoded.array(), encoded.position());
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
        if (value.length == 0 || value.length > MAX_SHORT_FIELD_LENGTH) {
            throw new IggyInvalidArgumentException(
                    "Wire name fields must be 1.." + MAX_SHORT_FIELD_LENGTH + " bytes, got " + value.length);
        }
        out.writeByte(value.length);
        out.writeBytes(value);
    }
}
