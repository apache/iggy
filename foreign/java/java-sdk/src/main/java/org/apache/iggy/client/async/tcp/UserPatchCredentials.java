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
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.user.UserOptionKey;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

final class UserPatchCredentials {

    private static final byte[] USERNAME_KEY = UserOptionKey.USERNAME.value().getBytes(StandardCharsets.UTF_8);

    private UserPatchCredentials() {}

    static Optional<String> patchedUsername(ByteBuf patch) {
        if (!skipResetKeys(patch)) {
            return Optional.empty();
        }
        while (patch.readableBytes() >= 2 * (1 + Integer.BYTES)) {
            patch.skipBytes(1);
            Optional<byte[]> key = readOptionField(patch);
            if (key.isEmpty() || !patch.isReadable()) {
                return Optional.empty();
            }
            int valueKind = patch.readUnsignedByte();
            Optional<byte[]> value = readOptionField(patch);
            if (value.isEmpty()) {
                return Optional.empty();
            }
            if (Arrays.equals(key.get(), USERNAME_KEY) && valueKind == HeaderKind.String.asCode()) {
                return Optional.of(new String(value.get(), StandardCharsets.UTF_8));
            }
        }
        return Optional.empty();
    }

    private static boolean skipResetKeys(ByteBuf patch) {
        if (!patch.isReadable()) {
            return false;
        }
        int resetCount = patch.readUnsignedByte();
        for (int index = 0; index < resetCount; index++) {
            if (!patch.isReadable()) {
                return false;
            }
            int length = patch.readUnsignedByte();
            if (patch.readableBytes() < length) {
                return false;
            }
            patch.skipBytes(length);
        }
        return true;
    }

    private static Optional<byte[]> readOptionField(ByteBuf body) {
        if (body.readableBytes() < Integer.BYTES) {
            return Optional.empty();
        }
        long length = body.readUnsignedIntLE();
        if (length > body.readableBytes()) {
            return Optional.empty();
        }
        byte[] field = new byte[(int) length];
        body.readBytes(field);
        return Optional.of(field);
    }
}
