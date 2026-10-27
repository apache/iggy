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

package org.apache.iggy.system;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyValidationException;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

/** Little-endian unsigned values and canonical Base64url, the encodings of change cursors and page tokens. */
final class MetadataChangeEncoding {

    private MetadataChangeEncoding() {}

    static BigInteger readUnsigned(byte[] bytes, int offset, int width) {
        byte[] bigEndian = new byte[width];
        for (int index = 0; index < width; index++) {
            bigEndian[width - 1 - index] = bytes[offset + index];
        }
        return new BigInteger(1, bigEndian);
    }

    static long readUnsignedInt(byte[] bytes, int offset) {
        return readUnsigned(bytes, offset, Integer.BYTES).longValueExact();
    }

    static void writeUnsigned(byte[] target, int offset, int width, BigInteger value) {
        for (int index = 0; index < width; index++) {
            target[offset + index] = (byte) value.shiftRight(Byte.SIZE * index).intValue();
        }
    }

    /** Checks that {@code value} fits an unsigned integer of {@code bits} bits. */
    static BigInteger requireUnsigned(BigInteger value, int bits, String field) {
        if (value == null || value.signum() < 0 || value.bitLength() > bits) {
            throw new IggyInvalidArgumentException(field + " must be an unsigned " + bits + "-bit value");
        }
        return value;
    }

    static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Decodes {@code text} only when it is the canonical encoding of its bytes: the Base64url alphabet, no
     * padding and zero trailing bits, so one value has exactly one text form.
     */
    static Optional<byte[]> decodeCanonical(String text) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(text);
            return encode(bytes).equals(text) ? Optional.of(bytes) : Optional.empty();
        } catch (IllegalArgumentException malformed) {
            return Optional.empty();
        }
    }

    /** The refusal the server returns for the same query, raised before anything is sent. */
    static IggyValidationException refused(String reason) {
        return new IggyValidationException(
                IggyErrorCode.INVALID_COMMAND,
                IggyErrorCode.INVALID_COMMAND.getCode(),
                reason,
                Optional.empty(),
                Optional.empty());
    }

    static byte[] copy(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length);
    }
}
