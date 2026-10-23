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

package org.apache.iggy.stream;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Checks and fingerprints a stream's own data encryption key.
 *
 * <p>A stream key is 32 random bytes. Never derive one from a password: anyone who can read the stream sees
 * its fingerprint, which would then become an offline guessing oracle. The server never returns the key,
 * only its fingerprint, so comparing {@link #fingerprint(byte[])} with
 * {@link StreamDetails#encryptionKeyFingerprint()} tells which key a stream uses.
 */
public final class EncryptionKeys {

    /** Length of a stream key in bytes. */
    public static final int KEY_LENGTH = 32;

    private static final byte[] FINGERPRINT_LABEL = "iggy/stream-key-fingerprint/v1".getBytes(StandardCharsets.UTF_8);
    private static final int FINGERPRINT_BYTES = 8;

    private EncryptionKeys() {}

    /**
     * The key's fingerprint: the first 8 bytes of {@code SHA-256("iggy/stream-key-fingerprint/v1" || key)}
     * as 16 lowercase hex characters.
     *
     * @throws IggyValidationException when the key is not exactly 32 bytes
     */
    public static String fingerprint(byte[] key) {
        MessageDigest digest = sha256();
        digest.update(FINGERPRINT_LABEL);
        digest.update(requireValid(key));
        byte[] hash = digest.digest();
        return HexFormat.of().formatHex(hash, 0, FINGERPRINT_BYTES);
    }

    /**
     * The fingerprint of a key given as canonical standard padded base64.
     *
     * @throws IggyValidationException when the text is not the canonical encoding of 32 bytes
     */
    public static String fingerprint(String base64Key) {
        return fingerprint(decodeBase64(base64Key));
    }

    /**
     * Decodes a key from canonical standard padded base64.
     *
     * <p>Whitespace, the URL-safe alphabet, missing padding and noncanonical trailing bits are refused, so
     * one key has exactly one accepted text form.
     *
     * @throws IggyValidationException when the text is not the canonical encoding of 32 bytes
     */
    public static byte[] decodeBase64(String base64Key) {
        byte[] key;
        try {
            key = Base64.getDecoder().decode(base64Key);
        } catch (IllegalArgumentException malformed) {
            throw invalidKey("the text is not standard base64");
        }
        requireValid(key);
        if (!Base64.getEncoder().encodeToString(key).equals(base64Key)) {
            throw invalidKey("the text is not the canonical padded base64 of the key");
        }
        return key;
    }

    /** Returns {@code key} after checking its length, without copying it. */
    static byte[] requireValid(byte[] key) {
        if (key.length != KEY_LENGTH) {
            throw invalidKey("expected " + KEY_LENGTH + " bytes, got " + key.length);
        }
        return key;
    }

    private static IggyValidationException invalidKey(String reason) {
        return new IggyValidationException(
                IggyErrorCode.INVALID_ENCRYPTION_KEY,
                IggyErrorCode.INVALID_ENCRYPTION_KEY.getCode(),
                "Invalid stream encryption key: " + reason,
                Optional.of(StreamOptionKey.ENCRYPTION_KEY.value()),
                Optional.empty());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException missing) {
            // Every Java platform is required to ship SHA-256.
            throw new IllegalStateException("SHA-256 is unavailable", missing);
        }
    }
}
