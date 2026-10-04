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

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * Mints the opaque 16-byte id stamped on a message whose caller passed a zero id.
 *
 * <p>Each thread runs its own AES-CTR keystream: a random 128-bit key and IV are drawn once from
 * {@link SecureRandom} and never reused or decrypted, so the counter blocks are cryptographically
 * strong random bytes. The id therefore has 128-bit collision resistance backed by 256 bits of
 * independent per-thread seed, and each mint is a single AES-NI block. This is a randomness source,
 * not a security primitive: the id is opaque, is not keyed on, and need not stay secret, so the
 * keystream is never reseeded.
 */
final class MessageIdGenerator {

    /** Constant plaintext encrypted to read a block of keystream; never mutated. */
    private static final byte[] PLAINTEXT = new byte[16];

    /** Draws each thread's key and IV once, from a cryptographic source. */
    private static final SecureRandom SEEDER = new SecureRandom();

    private static final ThreadLocal<Cipher> KEYSTREAM = ThreadLocal.withInitial(MessageIdGenerator::newKeystream);

    private MessageIdGenerator() {}

    /** Returns a fresh 16-byte id from the calling thread's keystream. */
    static byte[] mint() {
        byte[] minted = new byte[16];
        try {
            KEYSTREAM.get().update(PLAINTEXT, 0, 16, minted);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to mint a message id", e);
        }
        return minted;
    }

    private static Cipher newKeystream() {
        byte[] key = new byte[16];
        byte[] iv = new byte[16];
        SEEDER.nextBytes(key);
        SEEDER.nextBytes(iv);
        try {
            Cipher keystream = Cipher.getInstance("AES/CTR/NoPadding");
            keystream.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
            return keystream;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("failed to initialise the message-id keystream", e);
        }
    }
}
