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

package org.apache.iggy.hash;

/**
 * XXH32 (32-bit xxHash), one-shot, seed 0.
 *
 * <p>Vendored so message-key partitioning hashes identically across every
 * Iggy SDK and the server (the Rust side uses {@code twox_hash::XxHash32}
 * with seed 0). The routing contract is
 * {@code xxh32(key, 0) % partitionCount}, so any deviation here silently
 * breaks per-key ordering against other clients.
 */
public final class XxHash32 {

    private static final int PRIME_1 = 0x9E3779B1;
    private static final int PRIME_2 = 0x85EBCA77;
    private static final int PRIME_3 = 0xC2B2AE3D;
    private static final int PRIME_4 = 0x27D4EB2F;
    private static final int PRIME_5 = 0x165667B1;

    private XxHash32() {}

    /**
     * Hashes the given bytes with XXH32, seed 0.
     *
     * @param data the bytes to hash
     * @return the 32-bit hash, as an unsigned value in a long
     */
    public static long hashUnsigned(byte[] data) {
        return Integer.toUnsignedLong(hash(data));
    }

    static int hash(byte[] data) {
        final int length = data.length;
        int offset = 0;
        int hash;

        if (length >= 16) {
            int v1 = PRIME_1 + PRIME_2;
            int v2 = PRIME_2;
            int v3 = 0;
            int v4 = -PRIME_1;
            for (; offset <= length - 16; offset += 16) {
                v1 = round(v1, readIntLE(data, offset));
                v2 = round(v2, readIntLE(data, offset + 4));
                v3 = round(v3, readIntLE(data, offset + 8));
                v4 = round(v4, readIntLE(data, offset + 12));
            }
            hash = Integer.rotateLeft(v1, 1)
                    + Integer.rotateLeft(v2, 7)
                    + Integer.rotateLeft(v3, 12)
                    + Integer.rotateLeft(v4, 18);
        } else {
            hash = PRIME_5;
        }

        hash += length;

        for (; offset <= length - 4; offset += 4) {
            hash = Integer.rotateLeft(hash + readIntLE(data, offset) * PRIME_3, 17) * PRIME_4;
        }
        for (; offset < length; offset++) {
            hash = Integer.rotateLeft(hash + (data[offset] & 0xFF) * PRIME_5, 11) * PRIME_1;
        }

        hash ^= hash >>> 15;
        hash *= PRIME_2;
        hash ^= hash >>> 13;
        hash *= PRIME_3;
        hash ^= hash >>> 16;
        return hash;
    }

    private static int round(int accumulator, int lane) {
        return Integer.rotateLeft(accumulator + lane * PRIME_2, 13) * PRIME_1;
    }

    private static int readIntLE(byte[] data, int offset) {
        return (data[offset] & 0xFF)
                | (data[offset + 1] & 0xFF) << 8
                | (data[offset + 2] & 0xFF) << 16
                | (data[offset + 3] & 0xFF) << 24;
    }
}
