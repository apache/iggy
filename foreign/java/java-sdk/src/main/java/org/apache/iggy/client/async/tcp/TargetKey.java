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

import java.util.Arrays;

/**
 * The target bytes of a partition request, or their stream and topic prefix, compared by content. Nothing writes
 * the bytes after a key holds them.
 */
final class TargetKey {
    private final byte[] target;
    private final int length;
    private final int hash;

    private TargetKey(byte[] target, int length) {
        this.target = target;
        this.length = length;
        int contentHash = 1;
        for (int index = 0; index < length; index++) {
            contentHash = 31 * contentHash + target[index];
        }
        this.hash = contentHash;
    }

    static TargetKey of(byte[] target) {
        return new TargetKey(target, target.length);
    }

    /** The stream and topic of a partition target, without its trailing partition id. */
    TargetKey topic() {
        return new TargetKey(target, length - Integer.BYTES);
    }

    boolean startsWith(TargetKey prefix) {
        return length >= prefix.length && Arrays.equals(target, 0, prefix.length, prefix.target, 0, prefix.length);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetKey key && Arrays.equals(target, 0, length, key.target, 0, key.length);
    }

    @Override
    public int hashCode() {
        return hash;
    }
}
