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

package org.apache.iggy.message;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record Partitioning(PartitioningKind kind, byte[] value) {

    private static final int PARTITION_ID_LENGTH = 4;

    /** Server-side cap on a messages key, in encoded bytes, matching its u8 length prefix. */
    private static final int MAX_MESSAGES_KEY_LENGTH = 255;

    public Partitioning {
        if (kind == null || value == null) {
            throw new IggyInvalidArgumentException("Partitioning kind and value cannot be null");
        }
        boolean valid =
                switch (kind) {
                    case Balanced -> value.length == 0;
                    case PartitionId -> value.length == PARTITION_ID_LENGTH;
                    case MessagesKey -> value.length >= 1 && value.length <= MAX_MESSAGES_KEY_LENGTH;
                };
        if (!valid) {
            throw new IggyInvalidArgumentException(
                    kind + " partitioning value must be " + expectedLength(kind) + " bytes, got " + value.length);
        }
    }

    public static Partitioning balanced() {
        return new Partitioning(PartitioningKind.Balanced, new byte[] {});
    }

    public static Partitioning partitionId(Long id) {
        if (id == null) {
            throw new IggyInvalidArgumentException("Partition id cannot be null");
        }
        ByteBuffer buffer = ByteBuffer.allocate(PARTITION_ID_LENGTH);
        buffer.putInt(id.intValue());
        byte[] partitionId = buffer.array();
        ArrayUtils.reverse(partitionId);
        return new Partitioning(PartitioningKind.PartitionId, partitionId);
    }

    public static Partitioning messagesKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IggyInvalidArgumentException("Key must be non-empty");
        }
        return new Partitioning(PartitioningKind.MessagesKey, key.getBytes(StandardCharsets.UTF_8));
    }

    public int getSize() {
        // kind, 1 byte + length, 1 byte + value.length()
        return 2 + value.length;
    }

    private static String expectedLength(PartitioningKind kind) {
        return switch (kind) {
            case Balanced -> "0";
            case PartitionId -> String.valueOf(PARTITION_ID_LENGTH);
            case MessagesKey -> "1.." + MAX_MESSAGES_KEY_LENGTH;
        };
    }
}
