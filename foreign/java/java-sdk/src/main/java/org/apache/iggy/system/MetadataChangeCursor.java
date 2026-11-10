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

import java.math.BigInteger;

/**
 * Position of the last metadata change record a caller consumed or scanned. Send the cursor an answer returns in
 * {@link MetadataChanges#next()} back to read the records after it.
 *
 * <p>The text form is {@code mc1_} followed by unpadded Base64url of
 * {@code [log_id:u128][op:u64][checksum:u128][ordinal:u32]}, little-endian. Op 0 with a zero checksum and ordinal
 * names the beginning of the history {@code logId} identifies.
 *
 * @param logId prepare checksum of op 1, the root bootstrap of this history
 * @param op the metadata operation of the record
 * @param checksum prepare checksum of {@code op}
 * @param ordinal position of the record within {@code op}
 */
public record MetadataChangeCursor(BigInteger logId, BigInteger op, BigInteger checksum, long ordinal) {

    /** Text prefix of a change cursor. */
    public static final String PREFIX = "mc1_";

    /** Encoded size of a change cursor. */
    public static final int SIZE = 44;

    private static final int TEXT_LENGTH = 63;

    public MetadataChangeCursor {
        MetadataChangeEncoding.requireUnsigned(logId, 128, "Cursor log id");
        MetadataChangeEncoding.requireUnsigned(op, 64, "Cursor op");
        MetadataChangeEncoding.requireUnsigned(checksum, 128, "Cursor checksum");
        MetadataChangeEncoding.requireUnsigned(BigInteger.valueOf(ordinal), 32, "Cursor ordinal");
    }

    /**
     * Parses the {@code mc1_} text form, refusing a wrong prefix or length, padding, the standard Base64 alphabet,
     * non-canonical encodings and a non-canonical cursor.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command for any refused text
     */
    public static MetadataChangeCursor parse(String text) {
        if (text == null || text.length() != TEXT_LENGTH || !text.startsWith(PREFIX)) {
            throw MetadataChangeEncoding.refused("A change cursor is mc1_ followed by 59 Base64url characters");
        }
        byte[] bytes = MetadataChangeEncoding.decodeCanonical(text.substring(PREFIX.length()))
                .filter(decoded -> decoded.length == SIZE)
                .orElseThrow(() -> MetadataChangeEncoding.refused("A change cursor is not canonical Base64url"));
        var cursor = fromBytes(bytes, 0);
        if (!cursor.isCanonical()) {
            throw MetadataChangeEncoding.refused("A cursor at op 0 carries no checksum and no ordinal");
        }
        return cursor;
    }

    /** Reads the 44 bytes at {@code offset} without the canonical check. */
    public static MetadataChangeCursor fromBytes(byte[] bytes, int offset) {
        return new MetadataChangeCursor(
                MetadataChangeEncoding.readUnsigned(bytes, offset, 16),
                MetadataChangeEncoding.readUnsigned(bytes, offset + 16, 8),
                MetadataChangeEncoding.readUnsigned(bytes, offset + 24, 16),
                MetadataChangeEncoding.readUnsignedInt(bytes, offset + 40));
    }

    /** Whether the cursor names the beginning of its history. */
    public boolean isZero() {
        return op.signum() == 0;
    }

    /** Op 0 carries no checksum and no ordinal. A cursor that does is refused. */
    public boolean isCanonical() {
        return op.signum() != 0 || (checksum.signum() == 0 && ordinal == 0);
    }

    /** The 44-byte wire form. */
    public byte[] toBytes() {
        byte[] bytes = new byte[SIZE];
        MetadataChangeEncoding.writeUnsigned(bytes, 0, 16, logId);
        MetadataChangeEncoding.writeUnsigned(bytes, 16, 8, op);
        MetadataChangeEncoding.writeUnsigned(bytes, 24, 16, checksum);
        MetadataChangeEncoding.writeUnsigned(bytes, 40, 4, BigInteger.valueOf(ordinal));
        return bytes;
    }

    /** The {@code mc1_} text form. */
    @Override
    public String toString() {
        return PREFIX + MetadataChangeEncoding.encode(toBytes());
    }
}
