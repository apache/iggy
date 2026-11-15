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

import java.util.Arrays;
import java.util.Optional;

/**
 * The position after which the next page of a list starts. Send it back as you got it, with the query that returned
 * it: it holds no order, direction, filter or parent. Only the server reads its bytes, so the client keeps any 1 to
 * {@link #MAX_SIZE} bytes as they came, and the server refuses a cursor that it cannot use.
 *
 * <p>The text form is {@code pg1_} followed by unpadded Base64url of the bytes.
 */
public final class PageCursor {

    /** Text prefix of a page cursor. */
    public static final String PREFIX = "pg1_";

    /** Most bytes of a cursor: its length on the wire is one byte. */
    public static final int MAX_SIZE = 255;

    private static final int MAX_TEXT_LENGTH = PREFIX.length() + (MAX_SIZE * 4 + 2) / 3;

    private final byte[] bytes;

    private PageCursor(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Parses the {@code pg1_} text form, refusing a wrong prefix, padding, the standard Base64 alphabet, non-canonical
     * encodings, no bytes and more than {@link #MAX_SIZE} bytes.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command for any refused text
     */
    public static PageCursor parse(String text) {
        if (text == null || text.length() > MAX_TEXT_LENGTH || !text.startsWith(PREFIX)) {
            throw MetadataChangeEncoding.refused("A page cursor is pg1_ followed by Base64url");
        }
        return MetadataChangeEncoding.decodeCanonical(text.substring(PREFIX.length()))
                .flatMap(PageCursor::fromBytes)
                .orElseThrow(() -> MetadataChangeEncoding.refused(
                        "A page cursor is canonical Base64url of 1 to " + MAX_SIZE + " bytes"));
    }

    /** The cursor of {@code bytes}, or empty when they are not 1 to {@link #MAX_SIZE} bytes. */
    public static Optional<PageCursor> fromBytes(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > MAX_SIZE) {
            return Optional.empty();
        }
        return Optional.of(new PageCursor(MetadataChangeEncoding.copy(bytes)));
    }

    /** The bytes a list request carries. */
    public byte[] toBytes() {
        return MetadataChangeEncoding.copy(bytes);
    }

    /** The {@code pg1_} text form. */
    @Override
    public String toString() {
        return PREFIX + MetadataChangeEncoding.encode(bytes);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PageCursor cursor && Arrays.equals(bytes, cursor.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
