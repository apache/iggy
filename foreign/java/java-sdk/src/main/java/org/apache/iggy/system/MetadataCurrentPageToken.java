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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

/**
 * Opaque continuation of a current state scan. Send it back unchanged with the same groups through
 * {@link MetadataChangeStart#current(MetadataCurrentPageToken)}.
 *
 * <p>The text form is {@code cs1_} followed by unpadded Base64url of
 * {@code [anchor:44][required_head:u64][groups_mask:u8][group:u8][key_len:u16][last_key]}, little-endian.
 */
public final class MetadataCurrentPageToken {

    /** Text prefix of a current state page token. */
    public static final String PREFIX = "cs1_";

    /** Longest key: a user id and a personal access token name. */
    public static final int MAX_KEY_SIZE = 4 + 1 + 255;

    /** Longest decoded token payload. */
    public static final int MAX_SIZE = MetadataChangeCursor.SIZE + 8 + 1 + 1 + 2 + MAX_KEY_SIZE;

    private static final int FIXED_SIZE = MetadataChangeCursor.SIZE + 8 + 1 + 1 + 2;
    private static final int MAX_TEXT_LENGTH = PREFIX.length() + (MAX_SIZE + 2) / 3 * 4;
    private static final int MASK_OFFSET = MetadataChangeCursor.SIZE + 8;

    private final byte[] payload;

    private MetadataCurrentPageToken(byte[] payload) {
        this.payload = payload;
    }

    /**
     * Parses the {@code cs1_} text form, refusing a wrong prefix, padding, non-canonical encodings and a payload
     * that breaks the token's own rules.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command for any refused text
     */
    public static MetadataCurrentPageToken parse(String text) {
        if (text == null || text.length() > MAX_TEXT_LENGTH || !text.startsWith(PREFIX)) {
            throw MetadataChangeEncoding.refused("A page token is cs1_ followed by Base64url");
        }
        return MetadataChangeEncoding.decodeCanonical(text.substring(PREFIX.length()))
                .flatMap(MetadataCurrentPageToken::fromPayload)
                .orElseThrow(() -> MetadataChangeEncoding.refused("A page token is not a canonical page token"));
    }

    /**
     * Decodes a token payload, requiring it to be exactly one token: a canonical anchor, a known group selected
     * by the mask, a head at or above the anchor and a key of the exact width that group uses.
     */
    public static Optional<MetadataCurrentPageToken> fromPayload(byte[] payload) {
        if (payload.length < FIXED_SIZE) {
            return Optional.empty();
        }
        int keyLength = (payload[FIXED_SIZE - 2] & 0xFF) | ((payload[FIXED_SIZE - 1] & 0xFF) << Byte.SIZE);
        if (keyLength > MAX_KEY_SIZE || payload.length != FIXED_SIZE + keyLength) {
            return Optional.empty();
        }
        var token = new MetadataCurrentPageToken(MetadataChangeEncoding.copy(payload));
        Optional<MetadataChangeGroup> group = MetadataChangeGroup.fromCode(payload[MASK_OFFSET + 1] & 0xFF);
        boolean valid = token.anchor().isCanonical()
                && token.requiredHead().compareTo(token.anchor().op()) >= 0
                && group.filter(known -> (token.groupsMask() & known.bit()) != 0)
                        .filter(known -> keyFitsGroup(known, token.lastKey()))
                        .isPresent();
        return valid ? Optional.of(token) : Optional.empty();
    }

    /** The anchor captured before the scan's first page. */
    public MetadataChangeCursor anchor() {
        return MetadataChangeCursor.fromBytes(payload, 0);
    }

    /** The highest head any page of the scan reported so far. */
    public BigInteger requiredHead() {
        return MetadataChangeEncoding.readUnsigned(payload, MetadataChangeCursor.SIZE, 8);
    }

    /** The groups the scan was started with, as their mask. */
    public int groupsMask() {
        return payload[MASK_OFFSET] & 0xFF;
    }

    /** The groups the scan was started with. */
    public Set<MetadataChangeGroup> groups() {
        return MetadataChangeGroup.fromMask(groupsMask());
    }

    /** The group of the last item returned. */
    public MetadataChangeGroup lastGroup() {
        return MetadataChangeGroup.fromCode(payload[MASK_OFFSET + 1] & 0xFF).orElseThrow();
    }

    /** The key of the last item returned, in the page token key layout. */
    public byte[] lastKey() {
        return Arrays.copyOfRange(payload, FIXED_SIZE, payload.length);
    }

    /** The decoded payload binary requests and responses carry. */
    public byte[] payload() {
        return MetadataChangeEncoding.copy(payload);
    }

    /** The {@code cs1_} text form. */
    @Override
    public String toString() {
        return PREFIX + MetadataChangeEncoding.encode(payload);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof MetadataCurrentPageToken token && Arrays.equals(payload, token.payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }

    private static boolean keyFitsGroup(MetadataChangeGroup group, byte[] key) {
        return switch (group) {
            case STREAMS, USERS -> key.length == 4;
            case TOPICS, PARTITIONS, SESSIONS -> key.length == 8;
            case CONSUMER_GROUPS -> key.length == 12;
            case CONSUMER_GROUP_MEMBERS -> key.length == 16;
            default -> key.length > 5 && key.length == 5 + (key[4] & 0xFF) && isUtf8(key, 5);
        };
    }

    private static boolean isUtf8(byte[] bytes, int offset) {
        try {
            StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes, offset, bytes.length - offset));
            return true;
        } catch (CharacterCodingException malformed) {
            return false;
        }
    }
}
