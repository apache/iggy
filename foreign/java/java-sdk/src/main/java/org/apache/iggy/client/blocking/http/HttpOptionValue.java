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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One option entry as REST renders it: the value as reversible text, its kind, and the provenance flag
 * that the binary protocol carries as two separate blocks instead.
 *
 * <p>The text is UTF-8 for strings, {@code true} or {@code false}, base-10 integers, standard padded
 * base64 for raw bytes, and {@code 0x} followed by the IEEE-754 bits in fixed-width lowercase hex for
 * floats. Rebuilding each value in its kind lets a caller read the same {@link HeaderValue} on either
 * transport.
 */
record HttpOptionValue(String value, HeaderKind kind, boolean explicit) {

    private static final String RESET_FIELD = "reset";
    private static final String FLOAT_PREFIX = "0x";

    /** The entries of {@code options} whose provenance is {@code explicit}, rebuilt in their kinds. */
    static Map<String, HeaderValue> select(Map<String, HttpOptionValue> options, boolean explicit) {
        if (options == null) {
            return Map.of();
        }
        Map<String, HeaderValue> selected = new LinkedHashMap<>();
        options.forEach((key, option) -> {
            if (option.explicit() == explicit) {
                selected.put(key, option.toHeaderValue(key));
            }
        });
        return Map.copyOf(selected);
    }

    /**
     * Option values for a create body: strings, booleans, unsigned integers as JSON numbers, and raw bytes
     * as standard padded base64.
     */
    static Map<String, Object> requestValues(Map<String, HeaderValue> options) {
        Map<String, Object> values = new LinkedHashMap<>();
        options.forEach((key, value) -> values.put(key, requestValue(key, value)));
        return values;
    }

    /**
     * A {@code PUT} update body: one flat object of the keys to set, plus a {@code reset} array naming the
     * keys to return to their defaults.
     */
    static Map<String, Object> updateBody(Map<String, HeaderValue> set, List<String> reset) {
        Map<String, Object> body = requestValues(set);
        if (!reset.isEmpty()) {
            body.put(RESET_FIELD, reset);
        }
        return body;
    }

    private static Object requestValue(String key, HeaderValue value) {
        return switch (value.kind()) {
            case String -> value.asString();
            case Bool -> value.asBool();
            case Uint8 -> value.asUint8();
            case Uint16 -> value.asUint16();
            case Uint32 -> value.asUint32();
            case Uint64 -> value.asUint64();
            case Raw -> Base64.getEncoder().encodeToString(value.asRaw());
            default ->
                throw new IggyInvalidArgumentException(
                        "Option '" + key + "' of kind " + value.kind() + " has no HTTP form");
        };
    }

    private HeaderValue toHeaderValue(String key) {
        try {
            return switch (kind) {
                case Raw -> raw();
                case String -> new HeaderValue(kind, value.getBytes(StandardCharsets.UTF_8));
                case Bool -> bool();
                case Float32 -> floatBits(Integer.BYTES);
                case Float64 -> floatBits(Long.BYTES);
                default -> integer();
            };
        } catch (IllegalArgumentException malformed) {
            throw new IggyMalformedResponseException("Option '" + key + "' value does not decode as "
                    + kind.name().toLowerCase(Locale.ROOT));
        }
    }

    private HeaderValue raw() {
        byte[] bytes = Base64.getDecoder().decode(value);
        if (!Base64.getEncoder().encodeToString(bytes).equals(value)) {
            throw new IllegalArgumentException("noncanonical base64");
        }
        return new HeaderValue(kind, bytes);
    }

    private HeaderValue bool() {
        if (!"true".equals(value) && !"false".equals(value)) {
            throw new IllegalArgumentException("not a boolean");
        }
        return HeaderValue.fromBool("true".equals(value));
    }

    /** An integer kind: Int8 to Int128 are signed, Uint8 to Uint128 unsigned, widths doubling from one byte. */
    private HeaderValue integer() {
        boolean signed = kind.asCode() <= HeaderKind.Int128.asCode();
        int first = signed ? HeaderKind.Int8.asCode() : HeaderKind.Uint8.asCode();
        int width = 1 << (kind.asCode() - first);
        BigInteger number = new BigInteger(value);
        if (!number.toString().equals(value)) {
            throw new IllegalArgumentException("noncanonical integer");
        }
        boolean fits = signed
                ? number.bitLength() < width * Byte.SIZE
                : number.signum() >= 0 && number.bitLength() <= width * Byte.SIZE;
        if (!fits) {
            throw new IllegalArgumentException("out of range");
        }
        return new HeaderValue(kind, littleEndian(number, width));
    }

    private HeaderValue floatBits(int width) {
        String hex = value.startsWith(FLOAT_PREFIX) ? value.substring(FLOAT_PREFIX.length()) : "";
        if (hex.length() != width * 2 || !hex.matches("[0-9a-f]+")) {
            throw new IllegalArgumentException("not fixed-width float bits");
        }
        return new HeaderValue(kind, littleEndian(new BigInteger(hex, 16), width));
    }

    private static byte[] littleEndian(BigInteger number, int width) {
        byte[] bytes = new byte[width];
        for (int index = 0; index < width; index++) {
            bytes[index] = (byte) number.shiftRight(Byte.SIZE * index).intValue();
        }
        return bytes;
    }
}
