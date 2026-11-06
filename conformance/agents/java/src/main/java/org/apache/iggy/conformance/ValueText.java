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

package org.apache.iggy.conformance;

import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

/**
 * The reversible text of a header or option value: UTF-8 for strings, true or false, base-10 integers, padded
 * standard base64 for raw bytes, and 0x with the IEEE-754 bits in fixed-width lowercase hex for floats.
 */
final class ValueText {

    private ValueText() {}

    /** The kind name of the shared fixtures, such as {@code uint64} or {@code float32}. */
    static String kind(HeaderKind kind) {
        return kind.name().toLowerCase(Locale.ROOT);
    }

    static String text(HeaderValue value) {
        byte[] bytes = value.value();
        int width = width(value.kind());
        if (width > 0 && bytes.length != width) {
            throw new IllegalStateException("a " + kind(value.kind()) + " value of " + bytes.length + " bytes");
        }
        return switch (value.kind()) {
            case Raw -> Base64.getEncoder().encodeToString(bytes);
            case String -> new String(bytes, StandardCharsets.UTF_8);
            case Bool -> String.valueOf(bytes[0] == 1);
            case Int8, Int16, Int32, Int64, Int128 -> littleEndian(bytes, true).toString();
            case Uint8, Uint16, Uint32, Uint64, Uint128 ->
                littleEndian(bytes, false).toString();
            case Float32 -> String.format("0x%08x", littleEndian(bytes, false));
            case Float64 -> String.format("0x%016x", littleEndian(bytes, false));
        };
    }

    static HeaderValue fromText(String kindName, String text) {
        HeaderKind kind = kindNamed(kindName);
        int width = width(kind);
        byte[] bytes = switch (kind) {
            case Raw -> Base64.getDecoder().decode(text);
            case String -> text.getBytes(StandardCharsets.UTF_8);
            case Bool -> new byte[] {booleanByte(text)};
            case Float32, Float64 -> littleEndianBytes(floatBits(text, width), width, false);
            case Int8, Int16, Int32, Int64, Int128 -> littleEndianBytes(new BigInteger(text), width, true);
            case Uint8, Uint16, Uint32, Uint64, Uint128 -> littleEndianBytes(new BigInteger(text), width, false);
        };
        return new HeaderValue(kind, bytes);
    }

    private static HeaderKind kindNamed(String name) {
        for (HeaderKind kind : HeaderKind.values()) {
            if (kind(kind).equals(name)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown value kind " + name);
    }

    /** The number of bytes of a fixed-width kind, or zero for raw bytes and strings. */
    private static int width(HeaderKind kind) {
        return switch (kind) {
            case Raw, String -> 0;
            case Bool, Int8, Uint8 -> 1;
            case Int16, Uint16 -> 2;
            case Int32, Uint32, Float32 -> 4;
            case Int64, Uint64, Float64 -> 8;
            case Int128, Uint128 -> 16;
        };
    }

    private static BigInteger littleEndian(byte[] bytes, boolean signed) {
        byte[] bigEndian = new byte[bytes.length];
        for (int index = 0; index < bytes.length; index++) {
            bigEndian[bytes.length - 1 - index] = bytes[index];
        }
        return signed ? new BigInteger(bigEndian) : new BigInteger(1, bigEndian);
    }

    private static byte[] littleEndianBytes(BigInteger number, int width, boolean signed) {
        boolean fits = signed
                ? number.bitLength() < width * Byte.SIZE
                : number.signum() >= 0 && number.bitLength() <= width * Byte.SIZE;
        if (!fits) {
            throw new IllegalArgumentException(number + " does not fit in " + width + " bytes");
        }
        byte[] bytes = new byte[width];
        for (int index = 0; index < width; index++) {
            bytes[index] = number.shiftRight(Byte.SIZE * index).byteValue();
        }
        return bytes;
    }

    private static BigInteger floatBits(String text, int width) {
        if (!text.matches("0x[0-9a-f]{" + width * 2 + "}")) {
            throw new IllegalArgumentException(text + " is not 0x and " + width * 2 + " hex digits of IEEE-754 bits");
        }
        return new BigInteger(text.substring(2), 16);
    }

    private static byte booleanByte(String text) {
        return switch (text) {
            case "true" -> 1;
            case "false" -> 0;
            default -> throw new IllegalArgumentException(text + " is not true or false");
        };
    }
}
