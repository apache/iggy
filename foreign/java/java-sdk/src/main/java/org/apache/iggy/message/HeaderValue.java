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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public record HeaderValue(HeaderKind kind, byte[] value) {
    @JsonCreator
    public static HeaderValue fromJson(
            @JsonProperty("kind") HeaderKind kind, @JsonProperty("value") String base64Value) {
        byte[] decodedValue = Base64.getDecoder().decode(base64Value);
        return new HeaderValue(kind, decodedValue);
    }

    public static HeaderValue fromString(String val) {
        if (val.isEmpty() || val.length() > 255) {
            throw new IllegalArgumentException("Value has incorrect size, must be between 1 and 255");
        }
        return new HeaderValue(HeaderKind.String, val.getBytes(StandardCharsets.UTF_8));
    }

    public static HeaderValue fromBool(boolean val) {
        return new HeaderValue(HeaderKind.Bool, new byte[] {(byte) (val ? 1 : 0)});
    }

    public static HeaderValue fromInt8(byte val) {
        return new HeaderValue(HeaderKind.Int8, new byte[] {val});
    }

    public static HeaderValue fromInt16(short val) {
        ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(val);
        return new HeaderValue(HeaderKind.Int16, buffer.array());
    }

    public static HeaderValue fromInt32(int val) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(val);
        return new HeaderValue(HeaderKind.Int32, buffer.array());
    }

    public static HeaderValue fromInt64(long val) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(val);
        return new HeaderValue(HeaderKind.Int64, buffer.array());
    }

    public static HeaderValue fromUint8(short val) {
        if (val < 0 || val > 255) {
            throw new IllegalArgumentException("Value must be between 0 and 255");
        }
        return new HeaderValue(HeaderKind.Uint8, new byte[] {(byte) val});
    }

    public static HeaderValue fromUint16(int val) {
        if (val < 0 || val > 65535) {
            throw new IllegalArgumentException("Value must be between 0 and 65535");
        }
        ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort((short) val);
        return new HeaderValue(HeaderKind.Uint16, buffer.array());
    }

    public static HeaderValue fromUint32(long val) {
        if (val < 0 || val > 4294967295L) {
            throw new IllegalArgumentException("Value must be between 0 and 4294967295");
        }
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt((int) val);
        return new HeaderValue(HeaderKind.Uint32, buffer.array());
    }

    public static HeaderValue fromFloat32(float val) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putFloat(val);
        return new HeaderValue(HeaderKind.Float32, buffer.array());
    }

    public static HeaderValue fromFloat64(double val) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putDouble(val);
        return new HeaderValue(HeaderKind.Float64, buffer.array());
    }

    public static HeaderValue fromRaw(byte[] val) {
        if (val.length == 0 || val.length > 255) {
            throw new IllegalArgumentException("Value has incorrect size, must be between 1 and 255");
        }
        return new HeaderValue(HeaderKind.Raw, val);
    }

    public String asString() {
        if (kind != HeaderKind.String) {
            throw new IllegalStateException("Header value is not a string, kind: " + kind);
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    public boolean asBool() {
        if (kind != HeaderKind.Bool) {
            throw new IllegalStateException("Header value is not a bool, kind: " + kind);
        }
        return value[0] == 1;
    }

    public byte asInt8() {
        if (kind != HeaderKind.Int8) {
            throw new IllegalStateException("Header value is not an int8, kind: " + kind);
        }
        return value[0];
    }

    public short asInt16() {
        if (kind != HeaderKind.Int16) {
            throw new IllegalStateException("Header value is not an int16, kind: " + kind);
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getShort();
    }

    public int asInt32() {
        if (kind != HeaderKind.Int32) {
            throw new IllegalStateException("Header value is not an int32, kind: " + kind);
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    public long asInt64() {
        if (kind != HeaderKind.Int64) {
            throw new IllegalStateException("Header value is not an int64, kind: " + kind);
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public short asUint8() {
        if (kind != HeaderKind.Uint8) {
            throw new IllegalStateException("Header value is not a uint8, kind: " + kind);
        }
        return (short) (value[0] & 0xFF);
    }

    public int asUint16() {
        if (kind != HeaderKind.Uint16) {
            throw new IllegalStateException("Header value is not a uint16, kind: " + kind);
        }
        return (ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF);
    }

    public long asUint32() {
        if (kind != HeaderKind.Uint32) {
            throw new IllegalStateException("Header value is not a uint32, kind: " + kind);
        }
        return (ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL);
    }

    public float asFloat32() {
        if (kind != HeaderKind.Float32) {
            throw new IllegalStateException("Header value is not a float32, kind: " + kind);
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    public double asFloat64() {
        if (kind != HeaderKind.Float64) {
            throw new IllegalStateException("Header value is not a float64, kind: " + kind);
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    public byte[] asRaw() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HeaderValue that = (HeaderValue) o;
        return kind == that.kind && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = kind.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return toStringValue();
    }

    private String toStringValue() {
        if (kind == HeaderKind.String) {
            return asString();
        }
        if (kind == HeaderKind.Bool) {
            return String.valueOf(asBool());
        }
        return numericOrRawToString();
    }

    private String numericOrRawToString() {
        return switch (kind) {
            case Int8 -> String.valueOf(asInt8());
            case Int16 -> String.valueOf(asInt16());
            case Int32 -> String.valueOf(asInt32());
            case Int64 -> String.valueOf(asInt64());
            case Uint8 -> String.valueOf(asUint8());
            case Uint16 -> String.valueOf(asUint16());
            case Uint32 -> String.valueOf(asUint32());
            case Float32 -> String.valueOf(asFloat32());
            case Float64 -> String.valueOf(asFloat64());
            default -> Base64.getEncoder().encodeToString(value);
        };
    }
}
