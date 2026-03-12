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

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeaderValueTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void fromStringThrowsIggyInvalidArgumentExceptionWhenValueIsBlank(String value) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromString(value));
    }

    @Test
    void fromStringThrowsIggyInvalidArgumentExceptionWhenValueIsTooLong() {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromString("z".repeat(256)));
    }

    @Test
    void fromStringReturnsValidHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromString("foo");

        assertEquals(HeaderKind.String, headerValue.kind());
        assertArrayEquals(new byte[] {102, 111, 111}, headerValue.value());
    }

    @Test
    void fromBoolReturnsExpectedHeaderValueWhenValueIsTrue() {
        var headerValue = HeaderValue.fromBool(true);

        assertEquals(HeaderKind.Bool, headerValue.kind());
        assertArrayEquals(new byte[] {1}, headerValue.value());
    }

    @Test
    void fromBoolReturnsExpectedHeaderValueWhenValueIsFalse() {
        var headerValue = HeaderValue.fromBool(false);

        assertEquals(HeaderKind.Bool, headerValue.kind());
        assertArrayEquals(new byte[] {0}, headerValue.value());
    }

    @Test
    void fromInt8ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt8((byte) 127);

        assertEquals(HeaderKind.Int8, headerValue.kind());
        assertArrayEquals(new byte[] {127}, headerValue.value());
    }

    @Test
    void fromInt16ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt16(Short.MAX_VALUE);

        assertEquals(HeaderKind.Int16, headerValue.kind());
        assertArrayEquals(new byte[] {-1, 127}, headerValue.value());
    }

    @Test
    void fromInt32ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt32(Integer.MAX_VALUE);

        assertEquals(HeaderKind.Int32, headerValue.kind());
        assertArrayEquals(new byte[] {-1, -1, -1, 127}, headerValue.value());
    }

    @Test
    void fromInt64ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt64(Long.MAX_VALUE);

        assertEquals(HeaderKind.Int64, headerValue.kind());
        assertArrayEquals(new byte[] {-1, -1, -1, -1, -1, -1, -1, 127}, headerValue.value());
    }

    @ParameterizedTest
    @ValueSource(shorts = {-1, 256})
    void fromUint8ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(short value) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromUint8(value));
    }

    @Test
    void fromUint8ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint8((short) 255);

        assertEquals(HeaderKind.Uint8, headerValue.kind());
        assertArrayEquals(new byte[] {-1}, headerValue.value());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65536})
    void fromUint16ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(int value) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromUint16(value));
    }

    @Test
    void fromUint16ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint16(65535);

        assertEquals(HeaderKind.Uint16, headerValue.kind());
        assertArrayEquals(new byte[] {-1, -1}, headerValue.value());
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 4294967296L})
    void fromUint32ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(long value) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromUint32(value));
    }

    @Test
    void fromUint32ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint32(4294967295L);

        assertEquals(HeaderKind.Uint32, headerValue.kind());
        assertArrayEquals(new byte[] {-1, -1, -1, -1}, headerValue.value());
    }

    @Test
    void fromFloat32ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromFloat32(123.4f);

        assertEquals(HeaderKind.Float32, headerValue.kind());
        assertArrayEquals(new byte[] {-51, -52, -10, 66}, headerValue.value());
    }

    @Test
    void fromFloat64ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromFloat64(123.4d);

        assertEquals(HeaderKind.Float64, headerValue.kind());
        assertArrayEquals(new byte[] {-102, -103, -103, -103, -103, -39, 94, 64}, headerValue.value());
    }

    @Test
    void fromRawThrowsIggyInvalidArgumentExceptionWhenInputArrayEmpty() {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromRaw(new byte[0]));
    }

    @Test
    void fromRawThrowsIggyInvalidArgumentExceptionWhenInputArrayTooLong() {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderValue.fromRaw(new byte[256]));
    }

    @Test
    void fromRawReturnsExpectedHeaderValueWhenInputArrayValid() {
        var input = new byte[255];

        var headerValue = HeaderValue.fromRaw(input);

        assertEquals(HeaderKind.Raw, headerValue.kind());
        assertArrayEquals(input, headerValue.value());
    }

    @Test
    void asStringThrowsIggyInvalidArgumentExceptionWhenKindNotAString() {
        var headerValue = HeaderValue.fromBool(true);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asString);
    }

    @Test
    void asStringReturnsStringRepresentationOfValue() {
        var headerValue = HeaderValue.fromString("foo");

        assertEquals("foo", headerValue.asString());
    }

    @Test
    void asBoolThrowsIggyInvalidArgumentExceptionWhenKindNotBool() {
        var headerValue = HeaderValue.fromInt32(123);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asBool);
    }

    @Test
    void asBoolReturnsBoolRepresentationOfValue() {
        var headerValue = HeaderValue.fromBool(true);

        assertTrue(headerValue.asBool());
    }

    @Test
    void asInt8ThrowsIggyInvalidArgumentExceptionWhenKindNotInt8() {
        var headerValue = HeaderValue.fromFloat32(123.4f);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asInt8);
    }

    @Test
    void asInt8ReturnsInt8RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt8((byte) 12);

        assertEquals(12, headerValue.asInt8());
    }

    @Test
    void asInt16ThrowsIggyInvalidArgumentExceptionWhenKindNotInt16() {
        var headerValue = HeaderValue.fromInt32(12345);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asInt16);
    }

    @Test
    void asInt16ReturnsInt16RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt16((short) 1234);

        assertEquals(1234, headerValue.asInt16());
    }

    @Test
    void asInt32ThrowsIggyInvalidArgumentExceptionWhenKindNotInt32() {
        var headerValue = HeaderValue.fromBool(false);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asInt32);
    }

    @Test
    void asInt32ReturnsInt32RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt32(Integer.MAX_VALUE);

        assertEquals(Integer.MAX_VALUE, headerValue.asInt32());
    }

    @Test
    void asInt64ThrowsIggyInvalidArgumentExceptionWhenKindNotInt64() {
        var headerValue = HeaderValue.fromString("foo");

        assertThrows(IggyInvalidArgumentException.class, headerValue::asInt64);
    }

    @Test
    void asInt64ReturnsInt64RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt64(Long.MAX_VALUE - 1);

        assertEquals(Long.MAX_VALUE - 1, headerValue.asInt64());
    }

    @Test
    void asUint8ThrowsIggyInvalidArgumentExceptionWhenKindNotUint8() {
        var headerValue = HeaderValue.fromInt8((byte) 127);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asUint8);
    }

    @Test
    void asUint8ReturnsUint8RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint8((short) 1);

        assertEquals(1, headerValue.asUint8());
    }

    @Test
    void asUint16ThrowsIggyInvalidArgumentExceptionWhenKindNotUint16() {
        var headerValue = HeaderValue.fromFloat64(123.4d);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asUint16);
    }

    @Test
    void asUint16ReturnsUint16RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint16(Short.MAX_VALUE);

        assertEquals(Short.MAX_VALUE, headerValue.asUint16());
    }

    @Test
    void asUint32ThrowsIggyInvalidArgumentExceptionWhenKindNotUint32() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3, 4});

        assertThrows(IggyInvalidArgumentException.class, headerValue::asUint32);
    }

    @Test
    void asUint32ReturnsUint32RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint32(Integer.MAX_VALUE);

        assertEquals(Integer.MAX_VALUE, headerValue.asUint32());
    }

    @Test
    void asFloat32ThrowsIggyInvalidArgumentExceptionWhenKindNotFloat32() {
        var headerValue = HeaderValue.fromUint8((short) 10);

        assertThrows(IggyInvalidArgumentException.class, headerValue::asFloat32);
    }

    @Test
    void asFloat32ReturnsFloat32RepresentationOfValue() {
        var headerValue = HeaderValue.fromFloat32(987654.321f);

        assertEquals(987654.321f, headerValue.asFloat32());
    }

    @Test
    void asFloat64ThrowsIggyInvalidArgumentExceptionWhenKindNotFloat64() {
        var headerValue = HeaderValue.fromString("bar");

        assertThrows(IggyInvalidArgumentException.class, headerValue::asFloat64);
    }

    @Test
    void asFloat64ReturnsFloat64RepresentationOfValue() {
        var headerValue = HeaderValue.fromFloat64(9999999999.99d);

        assertEquals(9999999999.99d, headerValue.asFloat64());
    }

    @Test
    void asRawReturnsRawRepresentationOfValueRegardlessOfType() {
        assertArrayEquals(
                new byte[] {1, 2, 3}, HeaderValue.fromRaw(new byte[] {1, 2, 3}).asRaw());
        assertArrayEquals(
                new byte[] {102, 111, 111}, HeaderValue.fromString("foo").asRaw());
        assertArrayEquals(new byte[] {127}, HeaderValue.fromUint8((byte) 127).asRaw());
    }

    public static Stream<Arguments> toStringArgumentProvider() {
        return Stream.of(
                Arguments.of(HeaderValue.fromString("foo"), "foo"),
                Arguments.of(HeaderValue.fromBool(true), "true"),
                Arguments.of(HeaderValue.fromInt8((byte) -128), "-128"),
                Arguments.of(HeaderValue.fromInt16((short) -12345), "-12345"),
                Arguments.of(HeaderValue.fromInt32(987654321), "987654321"),
                Arguments.of(HeaderValue.fromInt64(0L), "0"),
                Arguments.of(HeaderValue.fromUint8((short) 127), "127"),
                Arguments.of(HeaderValue.fromUint16(333), "333"),
                Arguments.of(HeaderValue.fromUint32(99999999L), "99999999"),
                Arguments.of(HeaderValue.fromFloat32(123.4f), "123.4"),
                Arguments.of(HeaderValue.fromFloat64(99999.999d), "99999.999"),
                Arguments.of(HeaderValue.fromRaw(new byte[] {102, 111, 111}), "Zm9v"));
    }

    @ParameterizedTest
    @MethodSource("toStringArgumentProvider")
    void toString(HeaderValue headerValue, String expected) {
        assertEquals(expected, headerValue.toString());
    }

    @Test
    void equalsReturnsTrueForSameObject() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertTrue(headerValue.equals(headerValue));
    }

    @Test
    void equalsReturnsFalseWhenOtherIsNull() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertFalse(headerValue.equals(null));
    }

    @Test
    void equalsReturnsFalseWhenOtherIsDifferentType() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertFalse(headerValue.equals("foo"));
    }

    @Test
    void equalsReturnsFalseWhenKindIsDifferent() {
        var headerValue = HeaderValue.fromRaw(new byte[] {102, 111, 111});
        var other = HeaderValue.fromString("foo");

        assertFalse(headerValue.equals(other));
    }

    @Test
    void equalsReturnsFalseWhenValueIsDifferent() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});
        var other = HeaderValue.fromRaw(new byte[] {1, 2, 4});

        assertFalse(headerValue.equals(other));
    }

    @Test
    void equalsReturnsTrueWhenSameKindAndValue() {
        assertEquals(HeaderValue.fromBool(true), HeaderValue.fromBool(true));
        assertEquals(HeaderValue.fromInt32(123), HeaderValue.fromInt32(123));
        assertEquals(HeaderValue.fromRaw(new byte[] {1, 2, 3}), HeaderValue.fromRaw(new byte[] {1, 2, 3}));
    }

    @Test
    void hashCodeMatchesWhenKindsAndValuesMatch() {
        assertEquals(
                HeaderValue.fromString("foo").hashCode(),
                HeaderValue.fromString("foo").hashCode());
        assertEquals(
                HeaderValue.fromInt32(321).hashCode(),
                HeaderValue.fromInt32(321).hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenKindsAreDifferent() {
        assertNotEquals(
                HeaderValue.fromString("foo").hashCode(),
                HeaderValue.fromRaw(new byte[] {102, 111, 111}).hashCode());
        assertNotEquals(
                HeaderValue.fromInt8((byte) 127).hashCode(),
                HeaderValue.fromRaw(new byte[] {127}).hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenValuesAreDifferent() {
        assertNotEquals(
                HeaderValue.fromString("foo").hashCode(),
                HeaderValue.fromString("bar").hashCode());
        assertNotEquals(
                HeaderValue.fromBool(true).hashCode(),
                HeaderValue.fromBool(false).hashCode());
    }
}
