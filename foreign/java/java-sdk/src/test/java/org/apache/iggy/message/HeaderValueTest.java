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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderValueTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void fromStringThrowsIggyInvalidArgumentExceptionWhenValueIsBlank(String value) {
        assertThatThrownBy(() -> HeaderValue.fromString(value)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromStringThrowsIggyInvalidArgumentExceptionWhenValueIsTooLong() {
        assertThatThrownBy(() -> HeaderValue.fromString("z".repeat(256)))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromStringReturnsValidHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromString("foo");

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.String);
        assertThat(headerValue.value()).isEqualTo(new byte[] {102, 111, 111});
    }

    @Test
    void fromBoolReturnsExpectedHeaderValueWhenValueIsTrue() {
        var headerValue = HeaderValue.fromBool(true);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Bool);
        assertThat(headerValue.value()).isEqualTo(new byte[] {1});
    }

    @Test
    void fromBoolReturnsExpectedHeaderValueWhenValueIsFalse() {
        var headerValue = HeaderValue.fromBool(false);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Bool);
        assertThat(headerValue.value()).isEqualTo(new byte[] {0});
    }

    @Test
    void fromInt8ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt8((byte) 127);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Int8);
        assertThat(headerValue.value()).isEqualTo(new byte[] {127});
    }

    @Test
    void fromInt16ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt16(Short.MAX_VALUE);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Int16);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1, 127});
    }

    @Test
    void fromInt32ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt32(Integer.MAX_VALUE);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Int32);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1, -1, -1, 127});
    }

    @Test
    void fromInt64ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromInt64(Long.MAX_VALUE);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Int64);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1, -1, -1, -1, -1, -1, -1, 127});
    }

    @ParameterizedTest
    @ValueSource(shorts = {-1, 256})
    void fromUint8ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(short value) {
        assertThatThrownBy(() -> HeaderValue.fromUint8(value)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromUint8ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint8((short) 255);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Uint8);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1});
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 65536})
    void fromUint16ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(int value) {
        assertThatThrownBy(() -> HeaderValue.fromUint16(value)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromUint16ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint16(65535);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Uint16);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1, -1});
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 4294967296L})
    void fromUint32ThrowsIggyInvalidArgumentExceptionWhenValueOutOfBounds(long value) {
        assertThatThrownBy(() -> HeaderValue.fromUint32(value)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromUint32ReturnsExpectedHeaderValueWhenValueIsValid() {
        var headerValue = HeaderValue.fromUint32(4294967295L);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Uint32);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-1, -1, -1, -1});
    }

    @Test
    void fromFloat32ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromFloat32(123.4f);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Float32);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-51, -52, -10, 66});
    }

    @Test
    void fromFloat64ReturnsExpectedHeaderValue() {
        var headerValue = HeaderValue.fromFloat64(123.4d);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Float64);
        assertThat(headerValue.value()).isEqualTo(new byte[] {-102, -103, -103, -103, -103, -39, 94, 64});
    }

    @Test
    void fromRawThrowsIggyInvalidArgumentExceptionWhenInputArrayEmpty() {
        assertThatThrownBy(() -> HeaderValue.fromRaw(new byte[0])).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromRawThrowsIggyInvalidArgumentExceptionWhenInputArrayTooLong() {
        assertThatThrownBy(() -> HeaderValue.fromRaw(new byte[256])).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromRawReturnsExpectedHeaderValueWhenInputArrayValid() {
        var input = new byte[255];

        var headerValue = HeaderValue.fromRaw(input);

        assertThat(headerValue.kind()).isEqualTo(HeaderKind.Raw);
        assertThat(headerValue.value()).isEqualTo(input);
    }

    @Test
    void asStringThrowsIggyInvalidArgumentExceptionWhenKindNotAString() {
        var headerValue = HeaderValue.fromBool(true);

        assertThatThrownBy(headerValue::asString).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asStringReturnsStringRepresentationOfValue() {
        var headerValue = HeaderValue.fromString("foo");

        assertThat(headerValue.asString()).isEqualTo("foo");
    }

    @Test
    void asBoolThrowsIggyInvalidArgumentExceptionWhenKindNotBool() {
        var headerValue = HeaderValue.fromInt32(123);

        assertThatThrownBy(headerValue::asBool).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asBoolReturnsBoolRepresentationOfValue() {
        var headerValue = HeaderValue.fromBool(true);

        assertThat(headerValue.asBool()).isTrue();
    }

    @Test
    void asInt8ThrowsIggyInvalidArgumentExceptionWhenKindNotInt8() {
        var headerValue = HeaderValue.fromFloat32(123.4f);

        assertThatThrownBy(headerValue::asInt8).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asInt8ReturnsInt8RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt8((byte) 12);

        assertThat(headerValue.asInt8()).isEqualTo((byte) 12);
    }

    @Test
    void asInt16ThrowsIggyInvalidArgumentExceptionWhenKindNotInt16() {
        var headerValue = HeaderValue.fromInt32(12345);

        assertThatThrownBy(headerValue::asInt16).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asInt16ReturnsInt16RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt16((short) 1234);

        assertThat(headerValue.asInt16()).isEqualTo((short) 1234);
    }

    @Test
    void asInt32ThrowsIggyInvalidArgumentExceptionWhenKindNotInt32() {
        var headerValue = HeaderValue.fromBool(false);

        assertThatThrownBy(headerValue::asInt32).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asInt32ReturnsInt32RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt32(Integer.MAX_VALUE);

        assertThat(headerValue.asInt32()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void asInt64ThrowsIggyInvalidArgumentExceptionWhenKindNotInt64() {
        var headerValue = HeaderValue.fromString("foo");

        assertThatThrownBy(headerValue::asInt64).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asInt64ReturnsInt64RepresentationOfValue() {
        var headerValue = HeaderValue.fromInt64(Long.MAX_VALUE - 1);

        assertThat(headerValue.asInt64()).isEqualTo(Long.MAX_VALUE - 1);
    }

    @Test
    void asUint8ThrowsIggyInvalidArgumentExceptionWhenKindNotUint8() {
        var headerValue = HeaderValue.fromInt8((byte) 127);

        assertThatThrownBy(headerValue::asUint8).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asUint8ReturnsUint8RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint8((short) 1);

        assertThat(headerValue.asUint8()).isEqualTo((short) 1);
    }

    @Test
    void asUint16ThrowsIggyInvalidArgumentExceptionWhenKindNotUint16() {
        var headerValue = HeaderValue.fromFloat64(123.4d);

        assertThatThrownBy(headerValue::asUint16).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asUint16ReturnsUint16RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint16(Short.MAX_VALUE);

        assertThat(headerValue.asUint16()).isEqualTo(Short.MAX_VALUE);
    }

    @Test
    void asUint32ThrowsIggyInvalidArgumentExceptionWhenKindNotUint32() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3, 4});

        assertThatThrownBy(headerValue::asUint32).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asUint32ReturnsUint32RepresentationOfValue() {
        var headerValue = HeaderValue.fromUint32(Integer.MAX_VALUE);

        assertThat(headerValue.asUint32()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void asFloat32ThrowsIggyInvalidArgumentExceptionWhenKindNotFloat32() {
        var headerValue = HeaderValue.fromUint8((short) 10);

        assertThatThrownBy(headerValue::asFloat32).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asFloat32ReturnsFloat32RepresentationOfValue() {
        var headerValue = HeaderValue.fromFloat32(987654.321f);

        assertThat(headerValue.asFloat32()).isEqualTo(987654.321f);
    }

    @Test
    void asFloat64ThrowsIggyInvalidArgumentExceptionWhenKindNotFloat64() {
        var headerValue = HeaderValue.fromString("bar");

        assertThatThrownBy(headerValue::asFloat64).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void asFloat64ReturnsFloat64RepresentationOfValue() {
        var headerValue = HeaderValue.fromFloat64(9999999999.99d);

        assertThat(headerValue.asFloat64()).isEqualTo(9999999999.99d);
    }

    @Test
    void asRawReturnsRawRepresentationOfValueRegardlessOfType() {
        assertThat(HeaderValue.fromRaw(new byte[] {1, 2, 3}).asRaw()).isEqualTo(new byte[] {1, 2, 3});
        assertThat(HeaderValue.fromString("foo").asRaw()).isEqualTo(new byte[] {102, 111, 111});
        assertThat(HeaderValue.fromUint8((byte) 127).asRaw()).isEqualTo(new byte[] {127});
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
        assertThat(headerValue.toString()).isEqualTo(expected);
    }

    @Test
    void equalsReturnsTrueForSameObject() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertThat(headerValue.equals(headerValue)).isTrue();
    }

    @Test
    void equalsReturnsFalseWhenOtherIsNull() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertThat(headerValue.equals(null)).isFalse();
    }

    @Test
    void equalsReturnsFalseWhenOtherIsDifferentType() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});

        assertThat(headerValue.equals("foo")).isFalse();
    }

    @Test
    void equalsReturnsFalseWhenKindIsDifferent() {
        var headerValue = HeaderValue.fromRaw(new byte[] {102, 111, 111});
        var other = HeaderValue.fromString("foo");

        assertThat(headerValue.equals(other)).isFalse();
    }

    @Test
    void equalsReturnsFalseWhenValueIsDifferent() {
        var headerValue = HeaderValue.fromRaw(new byte[] {1, 2, 3});
        var other = HeaderValue.fromRaw(new byte[] {1, 2, 4});

        assertThat(headerValue.equals(other)).isFalse();
    }

    @Test
    void equalsReturnsTrueWhenSameKindAndValue() {
        assertThat(HeaderValue.fromBool(true)).isEqualTo(HeaderValue.fromBool(true));
        assertThat(HeaderValue.fromInt32(123)).isEqualTo(HeaderValue.fromInt32(123));
        assertThat(HeaderValue.fromRaw(new byte[] {1, 2, 3})).isEqualTo(HeaderValue.fromRaw(new byte[] {1, 2, 3}));
    }

    @Test
    void hashCodeMatchesWhenKindsAndValuesMatch() {
        assertThat(HeaderValue.fromString("foo").hashCode())
                .isEqualTo(HeaderValue.fromString("foo").hashCode());
        assertThat(HeaderValue.fromInt32(321).hashCode())
                .isEqualTo(HeaderValue.fromInt32(321).hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenKindsAreDifferent() {
        assertThat(HeaderValue.fromString("foo").hashCode())
                .isNotEqualTo(HeaderValue.fromRaw(new byte[] {102, 111, 111}).hashCode());
        assertThat(HeaderValue.fromInt8((byte) 127).hashCode())
                .isNotEqualTo(HeaderValue.fromRaw(new byte[] {127}).hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenValuesAreDifferent() {
        assertThat(HeaderValue.fromString("foo").hashCode())
                .isNotEqualTo(HeaderValue.fromString("bar").hashCode());
        assertThat(HeaderValue.fromBool(true).hashCode())
                .isNotEqualTo(HeaderValue.fromBool(false).hashCode());
    }
}
