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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderValueTest {

    @Nested
    class StringType {

        @Test
        void shouldCreateFromString() {
            var hv = HeaderValue.fromString("hello");
            assertThat(hv.kind()).isEqualTo(HeaderKind.String);
            assertThat(hv.asString()).isEqualTo("hello");
        }

        @Test
        void shouldRejectBlankString() {
            assertThatThrownBy(() -> HeaderValue.fromString("")).isInstanceOf(IggyInvalidArgumentException.class);
            assertThatThrownBy(() -> HeaderValue.fromString("   ")).isInstanceOf(IggyInvalidArgumentException.class);
            assertThatThrownBy(() -> HeaderValue.fromString(null)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectStringOver255Bytes() {
            String longString = "a".repeat(256);
            assertThatThrownBy(() -> HeaderValue.fromString(longString))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectAccessingStringAsBool() {
            var hv = HeaderValue.fromString("test");
            assertThatThrownBy(hv::asBool).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class BoolType {

        @Test
        void shouldCreateFromTrue() {
            var hv = HeaderValue.fromBool(true);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Bool);
            assertThat(hv.asBool()).isTrue();
        }

        @Test
        void shouldCreateFromFalse() {
            var hv = HeaderValue.fromBool(false);
            assertThat(hv.asBool()).isFalse();
        }

        @Test
        void shouldRejectAccessingBoolAsString() {
            var hv = HeaderValue.fromBool(true);
            assertThatThrownBy(hv::asString).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Int8Type {

        @Test
        void shouldCreateFromInt8() {
            var hv = HeaderValue.fromInt8((byte) 42);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Int8);
            assertThat(hv.asInt8()).isEqualTo((byte) 42);
        }

        @Test
        void shouldHandleNegativeInt8() {
            var hv = HeaderValue.fromInt8((byte) -1);
            assertThat(hv.asInt8()).isEqualTo((byte) -1);
        }

        @Test
        void shouldRejectAccessingInt8AsInt32() {
            var hv = HeaderValue.fromInt8((byte) 1);
            assertThatThrownBy(hv::asInt32).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Int16Type {

        @Test
        void shouldCreateFromInt16() {
            var hv = HeaderValue.fromInt16((short) 12345);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Int16);
            assertThat(hv.asInt16()).isEqualTo((short) 12345);
        }

        @Test
        void shouldRejectAccessingInt16AsInt64() {
            var hv = HeaderValue.fromInt16((short) 1);
            assertThatThrownBy(hv::asInt64).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Int32Type {

        @Test
        void shouldCreateFromInt32() {
            var hv = HeaderValue.fromInt32(123456789);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Int32);
            assertThat(hv.asInt32()).isEqualTo(123456789);
        }

        @Test
        void shouldRejectAccessingInt32AsInt16() {
            var hv = HeaderValue.fromInt32(1);
            assertThatThrownBy(hv::asInt16).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Int64Type {

        @Test
        void shouldCreateFromInt64() {
            var hv = HeaderValue.fromInt64(9876543210L);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Int64);
            assertThat(hv.asInt64()).isEqualTo(9876543210L);
        }

        @Test
        void shouldRejectAccessingInt64AsInt8() {
            var hv = HeaderValue.fromInt64(1L);
            assertThatThrownBy(hv::asInt8).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Uint8Type {

        @Test
        void shouldCreateFromUint8() {
            var hv = HeaderValue.fromUint8((short) 200);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Uint8);
            assertThat(hv.asUint8()).isEqualTo((short) 200);
        }

        @Test
        void shouldCreateFromUint8Zero() {
            var hv = HeaderValue.fromUint8((short) 0);
            assertThat(hv.asUint8()).isEqualTo((short) 0);
        }

        @Test
        void shouldCreateFromUint8Max() {
            var hv = HeaderValue.fromUint8((short) 255);
            assertThat(hv.asUint8()).isEqualTo((short) 255);
        }

        @Test
        void shouldRejectNegativeUint8() {
            assertThatThrownBy(() -> HeaderValue.fromUint8((short) -1))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectUint8Over255() {
            assertThatThrownBy(() -> HeaderValue.fromUint8((short) 256))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectAccessingUint8AsUint16() {
            var hv = HeaderValue.fromUint8((short) 1);
            assertThatThrownBy(hv::asUint16).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Uint16Type {

        @Test
        void shouldCreateFromUint16() {
            var hv = HeaderValue.fromUint16(50000);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Uint16);
            assertThat(hv.asUint16()).isEqualTo(50000);
        }

        @Test
        void shouldRejectNegativeUint16() {
            assertThatThrownBy(() -> HeaderValue.fromUint16(-1)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectUint16Over65535() {
            assertThatThrownBy(() -> HeaderValue.fromUint16(65536)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectAccessingUint16AsUint32() {
            var hv = HeaderValue.fromUint16(1);
            assertThatThrownBy(hv::asUint32).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Uint32Type {

        @Test
        void shouldCreateFromUint32() {
            var hv = HeaderValue.fromUint32(3000000000L);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Uint32);
            assertThat(hv.asUint32()).isEqualTo(3000000000L);
        }

        @Test
        void shouldRejectNegativeUint32() {
            assertThatThrownBy(() -> HeaderValue.fromUint32(-1)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectUint32Over4294967295() {
            assertThatThrownBy(() -> HeaderValue.fromUint32(4294967296L))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectAccessingUint32AsFloat32() {
            var hv = HeaderValue.fromUint32(1L);
            assertThatThrownBy(hv::asFloat32).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Float32Type {

        @Test
        void shouldCreateFromFloat32() {
            var hv = HeaderValue.fromFloat32(3.14f);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Float32);
            assertThat(hv.asFloat32()).isEqualTo(3.14f);
        }

        @Test
        void shouldRejectAccessingFloat32AsFloat64() {
            var hv = HeaderValue.fromFloat32(1.0f);
            assertThatThrownBy(hv::asFloat64).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class Float64Type {

        @Test
        void shouldCreateFromFloat64() {
            var hv = HeaderValue.fromFloat64(3.14159265358979);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Float64);
            assertThat(hv.asFloat64()).isEqualTo(3.14159265358979);
        }

        @Test
        void shouldRejectAccessingFloat64AsFloat32() {
            var hv = HeaderValue.fromFloat64(1.0);
            assertThatThrownBy(hv::asFloat32).isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class RawType {

        @Test
        void shouldCreateFromRaw() {
            byte[] data = {1, 2, 3};
            var hv = HeaderValue.fromRaw(data);
            assertThat(hv.kind()).isEqualTo(HeaderKind.Raw);
            assertThat(hv.asRaw()).isEqualTo(data);
        }

        @Test
        void shouldRejectEmptyRaw() {
            assertThatThrownBy(() -> HeaderValue.fromRaw(new byte[0])).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectRawOver255Bytes() {
            assertThatThrownBy(() -> HeaderValue.fromRaw(new byte[256]))
                    .isInstanceOf(IggyInvalidArgumentException.class);
        }
    }

    @Nested
    class ToStringConversion {

        @Test
        void shouldConvertStringToString() {
            assertThat(HeaderValue.fromString("hello").toString()).isEqualTo("hello");
        }

        @Test
        void shouldConvertBoolToString() {
            assertThat(HeaderValue.fromBool(true).toString()).isEqualTo("true");
            assertThat(HeaderValue.fromBool(false).toString()).isEqualTo("false");
        }

        @Test
        void shouldConvertInt8ToString() {
            assertThat(HeaderValue.fromInt8((byte) 42).toString()).isEqualTo("42");
        }

        @Test
        void shouldConvertInt16ToString() {
            assertThat(HeaderValue.fromInt16((short) 1234).toString()).isEqualTo("1234");
        }

        @Test
        void shouldConvertInt32ToString() {
            assertThat(HeaderValue.fromInt32(123456).toString()).isEqualTo("123456");
        }

        @Test
        void shouldConvertInt64ToString() {
            assertThat(HeaderValue.fromInt64(9876543210L).toString()).isEqualTo("9876543210");
        }

        @Test
        void shouldConvertUint8ToString() {
            assertThat(HeaderValue.fromUint8((short) 200).toString()).isEqualTo("200");
        }

        @Test
        void shouldConvertUint16ToString() {
            assertThat(HeaderValue.fromUint16(50000).toString()).isEqualTo("50000");
        }

        @Test
        void shouldConvertUint32ToString() {
            assertThat(HeaderValue.fromUint32(3000000000L).toString()).isEqualTo("3000000000");
        }

        @Test
        void shouldConvertFloat32ToString() {
            assertThat(HeaderValue.fromFloat32(3.14f).toString()).isEqualTo("3.14");
        }

        @Test
        void shouldConvertFloat64ToString() {
            assertThat(HeaderValue.fromFloat64(3.14).toString()).isEqualTo("3.14");
        }

        @Test
        void shouldConvertRawToBase64String() {
            byte[] data = {1, 2, 3};
            var hv = HeaderValue.fromRaw(data);
            assertThat(hv.toString()).isEqualTo("AQID");
        }
    }

    @Nested
    class EqualsAndHashCode {

        @Test
        void shouldBeEqualForSameValues() {
            var a = HeaderValue.fromString("test");
            var b = HeaderValue.fromString("test");
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @Test
        void shouldNotBeEqualForDifferentValues() {
            var a = HeaderValue.fromString("foo");
            var b = HeaderValue.fromString("bar");
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        void shouldNotBeEqualForDifferentKinds() {
            var a = HeaderValue.fromInt32(1);
            var b = HeaderValue.fromInt64(1);
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        void shouldNotBeEqualToNull() {
            var a = HeaderValue.fromString("test");
            assertThat(a).isNotEqualTo(null);
        }

        @Test
        void shouldBeEqualToItself() {
            var a = HeaderValue.fromString("test");
            assertThat(a).isEqualTo(a);
        }
    }
}
