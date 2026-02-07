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

class HeaderKeyTest {

    @Nested
    class FromString {

        @Test
        void shouldCreateFromValidString() {
            var key = HeaderKey.fromString("my-key");
            assertThat(key.kind()).isEqualTo(HeaderKind.String);
            assertThat(key.toString()).isEqualTo("my-key");
        }

        @Test
        void shouldRejectNullString() {
            assertThatThrownBy(() -> HeaderKey.fromString(null)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectEmptyString() {
            assertThatThrownBy(() -> HeaderKey.fromString("")).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectBlankString() {
            assertThatThrownBy(() -> HeaderKey.fromString("   ")).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldRejectStringOver255Bytes() {
            String longString = "a".repeat(256);
            assertThatThrownBy(() -> HeaderKey.fromString(longString)).isInstanceOf(IggyInvalidArgumentException.class);
        }

        @Test
        void shouldAcceptStringAt255Bytes() {
            String maxString = "a".repeat(255);
            var key = HeaderKey.fromString(maxString);
            assertThat(key.toString()).isEqualTo(maxString);
        }
    }

    @Nested
    class EqualsAndHashCode {

        @Test
        void shouldBeEqualForSameValues() {
            var a = HeaderKey.fromString("key");
            var b = HeaderKey.fromString("key");
            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @Test
        void shouldNotBeEqualForDifferentValues() {
            var a = HeaderKey.fromString("key1");
            var b = HeaderKey.fromString("key2");
            assertThat(a).isNotEqualTo(b);
        }

        @Test
        void shouldNotBeEqualToNull() {
            var a = HeaderKey.fromString("key");
            assertThat(a).isNotEqualTo(null);
        }

        @Test
        void shouldBeEqualToItself() {
            var a = HeaderKey.fromString("key");
            assertThat(a).isEqualTo(a);
        }
    }

    @Nested
    class ToStringConversion {

        @Test
        void shouldConvertToUtf8String() {
            var key = HeaderKey.fromString("hello");
            assertThat(key.toString()).isEqualTo("hello");
        }

        @Test
        void shouldHandleUtf8Characters() {
            var key = HeaderKey.fromString("hello世界");
            assertThat(key.toString()).isEqualTo("hello世界");
        }
    }
}
