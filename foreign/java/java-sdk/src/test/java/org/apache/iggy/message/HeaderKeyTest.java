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
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeaderKeyTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void fromStringThrowsIggyInvalidArgumentExceptionWhenStringIsBlank(String value) {
        assertThatThrownBy(() -> HeaderKey.fromString(value)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromStringThrowsIggyInvalidArgumentExceptionWhenStringIsTooLong() {
        assertThatThrownBy(() -> HeaderKey.fromString("a".repeat(256)))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void fromStringCreatesHeaderKeyWhenValueIsValid() {
        var result = HeaderKey.fromString("foo");

        assertThat(result.kind()).isEqualTo(HeaderKind.String);
        assertThat(result.value()).isEqualTo(new byte[] {102, 111, 111});
    }

    @Test
    void equalsReturnsTrueForSameObject() {
        var foo = HeaderKey.fromString("foo");

        assertThat(foo).isEqualTo(foo);
    }

    @Test
    void equalsReturnsFalseForNull() {
        var foo = HeaderKey.fromString("foo");

        assertThat(foo).isNotEqualTo(null);
    }

    @Test
    void equalsReturnsFalseForDifferentType() {
        var foo = HeaderKey.fromString("foo");

        assertThat(foo).isNotEqualTo("notaheaderkey");
    }

    @Test
    void equalsReturnsFalseForHeaderKeyWithDifferentValue() {
        var foo = HeaderKey.fromString("foo");
        var bar = HeaderKey.fromString("bar");

        assertThat(foo).isNotEqualTo(bar);
    }

    @Test
    void equalsReturnsTrueForEqualObjects() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("foo");

        assertThat(first).isEqualTo(second);
    }

    @Test
    void hashCodeMatchesWhenTwoObjectsAreEqual() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("foo");

        assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenTwoObjectsHaveDifferentValues() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("bar");

        assertThat(first.hashCode()).isNotEqualTo(second.hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenTwoObjectsHaveDifferentKinds() {
        var first = new HeaderKey(HeaderKind.Int16, "foo".getBytes());
        var second = new HeaderKey(HeaderKind.Int32, "foo".getBytes());

        assertThat(first.hashCode()).isNotEqualTo(second.hashCode());
    }

    @Test
    void toStringReturnsStringRepresentationOfValue() {
        var foo = HeaderKey.fromString("foo");

        assertThat(foo.toString()).isEqualTo("foo");
    }
}
