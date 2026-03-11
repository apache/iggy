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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeaderKeyTest {

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void fromStringThrowsIggyInvalidArgumentExceptionWhenStringIsBlank(String value) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderKey.fromString(value));
    }

    @Test
    void fromStringThrowsIggyInvalidArgumentExceptionWhenStringIsTooLong() {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderKey.fromString("a".repeat(256)));
    }

    @Test
    void fromStringCreatesHeaderKeyWhenValueIsValid() {
        var result = HeaderKey.fromString("foo");

        assertEquals(HeaderKind.String, result.kind());
        assertArrayEquals(new byte[] {102, 111, 111}, result.value());
    }

    @Test
    void equalsReturnsTrueForSameObject() {
        var foo = HeaderKey.fromString("foo");

        assertTrue(foo.equals(foo));
    }

    @Test
    void equalsReturnsFalseForNull() {
        var foo = HeaderKey.fromString("foo");

        assertFalse(foo.equals(null));
    }

    @Test
    void equalsReturnsFalseForDifferentType() {
        var foo = HeaderKey.fromString("foo");

        assertFalse(foo.equals("notaheaderkey"));
    }

    @Test
    void equalsReturnsFalseForHeaderKeyWithDifferentValue() {
        var foo = HeaderKey.fromString("foo");
        var bar = HeaderKey.fromString("bar");

        assertFalse(foo.equals(bar));
    }

    @Test
    void equalsReturnsTrueForEqualObjects() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("foo");

        assertTrue(first.equals(second));
    }

    @Test
    void hashCodeMatchesWhenTwoObjectsAreEqual() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("foo");

        assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenTwoObjectsHaveDifferentValues() {
        var first = HeaderKey.fromString("foo");
        var second = HeaderKey.fromString("bar");

        assertNotEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void hashCodeIsDifferentWhenTwoObjectsHaveDifferentKinds() {
        var first = new HeaderKey(HeaderKind.Int16, "foo".getBytes());
        var second = new HeaderKey(HeaderKind.Int32, "foo".getBytes());

        assertNotEquals(first.hashCode(), second.hashCode());
    }

    @Test
    void toStringReturnsStringRepresentationOfValue() {
        var foo = HeaderKey.fromString("foo");

        assertEquals("foo", foo.toString());
    }
}
