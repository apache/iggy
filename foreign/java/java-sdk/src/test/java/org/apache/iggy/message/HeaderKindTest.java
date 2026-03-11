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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HeaderKindTest {

    @ParameterizedTest
    @CsvSource({
        "1, Raw",
        "2, String",
        "3, Bool",
        "4, Int8",
        "5, Int16",
        "6, Int32",
        "7, Int64",
        "8, Int128",
        "9, Uint8",
        "10, Uint16",
        "11, Uint32",
        "12, Uint64",
        "13, Uint128",
        "14, Float32",
        "15, Float64",
    })
    void fromCodeReturnsCorrectHeaderKindWhenGivenAValidCode(int code, HeaderKind expectedHeaderKind) {
        assertEquals(expectedHeaderKind, HeaderKind.fromCode(code));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 16, 100})
    void fromCodeThrowsIggyInvalidArgumentExceptionWhenCodeInvalid(int code) {
        assertThrows(IggyInvalidArgumentException.class, () -> HeaderKind.fromCode(code));
    }

    @ParameterizedTest
    @CsvSource({
        "Raw, 1",
        "String, 2",
        "Bool, 3",
        "Int8, 4",
        "Int16, 5",
        "Int32, 6",
        "Int64, 7",
        "Int128, 8",
        "Uint8, 9",
        "Uint16, 10",
        "Uint32, 11",
        "Uint64, 12",
        "Uint128, 13",
        "Float32, 14",
        "Float64, 15",
    })
    void asCodeReturnsCorrectCode(HeaderKind headerKind, int expectedCode) {
        assertEquals(expectedCode, headerKind.asCode());
    }
}
