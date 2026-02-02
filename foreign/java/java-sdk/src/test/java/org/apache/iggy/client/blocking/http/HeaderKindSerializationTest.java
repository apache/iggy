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

import org.apache.iggy.message.HeaderKind;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.ObjectMapper;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderKindSerializationTest {

    private final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();

    @Nested
    class Serialization {

        @ParameterizedTest
        @MethodSource("org.apache.iggy.client.blocking.http.HeaderKindSerializationTest#headerKindMappings")
        void shouldSerializeToLowercase(HeaderKind kind, String expectedJson) {
            // given
            // kind provided by parameter

            // when
            String json = objectMapper.writeValueAsString(kind);

            // then
            assertThat(json).isEqualTo("\"" + expectedJson + "\"");
        }
    }

    @Nested
    class Deserialization {

        @ParameterizedTest
        @MethodSource("org.apache.iggy.client.blocking.http.HeaderKindSerializationTest#headerKindMappings")
        void shouldDeserializeFromLowercase(HeaderKind expectedKind, String jsonValue) {
            // given
            String json = "\"" + jsonValue + "\"";

            // when
            HeaderKind result = objectMapper.readValue(json, HeaderKind.class);

            // then
            assertThat(result).isEqualTo(expectedKind);
        }

        @ParameterizedTest
        @MethodSource("org.apache.iggy.client.blocking.http.HeaderKindSerializationTest#headerKindMappings")
        void shouldDeserializeCaseInsensitive(HeaderKind expectedKind, String jsonValue) {
            // given
            String json = "\"" + jsonValue.toUpperCase() + "\"";

            // when
            HeaderKind result = objectMapper.readValue(json, HeaderKind.class);

            // then
            assertThat(result).isEqualTo(expectedKind);
        }
    }

    @Nested
    class Roundtrip {

        @ParameterizedTest
        @MethodSource("org.apache.iggy.client.blocking.http.HeaderKindSerializationTest#headerKindMappings")
        void shouldRoundtripAllHeaderKinds(HeaderKind kind, String ignored) {
            // given
            // kind provided by parameter

            // when
            String json = objectMapper.writeValueAsString(kind);
            HeaderKind result = objectMapper.readValue(json, HeaderKind.class);

            // then
            assertThat(result).isEqualTo(kind);
        }
    }

    static Stream<Arguments> headerKindMappings() {
        return Stream.of(
                Arguments.of(HeaderKind.Raw, "raw"),
                Arguments.of(HeaderKind.String, "string"),
                Arguments.of(HeaderKind.Bool, "bool"),
                Arguments.of(HeaderKind.Int8, "int8"),
                Arguments.of(HeaderKind.Int16, "int16"),
                Arguments.of(HeaderKind.Int32, "int32"),
                Arguments.of(HeaderKind.Int64, "int64"),
                Arguments.of(HeaderKind.Int128, "int128"),
                Arguments.of(HeaderKind.Uint8, "uint8"),
                Arguments.of(HeaderKind.Uint16, "uint16"),
                Arguments.of(HeaderKind.Uint32, "uint32"),
                Arguments.of(HeaderKind.Uint64, "uint64"),
                Arguments.of(HeaderKind.Uint128, "uint128"),
                Arguments.of(HeaderKind.Float32, "float32"),
                Arguments.of(HeaderKind.Float64, "float64"));
    }
}
