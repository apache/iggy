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

package org.apache.iggy.exception;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyErrorCodeTest {

    @Nested
    class FromString {

        @Test
        void shouldReturnUnknownForNull() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString(null);

            // then
            assertThat(result).isEqualTo(IggyErrorCode.UNKNOWN);
        }

        @Test
        void shouldReturnUnknownForEmptyString() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.UNKNOWN);
        }

        @Test
        void shouldReturnUnknownForBlankString() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("   ");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.UNKNOWN);
        }

        @Test
        void shouldParseNumericCode() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("1009");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldReturnUnknownForUnknownNumericCode() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("99999");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.UNKNOWN);
        }

        @Test
        void shouldParseUppercaseEnumName() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("STREAM_ID_NOT_FOUND");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldParseLowercaseEnumName() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("stream_id_not_found");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldParseMixedCaseEnumName() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("Stream_Id_Not_Found");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldParseNameWithDots() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("stream.id.not.found");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldParseNameWithSpaces() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("stream id not found");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.STREAM_ID_NOT_FOUND);
        }

        @Test
        void shouldReturnUnknownForInvalidEnumName() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("not_a_valid_error_code");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.UNKNOWN);
        }

        @Test
        void shouldParseSimpleErrorCode() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("error");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.ERROR);
        }

        @Test
        void shouldParseCodeOne() {
            // when
            IggyErrorCode result = IggyErrorCode.fromString("1");

            // then
            assertThat(result).isEqualTo(IggyErrorCode.ERROR);
        }
    }
}
