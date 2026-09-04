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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @CsvSource({
        // General errors
        "1, ERROR",
        "3, INVALID_COMMAND",
        "4, INVALID_FORMAT",
        "5, FEATURE_UNAVAILABLE",
        "6, INVALID_IDENTIFIER",
        "20, RESOURCE_NOT_FOUND",
        "30, STALE_CLIENT",

        // Authentication/Authorization errors
        "40, UNAUTHENTICATED",
        "41, UNAUTHORIZED",
        "42, INVALID_CREDENTIALS",
        "43, INVALID_USERNAME",
        "44, INVALID_PASSWORD",
        "45, INVALID_USER_STATUS",
        "46, USER_ALREADY_EXISTS",
        "47, USER_INACTIVE",
        "48, CANNOT_DELETE_USER",
        "49, CANNOT_CHANGE_PERMISSIONS",
        "50, INVALID_PERSONAL_ACCESS_TOKEN_NAME",
        "51, PERSONAL_ACCESS_TOKEN_ALREADY_EXISTS",
        "52, PERSONAL_ACCESS_TOKENS_LIMIT_REACHED",
        "53, INVALID_PERSONAL_ACCESS_TOKEN",
        "54, PERSONAL_ACCESS_TOKEN_EXPIRED",
        "57, TRANSIENT_NOT_COMMITTED",
        "58, TRANSIENT_NOT_ACCEPTED",
        "77, ACCESS_TOKEN_MISSING",
        "78, INVALID_ACCESS_TOKEN",

        // Wire decoding errors
        "80, INVALID_SIZE_BYTES",
        "81, INVALID_UTF8",
        "82, INVALID_NUMBER_ENCODING",
        "83, INVALID_BOOLEAN_VALUE",
        "84, INVALID_NUMBER_VALUE",

        // Client errors
        "100, CLIENT_NOT_FOUND",

        // Stream errors
        "1001, CANNOT_CREATE_STREAM_DIRECTORY",
        "1009, STREAM_ID_NOT_FOUND",
        "1010, STREAM_NAME_NOT_FOUND",
        "1012, STREAM_NAME_ALREADY_EXISTS",
        "1013, INVALID_STREAM_NAME",
        "1014, INVALID_STREAM_ID",
        "1020, TOO_MANY_STREAMS",

        // Topic errors
        "2001, CANNOT_CREATE_TOPIC_DIRECTORY",
        "2010, TOPIC_ID_NOT_FOUND",
        "2011, TOPIC_NAME_NOT_FOUND",
        "2013, TOPIC_NAME_ALREADY_EXISTS",
        "2014, INVALID_TOPIC_NAME",
        "2015, TOO_MANY_PARTITIONS",
        "2016, INVALID_TOPIC_ID",
        "2018, INVALID_REPLICATION_FACTOR",
        "2021, TOO_MANY_TOPICS",

        // Partition errors
        "3007, PARTITION_NOT_FOUND",
        "3013, PARTITION_ID_SPACE_EXHAUSTED",
        "3024, TOO_MANY_CONSUMER_OFFSETS",

        // Segment errors
        "4000, SEGMENT_NOT_FOUND",
        "4001, SEGMENT_CLOSED",
        "4002, INVALID_SEGMENT_SIZE",
        "4003, CANNOT_CREATE_SEGMENT_LOG_FILE",

        // Message errors
        "4022, TOO_BIG_MESSAGE_PAYLOAD",
        "4023, TOO_MANY_MESSAGES",
        "4024, EMPTY_MESSAGE_PAYLOAD",
        "4027, INVALID_MESSAGE_CHECKSUM",

        // Consumer group errors
        "5000, CONSUMER_GROUP_ID_NOT_FOUND",
        "5002, INVALID_CONSUMER_GROUP_ID",
        "5003, CONSUMER_GROUP_NAME_NOT_FOUND",
        "5004, CONSUMER_GROUP_NAME_ALREADY_EXISTS",
        "5005, INVALID_CONSUMER_GROUP_NAME",
        "5006, CONSUMER_GROUP_MEMBER_NOT_FOUND",

        // VSR protocol errors
        "14003, INCOMPATIBLE_PROTOCOL_VERSION",
    })
    void fromCodeReturnsExpectedIggyErrorCodeWhenCodeIsValid(int code, IggyErrorCode expected) {
        var iggyErrorCode = IggyErrorCode.fromCode(code);

        assertThat(iggyErrorCode).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 123456789, 2099})
    void fromCodeReturnsUnknownIggyErrorCodeWhenCodeIsInvalid(int code) {
        assertThat(IggyErrorCode.fromCode(code)).isEqualTo(IggyErrorCode.UNKNOWN);
    }
}
