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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class IggyServerExceptionTest {

    @Nested
    class BuildMessage {

        @Test
        void shouldBuildMessageWithKnownErrorCode() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.STREAM_ID_NOT_FOUND, 1009, "Stream not found", Optional.empty(), Optional.empty());

            // then
            assertThat(exception.getMessage())
                    .isEqualTo("Server error [code=1009 (STREAM_ID_NOT_FOUND)]: Stream not found");
        }

        @Test
        void shouldBuildMessageWithUnknownErrorCode() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.UNKNOWN, 99999, "Unknown error", Optional.empty(), Optional.empty());

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=99999]: Unknown error");
        }

        @Test
        void shouldBuildMessageWithField() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.INVALID_STREAM_NAME,
                    1013,
                    "Invalid stream name",
                    Optional.of("name"),
                    Optional.empty());

            // then
            assertThat(exception.getMessage())
                    .isEqualTo("Server error [code=1013 (INVALID_STREAM_NAME)]: Invalid stream name (field: name)");
        }

        @Test
        void shouldBuildMessageWithErrorId() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.UNAUTHENTICATED, 40, "Not authenticated", Optional.empty(), Optional.of("abc-123"));

            // then
            assertThat(exception.getMessage())
                    .isEqualTo("Server error [code=40 (UNAUTHENTICATED)]: Not authenticated [errorId: abc-123]");
        }

        @Test
        void shouldBuildMessageWithFieldAndErrorId() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.INVALID_PASSWORD,
                    44,
                    "Password too short",
                    Optional.of("password"),
                    Optional.of("xyz-789"));

            // then
            assertThat(exception.getMessage())
                    .isEqualTo(
                            "Server error [code=44 (INVALID_PASSWORD)]: Password too short (field: password) [errorId: xyz-789]");
        }

        @Test
        void shouldBuildMessageWithRawCodeConstructor() {
            // given
            IggyServerException exception = new IggyServerException(1009);

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=1009 (STREAM_ID_NOT_FOUND)]");
        }

        @Test
        void shouldBuildMessageWithUnknownRawCode() {
            // given
            IggyServerException exception = new IggyServerException(88888);

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=88888]");
        }

        @Test
        void shouldBuildMessageWithEmptyReason() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.STREAM_ID_NOT_FOUND, 1009, "", Optional.empty(), Optional.empty());

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=1009 (STREAM_ID_NOT_FOUND)]");
        }

        @Test
        void shouldBuildMessageWithBlankReason() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.TOPIC_ID_NOT_FOUND, 2010, "   ", Optional.empty(), Optional.empty());

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=2010 (TOPIC_ID_NOT_FOUND)]");
        }

        @Test
        void shouldBuildMessageWithEmptyReasonAndField() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.INVALID_STREAM_NAME, 1013, "", Optional.of("name"), Optional.empty());

            // then
            assertThat(exception.getMessage())
                    .isEqualTo("Server error [code=1013 (INVALID_STREAM_NAME)] (field: name)");
        }

        @Test
        void shouldBuildMessageWithEmptyReasonAndErrorId() {
            // given
            IggyServerException exception = new IggyServerException(
                    IggyErrorCode.UNAUTHENTICATED, 40, "", Optional.empty(), Optional.of("abc-123"));

            // then
            assertThat(exception.getMessage()).isEqualTo("Server error [code=40 (UNAUTHENTICATED)] [errorId: abc-123]");
        }
    }

    @Test
    void getErrorCodeReturnsExpectedErrorCode() {
        var first = new IggyServerException(
                IggyErrorCode.UNAUTHORIZED, 41, "reason", Optional.of("foo"), Optional.of("bar"));
        var second = new IggyServerException(41);

        assertThat(first.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHORIZED);
        assertThat(second.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHORIZED);
    }

    @Test
    void getRawErrorCodeReturnsExpectedRawErrorCode() {
        var first = new IggyServerException(
                IggyErrorCode.UNAUTHENTICATED, 40, "reason", Optional.of("foo"), Optional.of("bar"));
        var second = new IggyServerException(40);

        assertThat(first.getRawErrorCode()).isEqualTo(40);
        assertThat(second.getRawErrorCode()).isEqualTo(40);
    }

    @Test
    void getReasonReturnsExpectedReason() {
        var first = new IggyServerException(IggyErrorCode.ERROR, 1, "reason", Optional.of("foo"), Optional.of("bar"));
        var second = new IggyServerException(1);

        assertThat(first.getReason()).isEqualTo("reason");
        assertThat(second.getReason()).isEqualTo("");
    }

    @Test
    void getFieldReturnsExpectedField() {
        var first = new IggyServerException(IggyErrorCode.ERROR, 1, "reason", Optional.of("foo"), Optional.of("bar"));
        var second = new IggyServerException(1);

        assertThat(first.getField()).isEqualTo(Optional.of("foo"));
        assertThat(second.getField()).isEqualTo(Optional.empty());
    }

    @Test
    void getErrorIdReturnsExpectedField() {
        var first = new IggyServerException(IggyErrorCode.ERROR, 1, "reason", Optional.of("foo"), Optional.of("bar"));
        var second = new IggyServerException(1);

        assertThat(first.getErrorId()).isEqualTo(Optional.of("bar"));
        assertThat(second.getErrorId()).isEqualTo(Optional.empty());
    }

    public static Stream<Arguments> fromTcpResponseArgumentProvider() {
        Optional<String> empty = Optional.empty();

        return Stream.of(
                Arguments.of(
                        1,
                        null,
                        IggyServerException.class,
                        new IggyServerException(IggyErrorCode.ERROR, 1, "Server error", empty, empty)),
                Arguments.of(
                        20,
                        new byte[] {102, 111, 111},
                        IggyResourceNotFoundException.class,
                        new IggyResourceNotFoundException(IggyErrorCode.RESOURCE_NOT_FOUND, 20, "foo", empty, empty)),
                Arguments.of(
                        40,
                        new byte[] {102, 111, 111},
                        IggyAuthenticationException.class,
                        new IggyAuthenticationException(IggyErrorCode.UNAUTHENTICATED, 40, "foo", empty, empty)),
                Arguments.of(
                        41,
                        new byte[] {102, 111, 111},
                        IggyAuthorizationException.class,
                        new IggyAuthorizationException(IggyErrorCode.UNAUTHORIZED, 41, "foo", empty, empty)),
                Arguments.of(
                        46,
                        new byte[] {102, 111, 111},
                        IggyConflictException.class,
                        new IggyConflictException(IggyErrorCode.USER_ALREADY_EXISTS, 46, "foo", empty, empty)),
                Arguments.of(
                        3,
                        new byte[] {102, 111, 111},
                        IggyValidationException.class,
                        new IggyValidationException(IggyErrorCode.INVALID_COMMAND, 3, "foo", empty, empty)),
                Arguments.of(
                        12345678,
                        new byte[] {102, 111, 111},
                        IggyServerException.class,
                        new IggyServerException(IggyErrorCode.UNKNOWN, 12345678, "foo", empty, empty)));
    }

    @ParameterizedTest
    @MethodSource("fromTcpResponseArgumentProvider")
    <T extends IggyServerException> void fromTcpResponseReturnsExpectedException(
            long status, byte[] payload, Class<T> expectedExceptionClass, T expectedException) {
        var exception = IggyServerException.fromTcpResponse(status, payload);

        assertThat(exception).isInstanceOf(expectedExceptionClass);
        assertThat(exception.getRawErrorCode()).isEqualTo(expectedException.getRawErrorCode());
        assertThat(exception.getErrorCode()).isEqualTo(expectedException.getErrorCode());
        assertThat(exception.getReason()).isEqualTo(expectedException.getReason());
        assertThat(exception.getField()).isEqualTo(expectedException.getField());
        assertThat(exception.getErrorId()).isEqualTo(expectedException.getErrorId());
        assertThat(exception.getMessage()).isEqualTo(expectedException.getMessage());
    }

    public static Stream<Arguments> fromHttpResponseArgumentProvider() {
        return Stream.of(
                Arguments.of(
                        "id",
                        "1",
                        "error",
                        "fld",
                        IggyServerException.class,
                        new IggyServerException(
                                IggyErrorCode.ERROR, 1, "error", Optional.of("fld"), Optional.of("id"))),
                Arguments.of(
                        "id",
                        "20",
                        "resourceNotFound",
                        "fld",
                        IggyResourceNotFoundException.class,
                        new IggyResourceNotFoundException(
                                IggyErrorCode.RESOURCE_NOT_FOUND,
                                20,
                                "resourceNotFound",
                                Optional.of("fld"),
                                Optional.of("id"))),
                Arguments.of(
                        "id",
                        "40",
                        "unauthenticated",
                        "fld",
                        IggyAuthenticationException.class,
                        new IggyAuthenticationException(
                                IggyErrorCode.UNAUTHENTICATED,
                                40,
                                "unauthenticated",
                                Optional.of("fld"),
                                Optional.of("id"))),
                Arguments.of(
                        "id",
                        "41",
                        "unauthorized",
                        "fld",
                        IggyAuthorizationException.class,
                        new IggyAuthorizationException(
                                IggyErrorCode.UNAUTHORIZED, 41, "unauthorized", Optional.of("fld"), Optional.of("id"))),
                Arguments.of(
                        "id",
                        "46",
                        "userAlreadyExists",
                        "fld",
                        IggyConflictException.class,
                        new IggyConflictException(
                                IggyErrorCode.USER_ALREADY_EXISTS,
                                46,
                                "userAlreadyExists",
                                Optional.of("fld"),
                                Optional.of("id"))),
                Arguments.of(
                        "id",
                        "3",
                        "invalidCommand",
                        "fld",
                        IggyValidationException.class,
                        new IggyValidationException(
                                IggyErrorCode.INVALID_COMMAND,
                                3,
                                "invalidCommand",
                                Optional.of("fld"),
                                Optional.of("id"))),
                Arguments.of(
                        "id",
                        "123456789",
                        "unknown",
                        "fld",
                        IggyServerException.class,
                        new IggyServerException(
                                IggyErrorCode.UNKNOWN, 123456789, "unknown", Optional.of("fld"), Optional.of("id"))));

        //
    }

    @ParameterizedTest
    @MethodSource("fromHttpResponseArgumentProvider")
    <T extends IggyServerException> void fromHttpResponseReturnsExpectedException(
            String id, String code, String reason, String field, Class<T> expectedExceptionClass, T expectedException) {
        var exception = IggyServerException.fromHttpResponse(id, code, reason, field);

        assertThat(exception).isInstanceOf(expectedExceptionClass);
        assertThat(exception.getRawErrorCode()).isEqualTo(expectedException.getRawErrorCode());
        assertThat(exception.getErrorCode()).isEqualTo(expectedException.getErrorCode());
        assertThat(exception.getReason()).isEqualTo(expectedException.getReason());
        assertThat(exception.getField()).isEqualTo(expectedException.getField());
        assertThat(exception.getErrorId()).isEqualTo(expectedException.getErrorId());
        assertThat(exception.getMessage()).isEqualTo(expectedException.getMessage());
    }
}
