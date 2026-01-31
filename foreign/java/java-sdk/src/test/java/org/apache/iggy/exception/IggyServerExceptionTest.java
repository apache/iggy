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

import java.util.Optional;

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
}
