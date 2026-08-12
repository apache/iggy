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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IggyValidationExceptionTest {
    @Test
    void constructorCreatesExpectedIggyValidationException() {
        var exception = new IggyValidationException(
                IggyErrorCode.INVALID_COMMAND, 3, "invalidCommand", Optional.of("foo"), Optional.of("bar"));

        assertThat(exception).isInstanceOf(IggyValidationException.class);
        assertThat(exception.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND);
        assertThat(exception.getRawErrorCode()).isEqualTo(3);
        assertThat(exception.getReason()).isEqualTo("invalidCommand");
        assertThat(exception.getField()).isEqualTo(Optional.of("foo"));
        assertThat(exception.getErrorId()).isEqualTo(Optional.of("bar"));
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "INVALID_COMMAND",
                "INVALID_FORMAT",
                "FEATURE_UNAVAILABLE",
                "CANNOT_PARSE_INT",
                "CANNOT_PARSE_SLICE",
                "CANNOT_PARSE_UTF8",
                "INVALID_STREAM_NAME",
                "CANNOT_CREATE_STREAM_DIRECTORY",
                "INVALID_TOPIC_NAME",
                "INVALID_REPLICATION_FACTOR",
                "CANNOT_CREATE_TOPIC_DIRECTORY",
                "CONSUMER_GROUP_MEMBER_NOT_FOUND",
                "INVALID_CONSUMER_GROUP_NAME",
                "TOO_MANY_MESSAGES",
                "EMPTY_MESSAGES",
                "TOO_BIG_MESSAGE",
                "INVALID_MESSAGE_CHECKSUM"
            })
    void matchesReturnsTrueForValidationRelatedCodes(IggyErrorCode code) {
        assertThat(IggyValidationException.matches(code)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "INVALID_COMMAND",
                "INVALID_FORMAT",
                "FEATURE_UNAVAILABLE",
                "CANNOT_PARSE_INT",
                "CANNOT_PARSE_SLICE",
                "CANNOT_PARSE_UTF8",
                "INVALID_STREAM_NAME",
                "CANNOT_CREATE_STREAM_DIRECTORY",
                "INVALID_TOPIC_NAME",
                "INVALID_REPLICATION_FACTOR",
                "CANNOT_CREATE_TOPIC_DIRECTORY",
                "CONSUMER_GROUP_MEMBER_NOT_FOUND",
                "INVALID_CONSUMER_GROUP_NAME",
                "TOO_MANY_MESSAGES",
                "EMPTY_MESSAGES",
                "TOO_BIG_MESSAGE",
                "INVALID_MESSAGE_CHECKSUM"
            },
            mode = Mode.EXCLUDE)
    void matchesReturnsFalseForNonValidationRelatedCodes(IggyErrorCode code) {
        assertThat(IggyValidationException.matches(code)).isFalse();
    }
}
