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

class IggyConflictExceptionTest {
    @Test
    void constructorCreatesExpectedIggyConflictException() {
        var exception = new IggyConflictException(
                IggyErrorCode.USER_ALREADY_EXISTS, 46, "userAlreadyExists", Optional.of("foo"), Optional.of("bar"));

        assertThat(exception).isInstanceOf(IggyConflictException.class);
        assertThat(exception.getErrorCode()).isEqualTo(IggyErrorCode.USER_ALREADY_EXISTS);
        assertThat(exception.getRawErrorCode()).isEqualTo(46);
        assertThat(exception.getReason()).isEqualTo("userAlreadyExists");
        assertThat(exception.getField()).isEqualTo(Optional.of("foo"));
        assertThat(exception.getErrorId()).isEqualTo(Optional.of("bar"));
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "USER_ALREADY_EXISTS",
                "CLIENT_ALREADY_EXISTS",
                "STREAM_ALREADY_EXISTS",
                "TOPIC_ALREADY_EXISTS",
                "CONSUMER_GROUP_ALREADY_EXISTS",
                "PAT_NAME_ALREADY_EXISTS"
            })
    void matchesReturnsTrueForConflictRelatedCodes(IggyErrorCode code) {
        assertThat(IggyConflictException.matches(code)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "USER_ALREADY_EXISTS",
                "CLIENT_ALREADY_EXISTS",
                "STREAM_ALREADY_EXISTS",
                "TOPIC_ALREADY_EXISTS",
                "CONSUMER_GROUP_ALREADY_EXISTS",
                "PAT_NAME_ALREADY_EXISTS"
            },
            mode = Mode.EXCLUDE)
    void matchesReturnsFalseForNonConflictRelatedCodes(IggyErrorCode code) {
        assertThat(IggyConflictException.matches(code)).isFalse();
    }
}
