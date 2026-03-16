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

class IggyAuthenticationExceptionTest {
    @Test
    void constructorCreatesExpectedIggyAuthenticationException() {
        var exception = new IggyAuthenticationException(
                IggyErrorCode.UNAUTHENTICATED, 40, "unauthenticated", Optional.of("foo"), Optional.of("bar"));

        assertThat(exception).isInstanceOf(IggyAuthenticationException.class);
        assertThat(exception.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHENTICATED);
        assertThat(exception.getRawErrorCode()).isEqualTo(40);
        assertThat(exception.getReason()).isEqualTo("unauthenticated");
        assertThat(exception.getField()).isEqualTo(Optional.of("foo"));
        assertThat(exception.getErrorId()).isEqualTo(Optional.of("bar"));
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "UNAUTHENTICATED",
                "INVALID_CREDENTIALS",
                "INVALID_USERNAME",
                "INVALID_PASSWORD",
                "INVALID_PAT_TOKEN",
                "PASSWORD_DOES_NOT_MATCH",
                "PASSWORD_HASH_INTERNAL_ERROR"
            })
    void matchesReturnsTrueForAuthenticationRelatedCodes(IggyErrorCode code) {
        assertThat(IggyAuthenticationException.matches(code)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "UNAUTHENTICATED",
                "INVALID_CREDENTIALS",
                "INVALID_USERNAME",
                "INVALID_PASSWORD",
                "INVALID_PAT_TOKEN",
                "PASSWORD_DOES_NOT_MATCH",
                "PASSWORD_HASH_INTERNAL_ERROR"
            },
            mode = Mode.EXCLUDE)
    void matchesReturnsFalseForNonAuthenticationRelatedCodes(IggyErrorCode code) {
        assertThat(IggyAuthenticationException.matches(code)).isFalse();
    }
}
