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

class IggyResourceNotFoundExceptionTest {
    @Test
    void constructorCreatesExpectedIggyResourceNotFoundException() {
        var exception = new IggyResourceNotFoundException(
                IggyErrorCode.RESOURCE_NOT_FOUND, 20, "resourceNotFound", Optional.of("foo"), Optional.of("bar"));

        assertThat(exception).isInstanceOf(IggyResourceNotFoundException.class);
        assertThat(exception.getErrorCode()).isEqualTo(IggyErrorCode.RESOURCE_NOT_FOUND);
        assertThat(exception.getRawErrorCode()).isEqualTo(20);
        assertThat(exception.getReason()).isEqualTo("resourceNotFound");
        assertThat(exception.getField()).isEqualTo(Optional.of("foo"));
        assertThat(exception.getErrorId()).isEqualTo(Optional.of("bar"));
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "RESOURCE_NOT_FOUND",
                "CANNOT_LOAD_RESOURCE",
                "STREAM_ID_NOT_FOUND",
                "STREAM_NAME_NOT_FOUND",
                "TOPIC_ID_NOT_FOUND",
                "TOPIC_NAME_NOT_FOUND",
                "PARTITION_NOT_FOUND",
                "SEGMENT_NOT_FOUND",
                "CLIENT_NOT_FOUND",
                "CONSUMER_GROUP_ID_NOT_FOUND",
                "CONSUMER_GROUP_NAME_NOT_FOUND",
                "CONSUMER_GROUP_NOT_JOINED",
                "MESSAGE_NOT_FOUND"
            })
    void matchesReturnsTrueForResourceNotFoundRelatedCodes(IggyErrorCode code) {
        assertThat(IggyResourceNotFoundException.matches(code)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "RESOURCE_NOT_FOUND",
                "CANNOT_LOAD_RESOURCE",
                "STREAM_ID_NOT_FOUND",
                "STREAM_NAME_NOT_FOUND",
                "TOPIC_ID_NOT_FOUND",
                "TOPIC_NAME_NOT_FOUND",
                "PARTITION_NOT_FOUND",
                "SEGMENT_NOT_FOUND",
                "CLIENT_NOT_FOUND",
                "CONSUMER_GROUP_ID_NOT_FOUND",
                "CONSUMER_GROUP_NAME_NOT_FOUND",
                "CONSUMER_GROUP_NOT_JOINED",
                "MESSAGE_NOT_FOUND"
            },
            mode = Mode.EXCLUDE)
    void matchesReturnsFalseForNonResourceNotFoundRelatedCodes(IggyErrorCode code) {
        assertThat(IggyResourceNotFoundException.matches(code)).isFalse();
    }
}
