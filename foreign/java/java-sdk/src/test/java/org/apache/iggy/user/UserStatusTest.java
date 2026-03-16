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

package org.apache.iggy.user;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UserStatusTest {
    @ParameterizedTest
    @CsvSource({"1, Active", "2, Inactive"})
    void fromCodeReturnsCorrectUserStatusForValidCode(int code, UserStatus expectedUserStatus) {
        assertThat(UserStatus.fromCode(code)).isEqualTo(expectedUserStatus);
    }

    @Test
    void fromCodeThrowsIggyInvalidArgumentExceptionForInvalidCode() {
        assertThatThrownBy(() -> UserStatus.fromCode(100)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"Active, 1", "Inactive, 2"})
    void asCodeReturnsValue(UserStatus userStatus, int expectedCode) {
        assertThat(userStatus.asCode()).isEqualTo(expectedCode);
    }
}
