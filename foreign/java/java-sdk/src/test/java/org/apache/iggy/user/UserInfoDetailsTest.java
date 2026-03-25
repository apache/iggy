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

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class UserInfoDetailsTest {
    @Test
    void constructorWithUserInfoCreatesUserInfoDetailsWithExpectedValues() {
        var userInfo = new UserInfo(123L, BigInteger.TEN, UserStatus.Active, "foo");
        var globalPermissions =
                new GlobalPermissions(true, false, false, false, false, false, false, false, false, false);
        var permissions = Optional.of(new Permissions(globalPermissions, Map.of()));
        var userInfoDetails = new UserInfoDetails(userInfo, permissions);

        assertThat(userInfoDetails.id()).isEqualTo(123L);
        assertThat(userInfoDetails.createdAt()).isEqualTo(BigInteger.TEN);
        assertThat(userInfoDetails.status()).isEqualTo(UserStatus.Active);
        assertThat(userInfoDetails.username()).isEqualTo("foo");
        assertThat(userInfoDetails.permissions()).isEqualTo(permissions);
    }
}
