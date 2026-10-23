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

import org.apache.iggy.message.HeaderValue;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

/** A user with its permissions. See {@link UserInfo} for what {@code options} and {@code derivedOptions} carry. */
public record UserInfoDetails(
        Long id,
        BigInteger createdAt,
        UserStatus status,
        String username,
        Optional<Permissions> permissions,
        Map<String, HeaderValue> options,
        Map<String, HeaderValue> derivedOptions) {
    public UserInfoDetails {
        options = options == null ? Map.of() : options;
        derivedOptions = derivedOptions == null ? Map.of() : derivedOptions;
    }

    public UserInfoDetails(UserInfo userInfo, Optional<Permissions> permissions) {
        this(
                userInfo.id(),
                userInfo.createdAt(),
                userInfo.status(),
                userInfo.username(),
                permissions,
                userInfo.options(),
                userInfo.derivedOptions());
    }
}
