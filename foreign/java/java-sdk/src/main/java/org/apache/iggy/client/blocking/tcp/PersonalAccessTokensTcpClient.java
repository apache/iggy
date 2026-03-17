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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.PersonalAccessTokensClient;
import org.apache.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import org.apache.iggy.personalaccesstoken.RawPersonalAccessToken;
import org.apache.iggy.user.IdentityInfo;

import java.math.BigInteger;
import java.util.List;

final class PersonalAccessTokensTcpClient implements PersonalAccessTokensClient {

    private final org.apache.iggy.client.async.PersonalAccessTokensClient delegate;

    PersonalAccessTokensTcpClient(org.apache.iggy.client.async.PersonalAccessTokensClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public RawPersonalAccessToken createPersonalAccessToken(String name, BigInteger expiry) {
        return FutureUtil.resolve(delegate.createPersonalAccessToken(name, expiry));
    }

    @Override
    public List<PersonalAccessTokenInfo> getPersonalAccessTokens() {
        return FutureUtil.resolve(delegate.getPersonalAccessTokens());
    }

    @Override
    public void deletePersonalAccessToken(String name) {
        FutureUtil.resolve(delegate.deletePersonalAccessToken(name));
    }

    @Override
    public IdentityInfo loginWithPersonalAccessToken(String token) {
        return FutureUtil.resolve(delegate.loginWithPersonalAccessToken(token));
    }
}
