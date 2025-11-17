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

package org.apache.iggy.client.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.iggy.client.blocking.PersonalAccessTokensClient;
import org.apache.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import org.apache.iggy.personalaccesstoken.RawPersonalAccessToken;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.TokenInfo;

import java.math.BigInteger;
import java.util.List;

class PersonalAccessTokensHttpClient implements PersonalAccessTokensClient {

    private static final String PERSONAL_ACCESS_TOKENS = "/personal-access-tokens";
    private final InternalHttpClient httpClient;

    public PersonalAccessTokensHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public RawPersonalAccessToken createPersonalAccessToken(String name, BigInteger expiry) {
        var request =
                httpClient.preparePostRequest(PERSONAL_ACCESS_TOKENS, new CreatePersonalAccessToken(name, expiry));
        return httpClient.execute(request, new TypeReference<>() {});
    }

    @Override
    public List<PersonalAccessTokenInfo> getPersonalAccessTokens() {
        var request = httpClient.prepareGetRequest(PERSONAL_ACCESS_TOKENS);
        return httpClient.execute(request, new TypeReference<>() {});
    }

    @Override
    public void deletePersonalAccessToken(String name) {
        var request = httpClient.prepareDeleteRequest(PERSONAL_ACCESS_TOKENS + "/" + name);
        httpClient.execute(request);
    }

    @Override
    public IdentityInfo loginWithPersonalAccessToken(String token) {
        var request = httpClient.preparePostRequest(
                PERSONAL_ACCESS_TOKENS + "/login", new LoginWithPersonalAccessToken(token));
        var response = httpClient.execute(request, IdentityInfo.class);
        httpClient.setToken(response.accessToken().map(TokenInfo::token));
        return response;
    }

    record CreatePersonalAccessToken(String name, BigInteger expiry) {}

    record LoginWithPersonalAccessToken(String token) {}
}
