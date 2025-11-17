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

package org.apache.iggy.client.blocking;

import org.apache.iggy.user.IdentityInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class PersonalAccessTokensBaseTest extends IntegrationTest {

    protected PersonalAccessTokensClient personalAccessTokensClient;

    @BeforeEach
    void beforeEachBase() {
        personalAccessTokensClient = client.personalAccessTokens();

        login();

        // Clean up any existing tokens before test
        var existingTokens = personalAccessTokensClient.getPersonalAccessTokens();
        for (var token : existingTokens) {
            try {
                personalAccessTokensClient.deletePersonalAccessToken(token.name());
            } catch (RuntimeException e) {
                // Ignore if already deleted
            }
        }
    }

    @Test
    void shouldManagePersonalAccessTokens() {
        // when
        var createdToken =
                personalAccessTokensClient.createPersonalAccessToken("new-token", BigInteger.valueOf(50_000));

        // then
        assertThat(createdToken).isNotNull();

        // when
        var tokens = personalAccessTokensClient.getPersonalAccessTokens();

        // then
        assertThat(tokens).isNotNull();
        assertThat(tokens).hasSize(1);

        // when
        personalAccessTokensClient.deletePersonalAccessToken("new-token");
        tokens = personalAccessTokensClient.getPersonalAccessTokens();

        //
        assertThat(tokens).hasSize(0);
    }

    @Test
    void shouldCreateAndLogInWithPersonalAccessToken() {
        // given
        var createdToken =
                personalAccessTokensClient.createPersonalAccessToken("new-token", BigInteger.valueOf(50_000));
        client.users().logout();

        // when
        IdentityInfo identityInfo = personalAccessTokensClient.loginWithPersonalAccessToken(createdToken.token());

        // then
        assertThat(identityInfo).isNotNull();

        // when
        var user = client.users().getUser(0L);

        // then
        assertThat(user).isPresent();

        // cleanup
        personalAccessTokensClient.deletePersonalAccessToken("new-token");
    }
}
