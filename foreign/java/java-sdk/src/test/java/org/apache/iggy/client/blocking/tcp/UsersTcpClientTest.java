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

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.UsersClient;
import org.apache.iggy.client.blocking.UsersClientBaseTest;
import org.apache.iggy.exception.IggyAuthenticationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UsersTcpClientTest extends UsersClientBaseTest {

    protected UsersClient usersClient;

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    @BeforeEach
    void beforeEach() {
        usersClient = client.users();
    }

    @Test
    void shouldRejectInvalidPasswordAndAllowAnotherLoginOnTheSameClient() {
        assertThatThrownBy(() -> usersClient.login("iggy", "invalid-password"))
                .isInstanceOf(IggyAuthenticationException.class);
        assertThat(usersClient.login("iggy", "iggy").userId()).isEqualTo(0L);
    }
}
