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
import org.apache.iggy.client.blocking.SystemClientBaseTest;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SystemTcpClientTest extends SystemClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
    }

    @Test
    void shouldGetMeAndClient() {
        // when
        ClientInfoDetails me = systemClient.getMe();

        // then
        assertThat(me).isNotNull();

        // when
        var clientInfo = systemClient.getClient(me.clientId());

        // then
        assertThat(clientInfo).isNotNull();
    }

    @Test
    void shouldGetClients() {
        // when
        List<ClientInfo> clients = systemClient.getClients();

        // then
        assertThat(clients).isNotNull();
        assertThat(clients.size()).isGreaterThanOrEqualTo(1); // At least our connection
    }
}
