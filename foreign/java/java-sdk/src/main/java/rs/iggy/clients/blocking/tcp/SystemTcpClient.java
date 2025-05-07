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

package rs.iggy.clients.blocking.tcp;

import io.netty.buffer.Unpooled;
import rs.iggy.clients.blocking.SystemClient;
import rs.iggy.system.ClientInfo;
import rs.iggy.system.ClientInfoDetails;
import rs.iggy.system.Stats;
import java.util.ArrayList;
import java.util.List;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.*;

class SystemTcpClient implements SystemClient {
    private static final int PING_CODE = 1;
    private static final int GET_STATS_CODE = 10;
    private static final int GET_ME_CODE = 20;
    private static final int GET_CLIENT_CODE = 21;
    private static final int GET_CLIENTS_CODE = 22;

    private final InternalTcpClient tcpClient;

    SystemTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Stats getStats() {
        var response = tcpClient.send(GET_STATS_CODE);
        return readStats(response);
    }

    @Override
    public ClientInfoDetails getMe() {
        var response = tcpClient.send(GET_ME_CODE);
        return readClientInfoDetails(response);
    }

    @Override
    public ClientInfoDetails getClient(Long clientId) {
        var payload = Unpooled.buffer(4);
        payload.writeIntLE(clientId.intValue());
        var response = tcpClient.send(GET_CLIENT_CODE, payload);
        return readClientInfoDetails(response);
    }

    @Override
    public List<ClientInfo> getClients() {
        var response = tcpClient.send(GET_CLIENTS_CODE);
        List<ClientInfo> clients = new ArrayList<>();
        while (response.isReadable()) {
            clients.add(readClientInfo(response));
        }
        return clients;
    }

    @Override
    public String ping() {
        tcpClient.send(PING_CODE);
        return "";
    }
}
