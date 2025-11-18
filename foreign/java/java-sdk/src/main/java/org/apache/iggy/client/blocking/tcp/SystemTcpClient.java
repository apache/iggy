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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.Stats;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readClientInfo;
import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readClientInfoDetails;
import static org.apache.iggy.client.blocking.tcp.BytesDeserializer.readStats;

class SystemTcpClient implements SystemClient {

    private final InternalTcpClient tcpClient;

    SystemTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Stats getStats() {
        var response = tcpClient.send(CommandCode.System.GET_STATS);
        return readStats(response);
    }

    @Override
    public ClientInfoDetails getMe() {
        var response = tcpClient.send(CommandCode.System.GET_ME);
        return readClientInfoDetails(response);
    }

    @Override
    public ClientInfoDetails getClient(Long clientId) {
        var payload = Unpooled.buffer(4);
        payload.writeIntLE(clientId.intValue());
        var response = tcpClient.send(CommandCode.System.GET_CLIENT, payload);
        return readClientInfoDetails(response);
    }

    @Override
    public List<ClientInfo> getClients() {
        var response = tcpClient.send(CommandCode.System.GET_ALL_CLIENTS);
        List<ClientInfo> clients = new ArrayList<>();
        while (response.isReadable()) {
            clients.add(readClientInfo(response));
        }
        return clients;
    }

    @Override
    public String ping() {
        tcpClient.send(CommandCode.System.PING);
        return "";
    }
}
