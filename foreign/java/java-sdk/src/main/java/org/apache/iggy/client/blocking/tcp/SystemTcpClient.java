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

import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.Stats;

import java.util.List;

final class SystemTcpClient implements SystemClient {

    private final org.apache.iggy.client.async.SystemClient delegate;

    SystemTcpClient(org.apache.iggy.client.async.SystemClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Stats getStats() {
        return FutureUtil.resolve(delegate.getStats());
    }

    @Override
    public ClientInfoDetails getMe() {
        return FutureUtil.resolve(delegate.getMe());
    }

    @Override
    public ClientInfoDetails getClient(Long clientId) {
        return FutureUtil.resolve(delegate.getClient(clientId));
    }

    @Override
    public List<ClientInfo> getClients() {
        return FutureUtil.resolve(delegate.getClients());
    }

    @Override
    public String ping() {
        return FutureUtil.resolve(delegate.ping());
    }
}
