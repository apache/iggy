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

import org.testcontainers.containers.GenericContainer;

import static org.apache.iggy.client.blocking.IntegrationTest.TCP_PORT;

final class TcpClientFactory {

    private TcpClientFactory() {}

    static IggyTcpClient create(GenericContainer<?> iggyServer) {
        if (iggyServer == null) {
            // Server is running externally
            return new IggyTcpClient("127.0.0.1", TCP_PORT);
        }
        String address = iggyServer.getHost();
        Integer port = iggyServer.getMappedPort(TCP_PORT);
        return new IggyTcpClient(address, port);
    }
}
