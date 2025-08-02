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

package org.apache.iggy.client.async.tcp;

import org.apache.iggy.client.async.IggyBaseAsyncClient;
import org.apache.iggy.client.async.MessagesAsyncClient;
import java.util.concurrent.CompletableFuture;

/**
 * TCP implementation of the IggyBaseAsyncClient interface.
 * This class provides access to the asynchronous client components.
 */
public class IggyTcpAsyncClient implements IggyBaseAsyncClient {

    private final MessagesAsyncClient messagesClient;
    private final InternalAsyncTcpClient tcpClient;

    /**
     * Creates a new IggyTcpAsyncClient.
     *
     * @param host the server host
     * @param port the server port
     */
    public IggyTcpAsyncClient(String host, Integer port) {
        this.tcpClient = new InternalAsyncTcpClient(host, port);
        this.messagesClient = new MessagesAsyncTcpClient(tcpClient);
    }

    /**
     * Connects to the server.
     *
     * @return a CompletableFuture that completes when the connection is established
     */
    public CompletableFuture<Void> connect() {
        return tcpClient.connect();
    }

    @Override
    public MessagesAsyncClient messages() {
        return messagesClient;
    }
}
