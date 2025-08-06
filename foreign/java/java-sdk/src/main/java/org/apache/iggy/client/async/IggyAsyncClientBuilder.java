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

package org.apache.iggy.client.async;

import org.apache.iggy.client.async.tcp.IggyTcpAsyncClient;
import java.util.concurrent.CompletableFuture;

/**
 * Builder for creating instances of {@link IggyAsyncClient}.
 */
public class IggyAsyncClientBuilder {

    private IggyBaseAsyncClient client;

    /**
     * Sets the base async client to use.
     *
     * @param client the base async client
     * @return this builder
     */
    public IggyAsyncClientBuilder withBaseClient(IggyBaseAsyncClient client) {
        this.client = client;
        return this;
    }

    /**
     * Configures the builder to use a TCP client.
     *
     * @param host the server host
     * @param port the server port
     * @return this builder
     */
    public IggyAsyncClientBuilder withTcpClient(String host, Integer port) {
        IggyTcpAsyncClient tcpClient = new IggyTcpAsyncClient(host, port);
        this.client = tcpClient;
        return this;
    }

    /**
     * Builds a new {@link IggyAsyncClient} instance and connects to the server.
     *
     * @return a CompletableFuture that emits the connected IggyAsyncClient
     * @throws IllegalArgumentException if the base client is not provided
     */
    public CompletableFuture<IggyAsyncClient> buildAndConnect() {
        IggyAsyncClient asyncClient = build();
        if (client instanceof IggyTcpAsyncClient) {
            return ((IggyTcpAsyncClient) client).connect()
                    .thenApply(v -> asyncClient);
        }
        return CompletableFuture.completedFuture(asyncClient);
    }

    /**
     * Builds a new {@link IggyAsyncClient} instance.
     *
     * @return a new IggyAsyncClient
     * @throws IllegalArgumentException if the base client is not provided
     */
    public IggyAsyncClient build() {
        if (client == null) {
            throw new IllegalArgumentException("Base client not provided");
        }
        return new IggyAsyncClient(client);
    }
}
