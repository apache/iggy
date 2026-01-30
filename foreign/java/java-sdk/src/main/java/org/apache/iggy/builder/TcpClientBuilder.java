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

package org.apache.iggy.builder;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClientBuilder;

/**
 * Entry point for building TCP clients.
 *
 * <p>Use this builder to choose between blocking and async TCP clients:
 * <pre>{@code
 * // Blocking TCP client - manual connect and login
 * var client = Iggy.tcpClientBuilder().blocking()
 *     .host("localhost")
 *     .port(8090)
 *     .build();
 * client.connect();
 * client.users().login("iggy", "iggy");
 *
 * // Blocking TCP client - convenience method with auto-login
 * var client = Iggy.tcpClientBuilder().blocking()
 *     .host("localhost")
 *     .port(8090)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin();
 *
 * // Async TCP client - manual connect and login
 * var asyncClient = Iggy.tcpClientBuilder().async()
 *     .host("localhost")
 *     .build();
 * asyncClient.connect().join();
 * asyncClient.users().login("iggy", "iggy").join();
 *
 * // Async TCP client - convenience method with auto-login
 * var asyncClient = Iggy.tcpClientBuilder().async()
 *     .host("localhost")
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin()
 *     .join();
 * }</pre>
 *
 * @see IggyTcpClientBuilder
 * @see AsyncIggyTcpClientBuilder
 */
public final class TcpClientBuilder {

    public TcpClientBuilder() {}

    /**
     * Returns a builder for creating a blocking TCP client.
     *
     * @return a new IggyTcpClientBuilder instance
     */
    public IggyTcpClientBuilder blocking() {
        return IggyTcpClient.builder();
    }

    /**
     * Returns a builder for creating an async TCP client.
     *
     * @return a new AsyncIggyTcpClientBuilder instance
     */
    public AsyncIggyTcpClientBuilder async() {
        return AsyncIggyTcpClient.builder();
    }
}
