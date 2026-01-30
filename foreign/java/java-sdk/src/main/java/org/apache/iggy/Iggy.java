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

package org.apache.iggy;

import org.apache.iggy.builder.HttpClientBuilder;
import org.apache.iggy.builder.TcpClientBuilder;

/**
 * Main entry point for creating Iggy clients.
 *
 * <p>Iggy provides a fluent API for creating clients with protocol-first design:
 *
 * <h2>TCP Clients (recommended for performance)</h2>
 * <pre>{@code
 * // Blocking TCP client
 * var client = Iggy.tcpClientBuilder().blocking()
 *     .host("localhost")
 *     .port(8090)
 *     .build();
 * client.connect();
 * client.users().login("iggy", "iggy");
 *
 * // Async TCP client
 * var asyncClient = Iggy.tcpClientBuilder().async()
 *     .host("localhost")
 *     .build();
 * asyncClient.connect().join();
 * asyncClient.users().login("iggy", "iggy").join();
 * }</pre>
 *
 * <h2>HTTP Clients</h2>
 * <pre>{@code
 * var httpClient = Iggy.httpClientBuilder().blocking()
 *     .url("http://localhost:3000")
 *     .build();
 *
 * // Login after creating the client
 * httpClient.users().login("iggy", "iggy");
 * }</pre>
 *
 * <h2>Version Information</h2>
 * <pre>{@code
 * String version = Iggy.version();           // e.g., "1.0.0"
 * IggyVersion info = Iggy.versionInfo();     // Full version details
 * }</pre>
 *
 * @see org.apache.iggy.builder.TcpClientBuilder
 * @see org.apache.iggy.builder.HttpClientBuilder
 * @see IggyVersion
 */
public final class Iggy {

    private Iggy() {}

    /**
     * Creates a builder for TCP clients.
     *
     * <p>TCP provides the best performance and is recommended for most use cases.
     *
     * @return a TCP client builder
     */
    public static TcpClientBuilder tcpClientBuilder() {
        return new TcpClientBuilder();
    }

    /**
     * Creates a builder for HTTP clients.
     *
     * <p>HTTP is useful when TCP is blocked by firewalls or when HTTP semantics are preferred.
     *
     * @return an HTTP client builder
     */
    public static HttpClientBuilder httpClientBuilder() {
        return new HttpClientBuilder();
    }

    /**
     * Returns the SDK version string.
     *
     * @return the version string (e.g., "1.0.0")
     */
    public static String version() {
        return IggyVersion.getInstance().getVersion();
    }

    /**
     * Returns detailed version information.
     *
     * @return the version information object
     */
    public static IggyVersion versionInfo() {
        return IggyVersion.getInstance();
    }
}
