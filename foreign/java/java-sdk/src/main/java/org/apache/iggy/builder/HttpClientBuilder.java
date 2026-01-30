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

import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.client.blocking.http.IggyHttpClientBuilder;

/**
 * Entry point for building HTTP clients.
 *
 * <p>Use this builder to create blocking HTTP clients:
 * <pre>{@code
 * // HTTP client - manual login
 * var client = Iggy.httpClientBuilder().blocking()
 *     .url("http://localhost:3000")
 *     .build();
 * client.users().login("iggy", "iggy");
 *
 * // HTTP client - convenience method with auto-login
 * var client = Iggy.httpClientBuilder().blocking()
 *     .host("localhost")
 *     .port(3000)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin();
 *
 * // HTTPS client with auto-login
 * var secureClient = Iggy.httpClientBuilder().blocking()
 *     .host("iggy-server.example.com")
 *     .port(443)
 *     .enableTls()
 *     .credentials("admin", "secret")
 *     .buildAndLogin();
 * }</pre>
 *
 * @see IggyHttpClientBuilder
 */
public final class HttpClientBuilder {

    public HttpClientBuilder() {}

    /**
     * Returns a builder for creating a blocking HTTP client.
     *
     * @return a new IggyHttpClientBuilder instance
     */
    public IggyHttpClientBuilder blocking() {
        return IggyHttpClient.builder();
    }
}
