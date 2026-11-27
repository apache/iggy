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

package org.apache.iggy.client.blocking.http;

import com.sun.net.httpserver.HttpServer;
import org.apache.iggy.exception.IggyAuthenticationException;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UsersHttpClientLogoutTest {

    private static final String UNAUTHENTICATED =
            "{\"id\":40,\"code\":\"unauthenticated\",\"reason\":\"Unauthenticated\",\"field\":null}";
    private static final String SERVER_ERROR = "{\"id\":1,\"code\":\"error\",\"reason\":\"Error\",\"field\":null}";

    @Test
    void shouldTreatARefusedTokenAsLoggedOutAndDropIt() throws IOException {
        try (var server = new ScriptedServer(401, UNAUTHENTICATED);
                var client = new IggyHttpClient(server.url())) {
            client.users().login("user", "secret");

            client.users().logout();

            assertThat(server.logoutBearers).containsExactly("Bearer token");
            assertThatThrownBy(() -> client.users().logout()).isInstanceOf(IggyAuthenticationException.class);
            assertThat(server.logoutBearers).containsExactly("Bearer token", "");
        }
    }

    @Test
    void shouldKeepTheTokenWhenTheServerFails() throws IOException {
        try (var server = new ScriptedServer(500, SERVER_ERROR);
                var client = new IggyHttpClient(server.url())) {
            client.users().login("user", "secret");

            assertThatThrownBy(() -> client.users().logout()).isInstanceOf(IggyServerException.class);
            assertThatThrownBy(() -> client.users().logout()).isInstanceOf(IggyServerException.class);

            assertThat(server.logoutBearers).containsExactly("Bearer token", "Bearer token");
        }
    }

    /** Signs every login in with one token and answers every logout with the scripted status. */
    private static final class ScriptedServer implements AutoCloseable {
        private static final String IDENTITY = "{\"user_id\":1,\"access_token\":{\"token\":\"token\",\"expiry\":1}}";

        private final HttpServer server;
        private final List<String> logoutBearers = new CopyOnWriteArrayList<>();

        ScriptedServer(int logoutStatus, String logoutBody) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", exchange -> {
                var logout = exchange.getRequestURI().getRawPath().endsWith("/logout");
                if (logout) {
                    var bearer = exchange.getRequestHeaders().getFirst("Authorization");
                    logoutBearers.add(bearer == null ? "" : bearer);
                }
                var body = (logout ? logoutBody : IDENTITY).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(logout ? logoutStatus : 200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
        }

        String url() {
            return "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":"
                    + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
