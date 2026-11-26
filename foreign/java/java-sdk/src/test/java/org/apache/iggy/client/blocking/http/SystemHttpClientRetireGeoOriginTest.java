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
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SystemHttpClientRetireGeoOriginTest {

    @Test
    void shouldPostAnEmptyObjectToTheRetireRouteOfTheOrigin() throws IOException {
        try (var server = new RecordingServer();
                var client = new IggyHttpClient(server.url())) {
            client.system().retireGeoOrigin("dc-a");
            client.system().retireGeoOrigin("dc a/b");

            assertThat(server.requests)
                    .containsExactly("POST /geo/origins/dc-a/retire {}", "POST /geo/origins/dc%20a%2Fb/retire {}");
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {".", "..", "\uD800"})
    void shouldRefuseAnOriginNoPathSegmentCarriesWithoutSendingARequest(String origin) throws IOException {
        try (var server = new RecordingServer();
                var client = new IggyHttpClient(server.url())) {
            assertThatThrownBy(() -> client.system().retireGeoOrigin(origin))
                    .isInstanceOf(IggyInvalidArgumentException.class);

            assertThat(server.requests).isEmpty();
        }
    }

    private static final class RecordingServer implements AutoCloseable {
        private final HttpServer server;
        private final List<String> requests = new CopyOnWriteArrayList<>();

        RecordingServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                requests.add(exchange.getRequestMethod() + " "
                        + exchange.getRequestURI().getRawPath() + " " + body);
                exchange.sendResponseHeaders(204, -1);
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
