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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotHttpResponseTest {

    @Test
    void shouldPreserveTheAuthorizationErrorAfterRetryingA503() throws IOException {
        var attempts = new AtomicInteger();
        try (var peer = new SnapshotPeer(exchange -> {
                    if (attempts.incrementAndGet() == 1) {
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        exchange.sendResponseHeaders(503, -1);
                    } else {
                        respond(exchange, 403, false, """
                                {"id":"41","code":"unauthorized","reason":"Snapshot is forbidden"}
                                """);
                    }
                });
                var client = peer.client()) {
            var request = client.preparePostRequest("/snapshot", Map.of());

            assertThatThrownBy(() -> client.executeSnapshot(request))
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            error -> assertThat(error.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHORIZED));
            assertThat(attempts.get()).isEqualTo(2);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldRejectABodyAboveTheLimitAndAcceptABodyAtTheLimit(boolean chunked) throws IOException {
        var attempts = new AtomicInteger();
        try (var peer = new SnapshotPeer(exchange ->
                        respond(exchange, 200, chunked, attempts.incrementAndGet() == 1 ? "12345" : "1234"));
                var client = peer.client()) {
            var oversized = client.preparePostRequest("/snapshot", Map.of());
            var fits = client.preparePostRequest("/snapshot", Map.of());

            assertThatThrownBy(() -> client.executeSnapshot(oversized, 4))
                    .isInstanceOf(IggyMalformedResponseException.class)
                    .hasMessageContaining("limit of 4 bytes");
            assertThat(client.executeSnapshot(fits, 4)).containsExactly("1234".getBytes(StandardCharsets.UTF_8));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {503, 307})
    void shouldRejectOversizedIntermediateBodiesBeforeHttpClientReplaysTheRequest(int status) throws IOException {
        var attempts = new AtomicInteger();
        try (var peer = new SnapshotPeer(exchange -> {
                    if (attempts.incrementAndGet() == 1) {
                        exchange.getResponseHeaders().set("Location", "/snapshot");
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        respond(exchange, status, true, "12345");
                    } else {
                        respond(exchange, 200, false, "1234");
                    }
                });
                var client = peer.client()) {
            var request = client.preparePostRequest("/snapshot", Map.of());

            assertThatThrownBy(() -> client.executeSnapshot(request, 4))
                    .isInstanceOf(IggyMalformedResponseException.class)
                    .hasMessageContaining("limit of 4 bytes");
            assertThat(attempts.get()).isEqualTo(1);
        }
    }

    private static void respond(HttpExchange exchange, int status, boolean chunked, String body) throws IOException {
        var bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, chunked ? 0 : bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static final class SnapshotPeer implements AutoCloseable {
        private final HttpServer server;

        SnapshotPeer(HttpHandler handler) throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/snapshot", exchange -> {
                try {
                    exchange.getRequestBody().readAllBytes();
                    handler.handle(exchange);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        InternalHttpClient client() {
            var timeout = Optional.of(Duration.ofSeconds(5));
            return new InternalHttpClient(
                    "http://127.0.0.1:" + server.getAddress().getPort(), timeout, timeout, Optional.empty());
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
