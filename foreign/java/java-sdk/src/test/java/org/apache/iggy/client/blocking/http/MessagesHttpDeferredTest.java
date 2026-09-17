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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyException;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The HTTP deferred poll against a peer that can misbehave: the route and its
 * query, the local refusal a stateless transport forces, the retry rule, and
 * the body bound a well-behaved server never exercises.
 */
class MessagesHttpDeferredTest {

    private static final String EMPTY_POLL = "{\"partition_id\":0,\"current_offset\":0,\"count\":0,\"messages\":[]}";
    private static final String NOT_ACCEPTED =
            "{\"id\":58,\"code\":\"transient_not_accepted\",\"reason\":\"no admission\"}";
    private static final String NOT_COMMITTED =
            "{\"id\":57,\"code\":\"transient_not_committed\",\"reason\":\"unknown outcome\"}";
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int TRANSIENT_NOT_COMMITTED = 57;

    @Test
    void shouldSendTheDeclaredLimitsToTheDeferredRoute() throws Exception {
        var peer = new FakePeer(exchange -> respond(exchange, 200, EMPTY_POLL));
        try (var server = peer.start();
                var client = client(peer)) {
            var options = DeferredPollOptions.defaults()
                    .withMaxWait(Duration.ofSeconds(2))
                    .withMinCount(4)
                    .withMaxBytes(2048)
                    .withRequestTimeout(Duration.ofSeconds(9));

            PolledMessages polled = poll(client, Consumer.of(3L), options);

            assertThat(polled.messages()).isEmpty();
            assertThat(peer.paths).containsExactly("/streams/1/topics/1/messages/deferred");
            Map<String, String> query = peer.queries.get(0);
            assertThat(Long.parseLong(query.get("wait_us")))
                    .as("both budgets are reduced by the time already spent")
                    .isPositive()
                    .isLessThanOrEqualTo(options.maxWaitMicros());
            assertThat(query.get("min_count")).isEqualTo("4");
            assertThat(query.get("max_bytes")).isEqualTo("2048");
            assertThat(Long.parseLong(query.get("request_timeout_us")))
                    .isPositive()
                    .isLessThanOrEqualTo(options.requestTimeoutMicros());
            assertThat(query.get("consumer_id")).isEqualTo("3");
            assertThat(query.get("partition_id")).isEqualTo("0");
            assertThat(query.get("count")).isEqualTo("10");
            assertThat(query.get("auto_commit")).isEqualTo("false");
        }
    }

    @Test
    void shouldRefuseAGroupConsumerWithoutReachingTheServer() throws Exception {
        var peer = new FakePeer(exchange -> respond(exchange, 200, EMPTY_POLL));
        try (var server = peer.start();
                var client = client(peer)) {

            assertThatThrownBy(() -> poll(client, Consumer.group(3L), DeferredPollOptions.defaults()))
                    .isInstanceOf(IggyOperationNotSupportedException.class);

            assertThat(peer.paths)
                    .as("the poll query has no consumer kind, so a group id would poll the wrong thing")
                    .isEmpty();
        }
    }

    @Test
    void shouldRetryAnExplicitRefusalWithWhatIsLeftOfTheBudget() throws Exception {
        var refusals = new AtomicInteger(2);
        var peer = new FakePeer(exchange -> {
            if (refusals.getAndDecrement() > 0) {
                respond(exchange, SERVICE_UNAVAILABLE, NOT_ACCEPTED);
            } else {
                respond(exchange, 200, EMPTY_POLL);
            }
        });
        try (var server = peer.start();
                var client = client(peer)) {

            poll(client, Consumer.of(3L), DeferredPollOptions.defaults());

            assertThat(peer.queries).hasSize(3);
            long firstWait = Long.parseLong(peer.queries.get(0).get("wait_us"));
            long lastWait = Long.parseLong(peer.queries.get(2).get("wait_us"));
            assertThat(lastWait).isLessThan(firstWait);
        }
    }

    @Test
    void shouldNotReplayAnAmbiguousRefusal() throws Exception {
        var peer = new FakePeer(exchange -> respond(exchange, SERVICE_UNAVAILABLE, NOT_COMMITTED));
        try (var server = peer.start();
                var client = client(peer)) {

            assertThatThrownBy(() -> poll(client, Consumer.of(3L), DeferredPollOptions.defaults()))
                    .isInstanceOf(IggyServerException.class)
                    .extracting(error -> ((IggyServerException) error).getRawErrorCode())
                    .isEqualTo(TRANSIENT_NOT_COMMITTED);
            assertThat(peer.queries)
                    .as("an auto-commit poll that may have been served is never replayed")
                    .hasSize(1);
        }
    }

    @Test
    void shouldRefuseABodyLargerThanTheDeclaredBound() throws Exception {
        var peer = new FakePeer(exchange -> {
            // Chunked, so the bound has to hold while the body is read.
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream body = exchange.getResponseBody()) {
                byte[] chunk = new byte[8 * 1024];
                for (int written = 0; written < 1024 * 1024; written += chunk.length) {
                    body.write(chunk);
                }
            }
        });
        try (var server = peer.start();
                var client = client(peer)) {
            var options = DeferredPollOptions.defaults().withMaxBytes(1024);

            assertThatThrownBy(() -> poll(client, Consumer.of(3L), options))
                    .isInstanceOf(IggyException.class)
                    .hasMessageContaining("bytes");
        }
    }

    private static IggyHttpClient client(FakePeer peer) {
        return new IggyHttpClient("http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + peer.port);
    }

    private static PolledMessages poll(IggyHttpClient client, Consumer consumer, DeferredPollOptions options) {
        return client.messages()
                .pollMessagesDeferred(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.of(0L),
                        consumer,
                        PollingStrategy.first(),
                        10L,
                        false,
                        options);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static final class FakePeer implements HttpHandler {
        private final List<String> paths = new CopyOnWriteArrayList<>();
        private final List<Map<String, String>> queries = new CopyOnWriteArrayList<>();
        private final HttpHandler handler;
        private int port;

        private FakePeer(HttpHandler handler) {
            this.handler = handler;
        }

        private ServerHandle start() throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", this);
            server.start();
            port = server.getAddress().getPort();
            return new ServerHandle(server);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            paths.add(exchange.getRequestURI().getPath());
            queries.add(parseQuery(exchange.getRequestURI().getRawQuery()));
            try {
                handler.handle(exchange);
            } finally {
                exchange.close();
            }
        }

        private static Map<String, String> parseQuery(String rawQuery) {
            Map<String, String> parsed = new HashMap<>();
            if (rawQuery == null) {
                return parsed;
            }
            for (String pair : rawQuery.split("&")) {
                int separator = pair.indexOf('=');
                if (separator > 0) {
                    parsed.put(pair.substring(0, separator), pair.substring(separator + 1));
                }
            }
            return parsed;
        }
    }

    private record ServerHandle(HttpServer server) implements AutoCloseable {
        @Override
        public void close() {
            server.stop(0);
        }
    }
}
