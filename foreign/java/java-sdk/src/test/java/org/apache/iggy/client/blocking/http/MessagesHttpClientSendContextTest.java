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
import com.sun.net.httpserver.HttpServer;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MessagesHttpClientSendContextTest {

    private static final ObjectMapper MAPPER = ObjectMapperFactory.getInstance();
    private static final StreamId STREAM = StreamId.of(1L);
    private static final TopicId TOPIC = TopicId.of(2L);
    private static final String GET_TOPIC = "GET /streams/1/topics/2";
    private static final String POST_MESSAGES = "POST /streams/1/topics/2/messages";
    private static final String POLL_MESSAGES = "GET /streams/1/topics/2/messages";
    private static final int HISTORY_UNAVAILABLE = 87;
    private static final int UNAUTHORIZED = 41;

    @Test
    void shouldFenceOnlyExplicitPartitionSendsWithTheContextTheTopicDetailsReport() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            send(client, Partitioning.partitionId(3L));
            send(client, Partitioning.partitionId(1L));
            send(client, Partitioning.balanced());
            send(client, Partitioning.messagesKey("key"));

            assertThat(server.requests)
                    .containsExactly(GET_TOPIC, POST_MESSAGES, POST_MESSAGES, POST_MESSAGES, POST_MESSAGES);
            assertThat(server.sends.get(0).get("context")).isEqualTo(context(103));
            assertThat(server.sends.get(1).get("context")).isEqualTo(context(101));
            assertThat(server.sends.get(2).has("context")).isFalse();
            assertThat(server.sends.get(3).has("context")).isFalse();
        }
    }

    @Test
    void shouldResendOnceUnderTheContextReadAgainWhenThePartitionWasRecreated() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            send(client, Partitioning.partitionId(3L));
            server.recreatePartitions();

            send(client, Partitioning.partitionId(3L));

            assertThat(server.requests)
                    .containsExactly(GET_TOPIC, POST_MESSAGES, POST_MESSAGES, GET_TOPIC, POST_MESSAGES);
            assertThat(server.sends.get(1).get("context")).isEqualTo(context(103));
            assertThat(server.sends.get(2).get("context")).isEqualTo(context(203));
        }
    }

    @Test
    void shouldReturnHistoryUnavailableWhenTheResendIsRefusedToo() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            server.recreateAfterEachRead = true;

            assertThatThrownBy(() -> send(client, Partitioning.partitionId(3L)))
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            error -> assertThat(error.getRawErrorCode()).isEqualTo(HISTORY_UNAVAILABLE));
            server.recreateAfterEachRead = false;
            send(client, Partitioning.partitionId(3L));

            assertThat(server.requests)
                    .containsExactly(GET_TOPIC, POST_MESSAGES, GET_TOPIC, POST_MESSAGES, GET_TOPIC, POST_MESSAGES);
            assertThat(server.sends.get(2).get("context")).isEqualTo(context(303));
        }
    }

    @ParameterizedTest
    @EnumSource(LayoutChange.class)
    void shouldReadTheTopicAgainAfterThisClientChangesALayout(LayoutChange change) throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            send(client, Partitioning.partitionId(3L));
            server.recreatePartitions();

            change.apply(client);
            send(client, Partitioning.partitionId(3L));

            assertThat(server.sends.get(1).get("context")).isEqualTo(context(203));
            assertThat(server.requests).filteredOn(GET_TOPIC::equals).hasSize(2);
        }
    }

    @Test
    void shouldKeepTheSendContextsWhenThisClientCreatesAStreamOrATopic() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            send(client, Partitioning.partitionId(3L));

            client.streams().createStream("stream");
            client.topics()
                    .createTopic(STREAM, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, "topic");
            send(client, Partitioning.partitionId(3L));

            assertThat(server.requests).filteredOn(GET_TOPIC::equals).hasSize(1);
        }
    }

    @Test
    void shouldSendWithoutAContextWhenTheCallerMayNotReadTheTopic() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            server.topicReadable = false;

            send(client, Partitioning.partitionId(3L));
            send(client, Partitioning.partitionId(3L));

            assertThat(server.requests).containsExactly(GET_TOPIC, POST_MESSAGES, POST_MESSAGES);
            assertThat(server.sends).noneMatch(send -> send.has("context"));
        }
    }

    @Test
    void shouldReadTheTopicAgainForAPartitionCreatedSinceTheLastRead() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            send(client, Partitioning.partitionId(3L));
            server.partitionIds = List.of(1L, 3L, 4L);

            send(client, Partitioning.partitionId(4L));

            assertThat(server.requests).containsExactly(GET_TOPIC, POST_MESSAGES, GET_TOPIC, POST_MESSAGES);
            assertThat(server.sends.get(1).get("context")).isEqualTo(context(104));
        }
    }

    @Test
    void shouldSendTheCallerPollContextAsAQueryParameter() throws IOException {
        try (var server = new FakeServer();
                var client = new IggyHttpClient(server.url())) {
            var context = new PartitionContext(BigInteger.valueOf(41), BigInteger.valueOf(42), BigInteger.valueOf(43));

            poll(client, PollingStrategy.offset(BigInteger.valueOf(5)).withContext(context));
            poll(client, PollingStrategy.offset(BigInteger.valueOf(5)));

            assertThat(server.requests).containsExactly(POLL_MESSAGES, POLL_MESSAGES);
            assertThat(queryParameter(server.polls.get(0), "context").map(MAPPER::readTree))
                    .contains(MAPPER.readTree("{\"incarnation\": 41, \"owner_generation\": 42, \"metadata_op\": 43}"));
            assertThat(queryParameter(server.polls.get(1), "context")).isEmpty();
        }
    }

    private static void send(IggyHttpClient client, Partitioning partitioning) {
        client.messages().sendMessages(STREAM, TOPIC, partitioning, List.of(Message.of("payload")));
    }

    private static void poll(IggyHttpClient client, PollingStrategy strategy) {
        client.messages().pollMessages(1L, 2L, Optional.of(3L), 7L, strategy, 10L, false);
    }

    private static Optional<String> queryParameter(String query, String name) {
        return Arrays.stream(query.split("&"))
                .filter(parameter -> parameter.startsWith(name + "="))
                .map(parameter -> URLDecoder.decode(parameter.substring(name.length() + 1), StandardCharsets.UTF_8))
                .findFirst();
    }

    private static JsonNode context(long incarnation) {
        return MAPPER.readTree("{\"incarnation\": " + incarnation + ", \"owner_generation\": 0, \"metadata_op\": 52}");
    }

    enum LayoutChange {
        DELETE_STREAM(client -> client.streams().deleteStream(STREAM)),
        DELETE_TOPIC(client -> client.topics().deleteTopic(STREAM, TOPIC)),
        CREATE_PARTITIONS(client -> client.partitions().createPartitions(STREAM, TOPIC, 1L)),
        DELETE_PARTITIONS(client -> client.partitions().deletePartitions(STREAM, TOPIC, 1L));

        private final Consumer<IggyHttpClient> change;

        LayoutChange(Consumer<IggyHttpClient> change) {
            this.change = change;
        }

        void apply(IggyHttpClient client) {
            change.accept(client);
        }
    }

    /**
     * Answers the REST calls these tests make. A partition's incarnation is the layout epoch times
     * 100 plus its id, and a send whose context names an older epoch is refused as the server
     * refuses a recreated partition.
     */
    private static final class FakeServer implements AutoCloseable {
        private static final long EPOCH_STRIDE = 100;

        private final HttpServer server;
        private final List<String> requests = new CopyOnWriteArrayList<>();
        private final List<JsonNode> sends = new CopyOnWriteArrayList<>();
        private final List<String> polls = new CopyOnWriteArrayList<>();
        private volatile long epoch = 1;
        private volatile List<Long> partitionIds = List.of(1L, 3L);
        private volatile boolean topicReadable = true;
        private volatile boolean recreateAfterEachRead;

        FakeServer() throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        String url() {
            return "http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":"
                    + server.getAddress().getPort();
        }

        void recreatePartitions() {
            epoch++;
        }

        private void handle(HttpExchange exchange) throws IOException {
            String request =
                    exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath();
            byte[] body = exchange.getRequestBody().readAllBytes();
            requests.add(request);
            switch (request) {
                case GET_TOPIC -> {
                    if (topicReadable) {
                        String details = topicDetails();
                        // Another client recreates the partitions right after this read, so every
                        // context it reports is stale by the time a send carries it.
                        if (recreateAfterEachRead) {
                            recreatePartitions();
                        }
                        reply(exchange, 200, details);
                    } else {
                        reply(exchange, 403, error(UNAUTHORIZED, "unauthorized"));
                    }
                }
                case POST_MESSAGES -> {
                    JsonNode send = MAPPER.readTree(body);
                    sends.add(send);
                    JsonNode context = send.get("context");
                    if (context != null && context.get("incarnation").asLong() / EPOCH_STRIDE != epoch) {
                        reply(exchange, 400, error(HISTORY_UNAVAILABLE, "history_unavailable"));
                    } else {
                        reply(exchange, 201, "{\"confirmations\": []}");
                    }
                }
                case POLL_MESSAGES -> {
                    polls.add(exchange.getRequestURI().getRawQuery());
                    reply(exchange, 200, """
                            {"partition_id": 3, "current_offset": 0, "count": 0, "messages": [],
                             "context": {"incarnation": 103, "owner_generation": 0, "metadata_op": 52}}
                            """);
                }
                case "POST /streams" -> reply(exchange, 201, """
                        {"id": 1, "created_at": 0, "name": "stream", "size": "0 B", "messages_count": 0,
                         "topics_count": 0, "topics": []}
                        """);
                case "POST /streams/1/topics" -> reply(exchange, 201, topicDetails());
                default -> reply(exchange, 204, "");
            }
        }

        private String topicDetails() {
            List<Long> ids = partitionIds;
            return """
                    {"id": 2, "created_at": 0, "name": "topic", "size": "0 B", "message_expiry": 0,
                     "compression_algorithm": "none", "max_topic_size": 0, "messages_count": 0,
                     "partitions_count": %d, "partitions": [%s], "options": {}}
                    """.formatted(ids.size(), ids.stream().map(this::partition).collect(Collectors.joining(", ")));
        }

        private String partition(long id) {
            return """
                    {"id": %d, "created_at": 0, "segments_count": 1, "current_offset": 0, "size": "0 B",
                     "messages_count": 0,
                     "context": {"incarnation": %d, "owner_generation": 0, "metadata_op": 52}}
                    """.formatted(id, epoch * EPOCH_STRIDE + id);
        }

        private static String error(int id, String code) {
            return "{\"id\": " + id + ", \"code\": \"" + code + "\", \"reason\": \"" + code + "\"}";
        }

        private static void reply(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
