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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageQuery;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.apache.iggy.ListPagesFixture.httpQuery;
import static org.apache.iggy.ListPagesFixture.namedQuery;
import static org.apache.iggy.ListPagesFixture.query;
import static org.apache.iggy.ListPagesFixture.section;
import static org.apache.iggy.ListPagesFixture.streamId;
import static org.apache.iggy.ListPagesFixture.topicId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The page methods of the HTTP client against a loopback peer that records each request. */
class ListPagesHttpTest {

    private static final String CURSOR = "pg1_AQcAAAAAAAAA";
    private static final String PAGE_WITH_NEXT = "{\"items\":[],\"next\":\"" + CURSOR + "\"}";
    private static final String LAST_PAGE = "{\"items\":[]}";
    private static final String PAGE_WITH_FOREIGN_NEXT = "{\"items\":[],\"next\":\"mc1_AQ\"}";
    private static final StreamId STREAM = StreamId.of(1L);
    private static final TopicId TOPIC = TopicId.of(1L);
    private static final NamedPageQuery FIRST_PAGE =
            new NamedPageQuery(PageQuery.builder().build());

    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> queries = new CopyOnWriteArrayList<>();
    private final Queue<String> answers = new ConcurrentLinkedQueue<>();

    @Test
    void givenSharedRequestsWhenReadingPagesShouldSendExactlyTheirHttpQuery() throws IOException {
        List<JsonNode> vectors = new ArrayList<>();
        section("requests").forEach(vectors::add);
        vectors.forEach(vector -> answers.add(LAST_PAGE));

        withClient(client -> vectors.forEach(vector -> read(client, vector)));

        assertThat(paths).hasSize(vectors.size());
        for (int index = 0; index < vectors.size(); index++) {
            JsonNode vector = vectors.get(index);
            String name = vector.get("case").asString();
            assertThat(paths.get(index)).as(name).isEqualTo(route(vector));
            assertThat(decode(queries.get(index))).as(name).isEqualTo(httpQuery(vector));
        }
    }

    @Test
    void givenEachPageShapeWhenReadingShouldKeepTheNextCursorOnlyWhenSent() throws IOException {
        answers.addAll(List.of(PAGE_WITH_NEXT, LAST_PAGE, PAGE_WITH_FOREIGN_NEXT));
        List<Page<?>> pages = new ArrayList<>();

        withClient(client -> {
            pages.add(client.streams().getStreamsPage(FIRST_PAGE));
            pages.add(client.streams().getStreamsPage(FIRST_PAGE));
            assertThatThrownBy(() -> client.streams().getStreamsPage(FIRST_PAGE))
                    .isInstanceOf(IggyMalformedResponseException.class);
        });

        assertThat(pages.get(0).next()).contains(PageCursor.parse(CURSOR));
        assertThat(pages.get(1).next()).isEmpty();
    }

    @Test
    void givenABrokenQueryWhenReadingAPageShouldFailBeforeSendingIt() throws IOException {
        PageQuery broken = PageQuery.builder().limit(0).build();
        NamedPageQuery brokenNamed = new NamedPageQuery(broken);

        withClient(client -> {
            assertRefused(() -> client.streams().getStreamsPage(brokenNamed));
            assertRefused(() -> client.topics().getTopicsPage(STREAM, brokenNamed));
            assertRefused(() -> client.partitions().getPartitionsPage(STREAM, TOPIC, broken));
            assertRefused(() -> client.consumerGroups().getConsumerGroupsPage(STREAM, TOPIC, brokenNamed));
            assertRefused(() -> client.users().getUsersPage(brokenNamed));
        });

        assertThat(paths).isEmpty();
    }

    private void withClient(Consumer<IggyHttpClient> calls) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::answer);
        server.start();
        try (var client =
                new IggyHttpClient("http://" + InetAddress.getLoopbackAddress().getHostAddress() + ":"
                        + server.getAddress().getPort())) {
            calls.accept(client);
        } finally {
            server.stop(0);
        }
    }

    private void answer(HttpExchange exchange) throws IOException {
        paths.add(exchange.getRequestURI().getPath());
        queries.add(exchange.getRequestURI().getRawQuery());
        byte[] body = answers.remove().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void read(IggyHttpClient client, JsonNode vector) {
        JsonNode input = vector.get("input");
        switch (vector.get("command").asString()) {
            case "get_streams" -> client.streams().getStreamsPage(namedQuery(input));
            case "get_topics" -> client.topics().getTopicsPage(streamOf(input), namedQuery(input));
            case "get_partitions" ->
                client.partitions().getPartitionsPage(streamOf(input), topicOf(input), query(input));
            case "get_consumer_groups" ->
                client.consumerGroups().getConsumerGroupsPage(streamOf(input), topicOf(input), namedQuery(input));
            case "get_users" -> client.users().getUsersPage(namedQuery(input));
            default -> throw new IllegalArgumentException("unknown command " + vector.get("command"));
        }
    }

    private static String route(JsonNode vector) {
        JsonNode input = vector.get("input");
        return switch (vector.get("command").asString()) {
            case "get_streams" -> "/streams";
            case "get_topics" -> "/streams/" + streamOf(input) + "/topics";
            case "get_partitions" -> "/streams/" + streamOf(input) + "/topics/" + topicOf(input) + "/partitions";
            case "get_consumer_groups" ->
                "/streams/" + streamOf(input) + "/topics/" + topicOf(input) + "/consumer-groups";
            case "get_users" -> "/users";
            default -> throw new IllegalArgumentException("unknown command " + vector.get("command"));
        };
    }

    /** Decodes a query string as the server does, refusing a repeated key as the server does. */
    private static Map<String, String> decode(String rawQuery) {
        Map<String, String> pairs = new TreeMap<>();
        for (String pair : rawQuery.split("&")) {
            int separator = pair.indexOf('=');
            String key = URLDecoder.decode(pair.substring(0, separator), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(separator + 1), StandardCharsets.UTF_8);
            assertThat(pairs.put(key, value)).as("repeated key " + key).isNull();
        }
        return pairs;
    }

    private static StreamId streamOf(JsonNode input) {
        return streamId(input).orElseThrow();
    }

    private static TopicId topicOf(JsonNode input) {
        return topicId(input).orElseThrow();
    }

    private static void assertRefused(ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }
}
