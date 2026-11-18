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

import io.netty.buffer.ByteBuf;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageOrder;
import org.apache.iggy.system.PageQuery;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.user.UserInfo;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/** The shared list page vectors that {@code core/common/examples/list_pages_fixtures.rs} writes. */
public final class ListPagesFixture {

    private static final JsonNode ROOT = load();

    private ListPagesFixture() {}

    public static JsonNode section(String name) {
        return ROOT.get(name);
    }

    public static JsonNode refused(String name) {
        return ROOT.get("refused").get(name);
    }

    /** The query a vector's {@code input} describes without its name, and without the page rules applied. */
    public static PageQuery query(JsonNode input) {
        var builder = PageQuery.builder()
                .order(PageOrder.valueOf(input.get("order").asString().toUpperCase(Locale.ROOT)))
                .descending(input.get("descending").asBoolean())
                .limit(input.get("limit").asLong());
        if (!input.get("cursor").isNull()) {
            builder.cursor(PageCursor.parse(input.get("cursor").asString()));
        }
        return builder.build();
    }

    /** The query a vector's {@code input} describes with its name, and without the page rules applied. */
    public static NamedPageQuery namedQuery(JsonNode input) {
        JsonNode name = input.get("name");
        return new NamedPageQuery(query(input), name.isNull() ? Optional.empty() : Optional.of(name.asString()));
    }

    public static Optional<StreamId> streamId(JsonNode input) {
        return identifier(input.get("stream_id"), StreamId::of, StreamId::of);
    }

    public static Optional<TopicId> topicId(JsonNode input) {
        return identifier(input.get("topic_id"), TopicId::of, TopicId::of);
    }

    public static Map<String, String> httpQuery(JsonNode vector) {
        Map<String, String> pairs = new TreeMap<>();
        vector.get("http_query")
                .properties()
                .forEach(pair -> pairs.put(pair.getKey(), pair.getValue().asString()));
        return pairs;
    }

    /** Decodes a reply of {@code command} to the ids and names the vectors list. */
    public static Page<Item> readPage(String command, ByteBuf reply) {
        Function<ByteBuf, Item> readItem = switch (command) {
            case "get_streams" ->
                body -> {
                    StreamBase stream = BytesDeserializer.readStreamBase(body);
                    return new Item(stream.id(), Optional.of(stream.name()));
                };
            case "get_topics" ->
                body -> {
                    Topic topic = BytesDeserializer.readTopic(body);
                    return new Item(topic.id(), Optional.of(topic.name()));
                };
            case "get_partitions" ->
                body -> {
                    Partition partition = BytesDeserializer.readPartition(body);
                    return new Item(partition.id(), Optional.empty());
                };
            case "get_consumer_groups" ->
                body -> {
                    ConsumerGroup group = BytesDeserializer.readConsumerGroup(body);
                    return new Item(group.id(), Optional.of(group.name()));
                };
            case "get_users" ->
                body -> {
                    UserInfo user = BytesDeserializer.readUserInfo(body);
                    return new Item(user.id(), Optional.of(user.username()));
                };
            default -> throw new IllegalArgumentException("unknown command " + command);
        };
        return BytesDeserializer.readPage(reply, readItem);
    }

    /** The {@code items} of a response vector. */
    public static List<Item> items(JsonNode vector) {
        List<Item> items = new ArrayList<>();
        for (JsonNode item : vector.get("items")) {
            JsonNode name = item.get("name");
            items.add(
                    new Item(item.get("id").asLong(), name.isNull() ? Optional.empty() : Optional.of(name.asString())));
        }
        return items;
    }

    private static <T> Optional<T> identifier(JsonNode id, Function<Long, T> numeric, Function<String, T> named) {
        if (id.isNull()) {
            return Optional.empty();
        }
        JsonNode value = id.get("value");
        return Optional.of(
                id.get("kind").asString().equals("numeric")
                        ? numeric.apply(value.asLong())
                        : named.apply(value.asString()));
    }

    private static JsonNode load() {
        try (InputStream fixture = ListPagesFixture.class.getResourceAsStream("/list_pages.json")) {
            if (fixture == null) {
                throw new IllegalStateException("list_pages.json is not on the test classpath");
            }
            return JsonMapper.builder().build().readTree(fixture);
        } catch (IOException unreadable) {
            throw new UncheckedIOException(unreadable);
        }
    }

    /** The id and the name of one list item. A partition has no name. */
    public record Item(long id, Optional<String> name) {}
}
