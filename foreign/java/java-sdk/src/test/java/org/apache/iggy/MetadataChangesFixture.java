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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeRecord;
import org.apache.iggy.system.MetadataChangeStart;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.MetadataCurrentItem;
import org.apache.iggy.system.MetadataCurrentPageToken;
import org.apache.iggy.user.Permissions;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared metadata changes fixture the Rust encoders generate, read from the test classpath, and the checks that
 * a decoded answer holds every value of the JSON the server renders for it.
 *
 * <p>Body fields are matched by name: a JSON property {@code stream_created_op} is the record component
 * {@code streamCreatedOp}, so every field of every body kind is compared without a hand-written list per kind.
 */
public final class MetadataChangesFixture {

    private static final JsonNode ROOT = load();
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .withConfigOverride(Map.class, map -> map.setNullHandling(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY)))
            .build();
    private static final Set<String> RECORD_ENVELOPE =
            Set.of("op", "ordinal", "checksum", "timestamp", "actor_user_id", "group", "kind");
    private static final Set<String> ITEM_ENVELOPE = Set.of("group");
    private static final Set<String> OPTION_COMPONENTS = Set.of("options", "derivedOptions", "optionsUnreadable");

    private MetadataChangesFixture() {}

    public static JsonNode section(String name) {
        return ROOT.get(name);
    }

    public static JsonNode refused(String name) {
        return ROOT.get("refused").get(name);
    }

    public static JsonNode find(JsonNode section, String vectorCase) {
        for (JsonNode vector : section) {
            if (vector.get("case").asString().equals(vectorCase)) {
                return vector;
            }
        }
        throw new IllegalArgumentException("no vector " + vectorCase);
    }

    public static List<String> cases(JsonNode section) {
        List<String> cases = new ArrayList<>();
        section.forEach(vector -> cases.add(vector.get("case").asString()));
        return cases;
    }

    public static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }

    public static ByteBuf buffer(String hex) {
        return Unpooled.wrappedBuffer(bytes(hex));
    }

    public static JsonNode parse(String json) {
        return MAPPER.readTree(json);
    }

    /** The query a fixture vector's {@code input} describes. */
    public static MetadataChangesQuery query(JsonNode input) {
        var builder = MetadataChangesQuery.builder()
                .start(start(input))
                .limit(input.get("limit").asLong());
        List<MetadataChangeGroup> groups = new ArrayList<>();
        input.get("groups")
                .forEach(group -> groups.add(
                        MetadataChangeGroup.fromValue(group.asString()).orElseThrow()));
        builder.groups(groups);
        if (!input.get("since").isNull()) {
            builder.since(input.get("since").bigIntegerValue());
        }
        if (!input.get("until").isNull()) {
            builder.until(input.get("until").bigIntegerValue());
        }
        return builder.build();
    }

    /** Asserts that {@code actual} holds every value of the answer the server renders as {@code expected}. */
    public static void assertAnswer(JsonNode expected, MetadataChanges actual) {
        assertThat(actual.status().value()).isEqualTo(expected.get("status").asString());
        assertThat(actual.phase().value()).isEqualTo(expected.get("phase").asString());
        assertValue("log_id", expected.get("log_id"), actual.logId());
        assertValue("oldest_op", expected.get("oldest_op"), actual.oldestOp());
        assertValue("retained_since", expected.get("retained_since"), actual.retainedSince());
        assertValue("head_op", expected.get("head_op"), actual.headOp());
        assertThat(actual.next().map(MetadataChangeCursor::toString)).isEqualTo(text(expected.get("next")));
        assertThat(actual.hasMore()).isEqualTo(expected.get("has_more").asBoolean());
        assertThat(actual.currentPageToken().map(MetadataCurrentPageToken::toString))
                .isEqualTo(text(expected.get("current_page_token")));
        assertThat(actual.records()).hasSize(expected.get("records").size());
        assertThat(actual.items()).hasSize(expected.get("items").size());
        for (int index = 0; index < actual.records().size(); index++) {
            assertRecord(expected.get("records").get(index), actual.records().get(index));
        }
        for (int index = 0; index < actual.items().size(); index++) {
            assertItem(expected.get("items").get(index), actual.items().get(index));
        }
    }

    /** Asserts that {@code actual} holds every value of the record the server renders as {@code expected}. */
    public static void assertRecord(JsonNode expected, MetadataChangeRecord actual) {
        if (actual.body() instanceof MetadataChangeBody.UnknownJson unknown) {
            assertThat(parse(unknown.json())).isEqualTo(expected);
            assertThat(actual.kind()).isEmpty();
            return;
        }
        assertValue("op", expected.get("op"), actual.op());
        assertValue("ordinal", expected.get("ordinal"), actual.ordinal());
        assertValue("checksum", expected.get("checksum"), actual.checksum());
        assertValue("timestamp", expected.get("timestamp"), actual.timestamp());
        assertThat(actual.actorUserId().map(String::valueOf)).isEqualTo(number(expected.get("actor_user_id")));
        if (actual.body() instanceof MetadataChangeBody.Unknown unknown) {
            assertThat(String.valueOf(unknown.group()))
                    .isEqualTo(expected.get("group").asString());
            assertThat(unknown.kind().map(String::valueOf))
                    .contains(expected.get("kind").asString());
            assertThat(Base64.getEncoder().encodeToString(unknown.body()))
                    .isEqualTo(expected.get("body").asString());
            return;
        }
        assertThat(actual.group().orElseThrow().value())
                .isEqualTo(expected.get("group").asString());
        assertThat(actual.kind().orElseThrow().value())
                .isEqualTo(expected.get("kind").asString());
        assertBody(expected, actual.body(), RECORD_ENVELOPE);
    }

    /** Asserts that {@code actual} holds every value of the item the server renders as {@code expected}. */
    public static void assertItem(JsonNode expected, MetadataCurrentItem actual) {
        if (actual.body() instanceof MetadataChangeBody.UnknownJson unknown) {
            assertThat(parse(unknown.json())).isEqualTo(expected);
            return;
        }
        assertThat(actual.group().orElseThrow().value())
                .isEqualTo(expected.get("group").asString());
        assertBody(expected, actual.body(), ITEM_ENVELOPE);
    }

    private static void assertBody(JsonNode expected, MetadataChangeBody body, Set<String> envelope) {
        for (Map.Entry<String, JsonNode> property : expected.properties()) {
            String name = property.getKey();
            if (envelope.contains(name) || name.startsWith("options")) {
                continue;
            }
            if ("permissions".equals(name)) {
                assertThat(component(body, "permissions")).as(name).isEqualTo(permissions(property.getValue()));
            } else {
                assertValue(name, property.getValue(), component(body, camelCase(name)));
            }
        }
        for (RecordComponent component : body.getClass().getRecordComponents()) {
            String name = snakeCase(component.getName());
            if (!expected.has(name) && !OPTION_COMPONENTS.contains(component.getName())) {
                assertThat(component(body, component.getName())).as(name).isEqualTo(Optional.empty());
            }
        }
        if (expected.has("options") || expected.has("options_unreadable")) {
            assertOptions(expected, body);
        }
    }

    private static void assertOptions(JsonNode expected, MetadataChangeBody body) {
        boolean unreadable = expected.has("options_unreadable")
                && expected.get("options_unreadable").asBoolean();
        assertThat(component(body, "optionsUnreadable")).isEqualTo(unreadable);
        JsonNode options = unreadable ? MAPPER.createObjectNode() : expected.get("options");
        assertThat(component(body, "options")).isEqualTo(ResourceOptionsFixture.optionMap(options, true));
        assertThat(component(body, "derivedOptions")).isEqualTo(ResourceOptionsFixture.optionMap(options, false));
    }

    private static void assertValue(String field, JsonNode expected, Object actual) {
        Object value = actual instanceof Optional<?> optional ? optional.orElse(null) : actual;
        if (expected.isNull()) {
            assertThat(value).as(field).isNull();
        } else if (expected.isArray()) {
            List<String> elements = new ArrayList<>();
            expected.forEach(element -> elements.add(element.bigIntegerValue().toString()));
            assertThat(((List<?>) value).stream().map(String::valueOf).toList())
                    .as(field)
                    .isEqualTo(elements);
        } else if (expected.isNumber()) {
            assertThat(String.valueOf(value))
                    .as(field)
                    .isEqualTo(expected.bigIntegerValue().toString());
        } else {
            assertThat(text(value))
                    .as(field)
                    .isEqualTo(expected.isBoolean() ? expected.asString() : expected.stringValue());
        }
    }

    /** A decoded value in the text the JSON renders it as: 128-bit values in hex, enums by lowercase name. */
    private static String text(Object value) {
        if (value instanceof BigInteger wide) {
            return String.format("%032x", wide);
        }
        if (value instanceof Enum<?> constant) {
            return constant.name().toLowerCase(Locale.ROOT);
        }
        return String.valueOf(value);
    }

    private static Optional<String> text(JsonNode node) {
        return node.isNull() ? Optional.empty() : Optional.of(node.asString());
    }

    private static Optional<String> number(JsonNode node) {
        return node == null || node.isNull()
                ? Optional.empty()
                : Optional.of(node.bigIntegerValue().toString());
    }

    private static Optional<Permissions> permissions(JsonNode node) {
        return node.isNull() ? Optional.empty() : Optional.of(MAPPER.treeToValue(node, Permissions.class));
    }

    private static Object component(MetadataChangeBody body, String name) {
        try {
            return body.getClass().getMethod(name).invoke(body);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException missing) {
            throw new AssertionError(body.getClass().getSimpleName() + " has no component " + name, missing);
        }
    }

    private static String camelCase(String snake) {
        StringBuilder camel = new StringBuilder();
        boolean upper = false;
        for (char character : snake.toCharArray()) {
            if (character == '_') {
                upper = true;
            } else {
                camel.append(upper ? Character.toUpperCase(character) : character);
                upper = false;
            }
        }
        return camel.toString();
    }

    private static String snakeCase(String camel) {
        return camel.replaceAll("([A-Z])", "_$1").toLowerCase(Locale.ROOT);
    }

    private static MetadataChangeStart start(JsonNode input) {
        return switch (input.get("start").asString()) {
            case "earliest" -> MetadataChangeStart.earliest();
            case "latest" -> MetadataChangeStart.latest();
            case "after" ->
                MetadataChangeStart.after(
                        MetadataChangeCursor.parse(input.get("cursor").asString()));
            default ->
                input.get("page_token").isNull()
                        ? MetadataChangeStart.current()
                        : MetadataChangeStart.current(MetadataCurrentPageToken.parse(
                                input.get("page_token").asString()));
        };
    }

    private static JsonNode load() {
        try (InputStream fixture = MetadataChangesFixture.class.getResourceAsStream("/metadata_changes.json")) {
            if (fixture == null) {
                throw new IllegalStateException("metadata_changes.json is not on the test classpath");
            }
            return JsonMapper.builder().build().readTree(fixture);
        } catch (IOException unreadable) {
            throw new UncheckedIOException(unreadable);
        }
    }
}
