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
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.stream.StreamOptionKey;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.OptionKey;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.UserOptionKey;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * The shared resource options fixture the Rust encoders generate, read from the test classpath.
 *
 * <p>The JSON copy is byte-identical with every other SDK's, so the vectors here are the server's contract,
 * not this SDK's own serializer run twice.
 */
public final class ResourceOptionsFixture {

    private static final JsonNode ROOT = load();

    private ResourceOptionsFixture() {}

    public static JsonNode section(String name) {
        return ROOT.get(name);
    }

    public static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }

    /** The readable bytes of {@code buffer} as lowercase hex, releasing it. */
    public static String hex(ByteBuf buffer) {
        try {
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), bytes);
            return HexFormat.of().formatHex(bytes);
        } finally {
            buffer.release();
        }
    }

    public static HeaderKind kind(String name) {
        for (HeaderKind kind : HeaderKind.values()) {
            if (kind.name().equalsIgnoreCase(name)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown kind " + name);
    }

    /** A value in the fixture's reversible text form for its kind. */
    public static HeaderValue optionValue(String kind, String text) {
        return switch (kind(kind)) {
            case String -> HeaderValue.fromString(text);
            case Bool -> HeaderValue.fromBool(Boolean.parseBoolean(text));
            case Uint32 -> HeaderValue.fromUint32(Long.parseLong(text));
            case Uint64 -> HeaderValue.fromUint64(new BigInteger(text));
            case Raw -> HeaderValue.fromRaw(Base64.getDecoder().decode(text));
            default -> throw new IllegalArgumentException("fixture kind " + kind);
        };
    }

    /** The entries of an option map in the fixture's JSON form whose provenance is {@code explicit}. */
    public static Map<String, HeaderValue> optionMap(JsonNode options, boolean explicit) {
        Map<String, HeaderValue> selected = new LinkedHashMap<>();
        for (Map.Entry<String, JsonNode> entry : options.properties()) {
            JsonNode option = entry.getValue();
            if (option.get("explicit").asBoolean() == explicit) {
                selected.put(
                        entry.getKey(),
                        optionValue(
                                option.get("kind").asString(),
                                option.get("value").asString()));
            }
        }
        return selected;
    }

    public static StreamId streamId(JsonNode target) {
        JsonNode id = target.get("stream_id");
        return id.isNumber() ? StreamId.of(id.asLong()) : StreamId.of(id.asString());
    }

    public static TopicId topicId(JsonNode target) {
        JsonNode id = target.get("topic_id");
        return id.isNumber() ? TopicId.of(id.asLong()) : TopicId.of(id.asString());
    }

    public static UserId userId(JsonNode target) {
        JsonNode id = target.get("user_id");
        return id.isNumber() ? UserId.of(id.asLong()) : UserId.of(id.asString());
    }

    /** The typed update a fixture vector's {@code input} describes, built for its {@code scope}. */
    public static StreamUpdate streamUpdate(JsonNode input) {
        var builder = StreamUpdate.builder();
        text(input, "name").ifPresent(builder::name);
        resetKeys(input, StreamOptionKey.class, builder::rawReset).forEach(builder::reset);
        return builder.build();
    }

    public static TopicUpdate topicUpdate(JsonNode input) {
        var builder = TopicUpdate.builder();
        text(input, "name").ifPresent(builder::name);
        text(input, "message_expiry").map(BigInteger::new).ifPresent(builder::messageExpiry);
        text(input, "max_topic_size").map(BigInteger::new).ifPresent(builder::maxTopicSize);
        resetKeys(input, TopicOptionKey.class, builder::rawReset).forEach(builder::reset);
        return builder.build();
    }

    public static UserUpdate userUpdate(JsonNode input) {
        var builder = UserUpdate.builder();
        text(input, "username").ifPresent(builder::username);
        text(input, "status")
                .map(status -> status.equals("active") ? UserStatus.Active : UserStatus.Inactive)
                .ifPresent(builder::status);
        resetKeys(input, UserOptionKey.class, builder::rawReset).forEach(builder::reset);
        return builder.build();
    }

    /** The catalog key enum constant spelled {@code value}. */
    public static <K extends Enum<K> & OptionKey> K optionKey(Class<K> keys, String value) {
        for (K key : keys.getEnumConstants()) {
            if (key.value().equals(value)) {
                return key;
            }
        }
        throw new IllegalArgumentException("no " + keys.getSimpleName() + " named " + value);
    }

    private static Optional<String> text(JsonNode input, String field) {
        return Optional.ofNullable(input.get(field)).map(JsonNode::asString);
    }

    /** The typed reset keys of {@code input}, handing every name without a key constant to {@code raw}. */
    private static <K extends Enum<K> & OptionKey> List<K> resetKeys(
            JsonNode input, Class<K> keys, Consumer<String> raw) {
        List<K> reset = new ArrayList<>();
        JsonNode names = input.get("reset");
        if (names != null) {
            names.forEach(name -> Arrays.stream(keys.getEnumConstants())
                    .filter(key -> key.value().equals(name.asString()))
                    .findFirst()
                    .ifPresentOrElse(reset::add, () -> raw.accept(name.asString())));
        }
        return reset;
    }

    private static JsonNode load() {
        try (InputStream fixture = ResourceOptionsFixture.class.getResourceAsStream("/resource_options.json")) {
            if (fixture == null) {
                throw new IllegalStateException("resource_options.json is not on the test classpath");
            }
            return JsonMapper.builder().build().readTree(fixture);
        } catch (IOException unreadable) {
            throw new UncheckedIOException(unreadable);
        }
    }
}
