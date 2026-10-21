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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.user.UserInfo;
import org.junit.jupiter.api.Test;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.apache.iggy.ResourceOptionsFixture.bytes;
import static org.apache.iggy.ResourceOptionsFixture.optionMap;
import static org.apache.iggy.ResourceOptionsFixture.section;
import static org.apache.iggy.ResourceOptionsFixture.streamUpdate;
import static org.apache.iggy.ResourceOptionsFixture.topicUpdate;
import static org.apache.iggy.ResourceOptionsFixture.userUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpOptionValueTest {

    private static final ObjectMapper MAPPER = ObjectMapperFactory.getInstance();
    private static final TypeReference<Map<String, HttpOptionValue>> OPTIONS = new TypeReference<>() {};

    @Test
    void givenServerDescribeJsonWhenMappingShouldRebuildFixtureFlags() {
        JsonNode flagBits = section("constants").get("option_flags");
        for (JsonNode vector : section("describe_options")) {
            var json = MAPPER.createArrayNode();
            for (JsonNode entry : vector.get("entries")) {
                int flags = entry.get("flags").asInt();
                var spec = json.addObject()
                        .put("key", entry.get("key").asString())
                        .put("kind", entry.get("kind").asString())
                        .put("description", "");
                spec.putArray("default_value");
                for (String flag : List.of("updatable", "resettable", "secret", "read_only", "critical")) {
                    spec.put(flag, (flags & flagBits.get(flag).asInt()) != 0);
                }
            }
            List<SystemHttpClient.HttpOptionSpec> specs =
                    MAPPER.readValue(MAPPER.writeValueAsString(json), new TypeReference<>() {});
            List<String> expected = new ArrayList<>();
            vector.get("entries")
                    .forEach(entry -> expected.add(entry.get("key").asString() + " "
                            + entry.get("flags").asInt()));

            assertThat(specs.stream()
                            .map(SystemHttpClient.HttpOptionSpec::toOptionSpec)
                            .map(spec -> spec.key() + " " + spec.flags())
                            .toList())
                    .as(vector.get("scope").asString())
                    .isEqualTo(expected);
        }
    }

    @Test
    void givenSharedResponsesWhenDecodingHttpOptionsShouldMatchBinaryBlocks() {
        for (JsonNode vector : section("stream_responses")) {
            StreamBase stream = BytesDeserializer.readStreamBase(response(vector));
            assertSameBlocks(vector, stream.options(), stream.derivedOptions());
        }
        for (JsonNode vector : section("topic_responses")) {
            Topic topic = BytesDeserializer.readTopic(response(vector));
            assertSameBlocks(vector, topic.options(), topic.derivedOptions());
        }
        for (JsonNode vector : section("user_responses")) {
            UserInfo user = BytesDeserializer.readUserInfo(response(vector));
            assertSameBlocks(vector, user.options(), user.derivedOptions());
        }
    }

    @Test
    void givenTypedUpdatesWhenRenderingPutBodyShouldMatchWireSetAndReset() {
        Map<String, JsonNode> bodiesByWire = new HashMap<>();
        for (JsonNode vector : section("update_resource")) {
            JsonNode body = render(updateBody(vector));
            String name = vector.get("case").asString();
            var expectedSet = optionMap(vector.get("wire_set"), true);
            List<String> expectedReset = new ArrayList<>();
            vector.get("wire_reset").forEach(key -> expectedReset.add(key.asString()));
            List<String> keys = new ArrayList<>();
            body.properties().forEach(entry -> keys.add(entry.getKey()));
            List<String> expectedKeys = new ArrayList<>(expectedSet.keySet());
            if (!expectedReset.isEmpty()) {
                expectedKeys.add("reset");
            }
            assertThat(keys).as(name).containsExactlyInAnyOrderElementsOf(expectedKeys);
            expectedSet.forEach((key, value) ->
                    assertThat(body.get(key).asString()).as(name + " " + key).isEqualTo(value.toStringValue()));
            List<String> reset = new ArrayList<>();
            if (body.has("reset")) {
                body.get("reset").forEach(key -> reset.add(key.asString()));
            }
            assertThat(reset).as(name).isEqualTo(expectedReset);
            bodiesByWire.put(vector.get("body_hex").asString(), body);
        }
        int matched = 0;
        for (JsonNode vector : section("http_update")) {
            JsonNode wire = vector.get("update_resource_hex");
            if (vector.get("outcome").asString().equals("accepted") && bodiesByWire.containsKey(wire.asString())) {
                assertThat(bodiesByWire.get(wire.asString()))
                        .as(vector.get("case").asString())
                        .isEqualTo(MAPPER.readTree(vector.get("body").asString()));
                matched++;
            }
        }
        assertThat(matched).isEqualTo(3);
    }

    @Test
    void givenEveryHeaderKindWhenDecodingHttpTextShouldRebuildExactBytes() {
        Map<String, HttpOptionValue> options = Map.of(
                "int8", new HttpOptionValue("-5", HeaderKind.Int8, true),
                "int128", new HttpOptionValue("-170141183460469231731687303715884105728", HeaderKind.Int128, true),
                "uint128", new HttpOptionValue("340282366920938463463374607431768211455", HeaderKind.Uint128, true),
                "uint16", new HttpOptionValue("65535", HeaderKind.Uint16, true),
                "nan", new HttpOptionValue("0x7fc00001", HeaderKind.Float32, true),
                "negative_zero", new HttpOptionValue("0x8000000000000000", HeaderKind.Float64, true),
                "raw", new HttpOptionValue("AAEC", HeaderKind.Raw, true),
                "bool", new HttpOptionValue("false", HeaderKind.Bool, true));

        Map<String, HeaderValue> decoded = HttpOptionValue.select(options, true);

        assertThat(hex(decoded.get("int8"))).isEqualTo("fb");
        assertThat(hex(decoded.get("int128"))).isEqualTo("00000000000000000000000000000080");
        assertThat(hex(decoded.get("uint128"))).isEqualTo("ffffffffffffffffffffffffffffffff");
        assertThat(hex(decoded.get("uint16"))).isEqualTo("ffff");
        assertThat(hex(decoded.get("nan"))).isEqualTo("0100c07f");
        assertThat(hex(decoded.get("negative_zero"))).isEqualTo("0000000000000080");
        assertThat(hex(decoded.get("raw"))).isEqualTo("000102");
        assertThat(decoded.get("bool")).isEqualTo(HeaderValue.fromBool(false));
        for (HttpOptionValue noncanonical : List.of(
                new HttpOptionValue("+5", HeaderKind.Int8, true),
                new HttpOptionValue("05", HeaderKind.Uint32, true),
                new HttpOptionValue("256", HeaderKind.Uint8, true),
                new HttpOptionValue("AAE", HeaderKind.Raw, true),
                new HttpOptionValue("0x7FC00001", HeaderKind.Float32, true),
                new HttpOptionValue("TRUE", HeaderKind.Bool, true))) {
            assertThatThrownBy(() -> HttpOptionValue.select(Map.of("key", noncanonical), true))
                    .as(noncanonical.toString())
                    .isInstanceOf(IggyMalformedResponseException.class);
        }
    }

    private static void assertSameBlocks(
            JsonNode vector, Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived) {
        Map<String, HttpOptionValue> options =
                MAPPER.convertValue(vector.get("expected").get("options"), OPTIONS);
        String name = vector.get("case").asString();
        assertThat(HttpOptionValue.select(options, true)).as(name).isEqualTo(explicit);
        assertThat(HttpOptionValue.select(options, false)).as(name).isEqualTo(derived);
    }

    private static Map<String, Object> updateBody(JsonNode vector) {
        JsonNode input = vector.get("input");
        return switch (vector.get("scope").asString()) {
            case "topic" -> {
                var update = topicUpdate(input);
                yield HttpOptionValue.updateBody(update.entries(), update.resetKeys());
            }
            case "stream" -> {
                var update = streamUpdate(input);
                yield HttpOptionValue.updateBody(update.entries(), update.resetKeys());
            }
            default -> {
                var update = userUpdate(input);
                yield HttpOptionValue.updateBody(update.entries(), update.resetKeys());
            }
        };
    }

    private static JsonNode render(Map<String, Object> body) {
        return MAPPER.readTree(MAPPER.writeValueAsString(body));
    }

    private static ByteBuf response(JsonNode vector) {
        return Unpooled.wrappedBuffer(bytes(vector.get("response_hex").asString()));
    }

    private static String hex(HeaderValue value) {
        return HexFormat.of().formatHex(value.value());
    }
}
