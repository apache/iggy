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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamOptionKey;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.OptionKey;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Durability;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserOptionKey;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.iggy.ResourceOptionsFixture.bytes;
import static org.apache.iggy.ResourceOptionsFixture.hex;
import static org.apache.iggy.ResourceOptionsFixture.kind;
import static org.apache.iggy.ResourceOptionsFixture.optionMap;
import static org.apache.iggy.ResourceOptionsFixture.section;
import static org.apache.iggy.ResourceOptionsFixture.streamId;
import static org.apache.iggy.ResourceOptionsFixture.streamUpdate;
import static org.apache.iggy.ResourceOptionsFixture.topicId;
import static org.apache.iggy.ResourceOptionsFixture.topicUpdate;
import static org.apache.iggy.ResourceOptionsFixture.userId;
import static org.apache.iggy.ResourceOptionsFixture.userUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * The shared resource options vectors against this SDK's own encoder and decoder.
 *
 * <p>The Java SDK encodes the wire itself, so every request here must match the Rust encoder byte for byte
 * and every response must decode to the projection the Rust reader returns.
 */
class ResourceOptionsTest {

    @Test
    void givenSharedVectorsWhenEncodingShouldMatchServerContract() {
        for (JsonNode vector : section("update_resource")) {
            String name = vector.get("case").asString();
            assertThat(hex(encodeUpdate(vector)))
                    .as(name)
                    .isEqualTo(vector.get("body_hex").asString());
        }
        for (JsonNode vector : section("create_stream")) {
            JsonNode input = vector.get("input");
            var options = StreamOptions.builder();
            Optional.ofNullable(input.get("encryption_key"))
                    .ifPresent(key ->
                            options.encryptionKey(bytes(key.get("raw_hex").asString())));
            Optional.ofNullable(input.get("raw"))
                    .ifPresent(raw -> raw.properties()
                            .forEach(entry -> options.raw(
                                    entry.getKey(),
                                    HeaderValue.fromString(entry.getValue().asString()))));
            ByteBuf body = BytesSerializer.toCreateStream(
                    input.get("name").asString(), options.build().entries());
            assertThat(hex(body))
                    .as(vector.get("case").asString())
                    .isEqualTo(vector.get("body_hex").asString());
        }
        for (JsonNode vector : section("create_topic")) {
            JsonNode input = vector.get("input");
            var options = TopicOptions.builder()
                    .compressionAlgorithm(CompressionAlgorithm.Gzip)
                    .messageExpiry(new BigInteger(input.get("message_expiry").asString()))
                    .maxTopicSize(new BigInteger(input.get("max_topic_size").asString()))
                    .segmentSize(new BigInteger(input.get("segment_size").asString()))
                    .durability(Durability.PERSISTED)
                    .consumerOffsetDurability(Durability.REPLICATED)
                    .build();
            assertThat(input.get("compression_algorithm").asString()).isEqualTo("gzip");
            ByteBuf body = BytesSerializer.toCreateTopic(
                    StreamId.of(input.get("stream_id").asLong()),
                    input.get("name").asString(),
                    input.get("partitions_count").asLong(),
                    options.entries());
            assertThat(hex(body))
                    .as(vector.get("case").asString())
                    .isEqualTo(vector.get("body_hex").asString());
        }
        for (JsonNode vector : section("create_user")) {
            JsonNode input = vector.get("input");
            ByteBuf body = BytesSerializer.toCreateUser(
                    input.get("username").asString(),
                    input.get("password").asString(),
                    UserStatus.Active,
                    Optional.empty(),
                    UserOptions.empty().entries());
            assertThat(hex(body))
                    .as(vector.get("case").asString())
                    .isEqualTo(vector.get("body_hex").asString());
        }
    }

    @Test
    void givenSharedVectorsWhenSplittingUpdatesShouldMatchWireSetAndReset() {
        for (JsonNode vector : section("update_resource")) {
            JsonNode input = vector.get("input");
            var expectedSet = optionMap(vector.get("wire_set"), true);
            List<String> expectedReset = new ArrayList<>();
            vector.get("wire_reset").forEach(key -> expectedReset.add(key.asString()));
            String name = vector.get("case").asString();
            switch (vector.get("scope").asString()) {
                case "topic" -> {
                    assertThat(topicUpdate(input).entries()).as(name).isEqualTo(expectedSet);
                    assertThat(topicUpdate(input).resetKeys()).as(name).isEqualTo(expectedReset);
                }
                case "stream" -> {
                    assertThat(streamUpdate(input).entries()).as(name).isEqualTo(expectedSet);
                    assertThat(streamUpdate(input).resetKeys()).as(name).isEqualTo(expectedReset);
                }
                default -> {
                    assertThat(userUpdate(input).entries()).as(name).isEqualTo(expectedSet);
                    assertThat(userUpdate(input).resetKeys()).as(name).isEqualTo(expectedReset);
                }
            }
        }
    }

    @Test
    void givenSharedUpdateErrorsWhenBuildingShouldRefuseWithServerCategoryBeforeSending() {
        for (JsonNode vector : section("update_resource_errors")) {
            JsonNode error = vector.get("error");
            if (namesKeyWithoutConstant(vector)) {
                // A reset this build has no constant for travels raw, and the server decides it.
                assertThat(hex(encodeUpdate(vector)))
                        .as(vector.get("case").asString())
                        .isNotEmpty();
                continue;
            }
            assertThatThrownBy(() -> encodeUpdate(vector))
                    .as(vector.get("case").asString())
                    .isInstanceOfSatisfying(IggyValidationException.class, refusal -> {
                        assertThat(refusal.getRawErrorCode())
                                .isEqualTo(error.get("code").asInt());
                        assertThat(refusal.getErrorCode().name().toLowerCase(Locale.ROOT))
                                .isEqualTo(error.get("name").asString());
                    });
        }
    }

    @Test
    void givenValuesTheServerRefusesWhenBuildingShouldReportTheServerCode() {
        assertRefusal(() -> TopicUpdate.builder().name("").build(), IggyErrorCode.INVALID_TOPIC_NAME);
        assertRefusal(() -> StreamUpdate.builder().name("n".repeat(256)).build(), IggyErrorCode.INVALID_STREAM_NAME);
        assertRefusal(() -> UserUpdate.builder().username("ab").build(), IggyErrorCode.INVALID_USERNAME);
        assertRefusal(
                () -> TopicUpdate.builder()
                        .name("")
                        .reset(TopicOptionKey.SEGMENT_SIZE)
                        .build(),
                IggyErrorCode.OPTION_NOT_UPDATABLE);
        assertRefusal(() -> StreamOptions.builder().encryptionKey(new byte[31]), IggyErrorCode.INVALID_ENCRYPTION_KEY);
        assertRefusal(() -> StreamOptions.builder().encryptionKeyBase64("AAAA"), IggyErrorCode.INVALID_ENCRYPTION_KEY);
        assertThat(StreamUpdate.builder().name(" ").build().entries().get(StreamOptionKey.NAME.value()))
                .isEqualTo(new HeaderValue(HeaderKind.String, new byte[] {' '}));
    }

    @Test
    void givenRawResetWhenBuildingUpdateShouldSendItForTheServerToDecide() {
        assertThat(UserUpdate.builder().rawReset("critical_keys").build().resetKeys())
                .containsExactly("critical_keys");
        TopicUpdate topic = TopicUpdate.builder()
                .name("")
                .reset(TopicOptionKey.SEGMENT_SIZE)
                .reset(TopicOptionKey.MESSAGE_EXPIRY)
                .rawReset("message_expiry")
                .rawReset("future_key")
                .build();
        assertThat(topic.resetKeys()).containsExactly("future_key", "message_expiry", "segment_size");
        assertThat(topic.entries().get(TopicOptionKey.NAME.value()).kind()).isEqualTo(HeaderKind.Bool);
        assertRefusal(
                () -> TopicUpdate.builder()
                        .messageExpiry(BigInteger.ONE)
                        .rawReset("message_expiry")
                        .build(),
                IggyErrorCode.INVALID_COMMAND);
        assertRefusal(() -> StreamUpdate.builder().build(), IggyErrorCode.INVALID_COMMAND);
    }

    @Test
    void givenMoreResetNamesThanTheCountByteCarriesWhenBuildingShouldRefuseAnUnsupportedKey() {
        StreamUpdate.Builder atLimit = StreamUpdate.builder();
        futureKeys(255).forEach(atLimit::rawReset);
        ByteBuf payload = BytesSerializer.toUpdateResource(StreamId.of(1L), atLimit.build());
        try {
            int resetCountIndex = 1 + 6;
            assertThat(payload.getUnsignedByte(resetCountIndex)).isEqualTo((short) 255);
        } finally {
            payload.release();
        }

        TopicUpdate.Builder overLimit =
                TopicUpdate.builder().reset(TopicOptionKey.MESSAGE_EXPIRY).rawReset("compression_algorithm");
        futureKeys(255).forEach(overLimit::rawReset);
        assertThatThrownBy(overLimit::build).isInstanceOfSatisfying(IggyValidationException.class, refusal -> {
            assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.UNSUPPORTED_OPTION_KEY);
            assertThat(refusal.getField()).contains("future_000");
        });

        UserUpdate.Builder rawOnly = UserUpdate.builder();
        futureKeys(256).forEach(rawOnly::rawReset);
        assertRefusal(rawOnly::build, IggyErrorCode.UNSUPPORTED_OPTION_KEY);
    }

    @Test
    void givenResetNameTheWireCannotCarryWhenBuildingShouldRefuseAnUnsupportedKey() {
        String longest = "é".repeat(127) + "k";
        ByteBuf payload = BytesSerializer.toUpdateResource(
                StreamId.of(1L), StreamUpdate.builder().rawReset(longest).build());
        payload.release();
        for (String name : List.of("", "é".repeat(128))) {
            assertThatThrownBy(() -> UserUpdate.builder().rawReset(name).build())
                    .isInstanceOfSatisfying(IggyValidationException.class, refusal -> {
                        assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.UNSUPPORTED_OPTION_KEY);
                        assertThat(refusal.getField()).contains(name);
                    });
        }
    }

    @Test
    void givenRawSecretEntriesWhenPrintingOptionsShouldShowNamesWithoutValues() {
        JsonNode key = section("stream_key");
        String keyHex = key.get("key_hex").asString();
        String keyBase64 = key.get("key_base64").asString();
        HeaderValue rawKey = HeaderValue.fromRaw(bytes(keyHex));
        HeaderValue keyText = HeaderValue.fromString(keyBase64);
        List<Object> printed = List.of(
                StreamOptions.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build(),
                StreamUpdate.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build(),
                TopicOptions.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build(),
                TopicUpdate.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build(),
                UserOptions.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build(),
                UserUpdate.builder()
                        .raw("encryption_key", rawKey)
                        .raw("future_secret", keyText)
                        .build());
        assertSoftly(softly -> printed.forEach(options -> softly.assertThat(options.toString())
                .as(options.getClass().getSimpleName())
                .contains("encryption_key", "future_secret")
                .doesNotContain(keyHex)
                .doesNotContain(keyBase64)));
    }

    @Test
    void givenSharedResponsesWhenDecodingShouldMatchProjection() {
        for (JsonNode vector : section("stream_responses")) {
            JsonNode expected = vector.get("expected");
            StreamDetails stream = BytesDeserializer.readStreamDetails(responseOf(vector));
            String name = vector.get("case").asString();
            assertThat(stream.id()).as(name).isEqualTo(expected.get("id").asLong());
            assertThat(stream.createdAt())
                    .as(name)
                    .isEqualTo(new BigInteger(expected.get("created_at").asString()));
            assertThat(stream.name()).as(name).isEqualTo(expected.get("name").asString());
            assertThat(stream.topicsCount())
                    .as(name)
                    .isEqualTo(expected.get("topics_count").asLong());
            assertThat(stream.size())
                    .as(name)
                    .isEqualTo(expected.get("size_bytes").asString());
            assertThat(stream.messagesCount())
                    .as(name)
                    .isEqualTo(BigInteger.valueOf(expected.get("messages_count").asLong()));
            assertThat(stream.options()).as(name).isEqualTo(optionMap(expected.get("options"), true));
            assertThat(stream.derivedOptions()).as(name).isEqualTo(optionMap(expected.get("options"), false));
            assertThat(stream.encryptionKeySource().map(EncryptionKeySource::value))
                    .as(name)
                    .contains(expected.get("encryption_key_source").asString());
            JsonNode fingerprint = expected.get("encryption_key_fingerprint");
            assertThat(stream.encryptionKeyFingerprint())
                    .as(name)
                    .isEqualTo(fingerprint.isNull() ? Optional.empty() : Optional.of(fingerprint.asString()));
            List<String> keks = new ArrayList<>();
            expected.get("encryption_kek_fingerprints").forEach(kek -> keks.add(kek.asString()));
            assertThat(stream.encryptionKekFingerprints()).as(name).isEqualTo(keks);
            assertNoSecretKeys(
                    name, stream.options().keySet(), stream.derivedOptions().keySet());
        }
        for (JsonNode vector : section("topic_responses")) {
            JsonNode expected = vector.get("expected");
            Topic topic = BytesDeserializer.readTopic(responseOf(vector));
            String name = vector.get("case").asString();
            assertThat(topic.id()).as(name).isEqualTo(expected.get("id").asLong());
            assertThat(topic.name()).as(name).isEqualTo(expected.get("name").asString());
            assertThat(topic.partitionsCount())
                    .as(name)
                    .isEqualTo(expected.get("partitions_count").asLong());
            assertThat(topic.size())
                    .as(name)
                    .isEqualTo(expected.get("size_bytes").asString());
            assertThat(topic.options()).as(name).isEqualTo(optionMap(expected.get("options"), true));
            assertThat(topic.derivedOptions()).as(name).isEqualTo(optionMap(expected.get("options"), false));
            assertThat(topic.compressionAlgorithm()).as(name).contains(CompressionAlgorithm.Gzip);
            assertThat(expected.get("compression_algorithm").asString()).isEqualTo("gzip");
            assertThat(topic.messageExpiry())
                    .as(name)
                    .contains(new BigInteger(expected.get("message_expiry").asString()));
            assertThat(topic.maxTopicSize())
                    .as(name)
                    .contains(new BigInteger(expected.get("max_topic_size").asString()));
        }
        for (JsonNode vector : section("user_responses")) {
            JsonNode expected = vector.get("expected");
            UserInfo user = BytesDeserializer.readUserInfo(responseOf(vector));
            String name = vector.get("case").asString();
            assertThat(user.id()).as(name).isEqualTo(expected.get("id").asLong());
            assertThat(user.username())
                    .as(name)
                    .isEqualTo(expected.get("username").asString());
            assertThat(user.status().name().toLowerCase(Locale.ROOT))
                    .as(name)
                    .isEqualTo(expected.get("status").asString());
            assertThat(user.options()).as(name).isEqualTo(optionMap(expected.get("options"), true));
            assertThat(user.derivedOptions()).as(name).isEqualTo(optionMap(expected.get("options"), false));
        }
    }

    @Test
    void givenDescribeVectorsWhenDecodingShouldReportKindsDefaultsAndFlags() {
        for (JsonNode vector : section("describe_options")) {
            String scope = vector.get("scope").asString();
            OptionsScope optionsScope =
                    OptionsScope.valueOf(scope.substring(0, 1).toUpperCase(Locale.ROOT) + scope.substring(1));
            assertThat(HexFormat.of().formatHex(new byte[] {(byte) optionsScope.asCode()}))
                    .isEqualTo(vector.get("request_hex").asString());
            List<OptionSpec> specs = BytesDeserializer.readOptionSpecs(responseOf(vector));
            List<String> expected = new ArrayList<>();
            vector.get("entries")
                    .forEach(entry -> expected.add(describe(
                            entry.get("key").asString(),
                            entry.get("kind").asString(),
                            entry.get("flags").asInt(),
                            entry.get("default").isNull()
                                    ? ""
                                    : entry.get("default").asString())));
            assertThat(specs.stream()
                            .map(spec -> describe(
                                    spec.key(),
                                    spec.defaultValue().kind().name().toLowerCase(Locale.ROOT),
                                    spec.flags(),
                                    spec.defaultValue().value().length == 0
                                            ? ""
                                            : spec.defaultValue().toStringValue()))
                            .toList())
                    .as(scope)
                    .isEqualTo(expected);
        }
    }

    @Test
    void givenLiveCatalogWhenComparingKeyEnumsShouldMatchKeysAndFlags() {
        assertThat(catalogKeys("stream")).isEqualTo(enumKeys(StreamOptionKey.values()));
        assertThat(catalogKeys("topic")).isEqualTo(enumKeys(TopicOptionKey.values()));
        assertThat(catalogKeys("user")).isEqualTo(enumKeys(UserOptionKey.values()));
    }

    @Test
    void givenFixtureCatalogWhenComparingTypedSurfaceShouldExposeEveryKey() {
        Map<String, List<Class<?>>> create = Map.of(
                "stream", List.of(StreamOptions.Builder.class),
                "topic", List.of(TopicOptions.Builder.class),
                "user", List.of(UserOptions.Builder.class));
        Map<String, List<Class<?>>> update = Map.of(
                "stream", List.of(StreamUpdate.Builder.class),
                "topic", List.of(TopicUpdate.Builder.class),
                "user", List.of(UserUpdate.Builder.class));
        Map<String, List<Class<?>>> reads = Map.of(
                "stream", List.of(StreamBase.class, StreamDetails.class),
                "topic", List.of(Topic.class, TopicDetails.class),
                "user", List.of(UserInfo.class, UserInfoDetails.class));
        for (JsonNode row : section("catalog")) {
            if (!row.get("described").asBoolean()) {
                continue;
            }
            String key = row.get("key").asString();
            String mutability = row.get("mutability").asString();
            boolean mapKey = "map".equals(row.get("storage").asString());
            for (JsonNode scopeNode : row.get("scopes")) {
                String scope = scopeNode.asString();
                if (mapKey && !"read_only".equals(mutability)) {
                    assertTypedMember(create.get(scope), key, scope + " create " + key);
                }
                if ("updatable".equals(mutability)) {
                    assertTypedMember(update.get(scope), key, scope + " update " + key);
                }
                if ("read_only".equals(mutability)) {
                    assertTypedMember(reads.get(scope), key, scope + " read " + key);
                }
                if ((row.get("flags").asInt() & OptionSpec.FLAG_RESETTABLE) != 0) {
                    assertThat(scopeKeys(scope)).as(scope + " reset " + key).anySatisfy(constant -> {
                        assertThat(constant.value()).isEqualTo(key);
                        assertThat(constant.resettable()).isTrue();
                    });
                }
            }
        }
    }

    @Test
    void givenFixtureConstantsWhenMirroringShouldMatchCodesKindsAndFlags() {
        JsonNode constants = section("constants");
        assertThat(IggyErrorCode.CANNOT_MODIFY_ROOT_USER.getCode())
                .isEqualTo(constants
                        .get("error_codes")
                        .get("cannot_modify_root_user")
                        .asInt());
        JsonNode codes = constants.get("command_codes");
        assertThat(CommandCode.System.UPDATE_RESOURCE.getValue())
                .isEqualTo(codes.get("update_resource").get("code").asInt());
        assertThat(CommandCode.System.DESCRIBE_OPTIONS.getValue())
                .isEqualTo(codes.get("describe_options").get("code").asInt());
        assertThat(CommandCode.Stream.CREATE.getValue())
                .isEqualTo(codes.get("create_stream").get("code").asInt());
        assertThat(CommandCode.Topic.CREATE.getValue())
                .isEqualTo(codes.get("create_topic").get("code").asInt());
        assertThat(CommandCode.User.CREATE.getValue())
                .isEqualTo(codes.get("create_user").get("code").asInt());
        List<Integer> liveCodes = new ArrayList<>();
        for (Class<?> group : CommandCode.class.getDeclaredClasses()) {
            for (Object code : group.getEnumConstants()) {
                liveCodes.add(((CommandCode) code).getValue());
            }
        }
        constants
                .get("retired_command_codes")
                .forEach(code -> assertThat(liveCodes).doesNotContain(code.asInt()));
        JsonNode scopes = constants.get("scope_codes");
        assertThat(OptionsScope.Topic.asCode()).isEqualTo(scopes.get("topic").asInt());
        assertThat(OptionsScope.Stream.asCode()).isEqualTo(scopes.get("stream").asInt());
        assertThat(OptionsScope.User.asCode()).isEqualTo(scopes.get("user").asInt());
        constants
                .get("header_kind_codes")
                .properties()
                .forEach(kind -> assertThat(kind(kind.getKey()).asCode())
                        .isEqualTo(kind.getValue().asInt()));
        JsonNode flags = constants.get("option_flags");
        assertThat(OptionSpec.FLAG_UPDATABLE).isEqualTo(flags.get("updatable").asInt());
        assertThat(OptionSpec.FLAG_RESETTABLE).isEqualTo(flags.get("resettable").asInt());
        assertThat(OptionSpec.FLAG_SECRET).isEqualTo(flags.get("secret").asInt());
        assertThat(OptionSpec.FLAG_READ_ONLY).isEqualTo(flags.get("read_only").asInt());
        assertThat(OptionSpec.FLAG_CRITICAL).isEqualTo(flags.get("critical").asInt());
        assertThat(UserStatus.Active.asCode())
                .isEqualTo(constants.get("user_status_codes").get("active").asInt());
        assertThat(UserStatus.Inactive.asCode())
                .isEqualTo(constants.get("user_status_codes").get("inactive").asInt());
        assertThat(EncryptionKeys.KEY_LENGTH)
                .isEqualTo(constants.get("stream_key_length").asInt());
    }

    @Test
    void givenFixedKeyWhenFingerprintingShouldMatchFixture() {
        JsonNode key = section("stream_key");
        String expected = key.get("fingerprint").asString();

        assertThat(EncryptionKeys.fingerprint(bytes(key.get("key_hex").asString())))
                .isEqualTo(expected);
        assertThat(EncryptionKeys.fingerprint(key.get("key_base64").asString())).isEqualTo(expected);
        assertThat(EncryptionKeys.decodeBase64(key.get("key_base64").asString()))
                .isEqualTo(bytes(key.get("key_hex").asString()));
    }

    @Test
    void givenInvalidKeyWhenEncodingShouldRejectBeforeNetworkIo() {
        for (JsonNode vector : section("stream_key").get("invalid_keys")) {
            JsonNode input = vector.get("input");
            assertThat(vector.get("error").get("code").asInt())
                    .isEqualTo(IggyErrorCode.INVALID_ENCRYPTION_KEY.getCode());
            if (input.has("raw_hex")) {
                byte[] key = bytes(input.get("raw_hex").asString());
                assertThatThrownBy(() -> StreamOptions.builder().encryptionKey(key))
                        .as(vector.get("case").asString())
                        .isInstanceOfSatisfying(IggyValidationException.class, ResourceOptionsTest::assertInvalidKey);
                assertThatThrownBy(() -> EncryptionKeys.fingerprint(key))
                        .isInstanceOfSatisfying(IggyValidationException.class, ResourceOptionsTest::assertInvalidKey);
            } else {
                String key = input.get("base64").asString();
                assertThatThrownBy(() -> StreamOptions.builder().encryptionKeyBase64(key))
                        .as(vector.get("case").asString())
                        .isInstanceOfSatisfying(IggyValidationException.class, ResourceOptionsTest::assertInvalidKey)
                        .satisfies(error -> assertThat(error.getMessage()).doesNotContain(key.strip()));
                assertThatThrownBy(() -> EncryptionKeys.fingerprint(key))
                        .isInstanceOfSatisfying(IggyValidationException.class, ResourceOptionsTest::assertInvalidKey);
            }
        }
        JsonNode valid = section("stream_key");
        byte[] key = bytes(valid.get("key_hex").asString());
        StreamOptions options = StreamOptions.builder().encryptionKey(key).build();
        assertThat(options.encryptionKey()).hasValue(key);
        assertThat(options.toString())
                .doesNotContain(valid.get("key_base64").asString())
                .doesNotContain(valid.get("key_hex").asString())
                .contains("<redacted>");
        assertThat(StreamOptions.builder()
                        .encryptionKeyBase64(valid.get("key_base64").asString())
                        .build()
                        .encryptionKey())
                .hasValue(key);
        assertThat(options.entries().get(StreamOptionKey.ENCRYPTION_KEY.value()))
                .isEqualTo(new HeaderValue(HeaderKind.Raw, key));
    }

    private static void assertInvalidKey(IggyValidationException refusal) {
        assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_ENCRYPTION_KEY);
    }

    private static void assertTypedMember(List<Class<?>> owners, String key, String description) {
        String[] words = key.split("_");
        StringBuilder camel = new StringBuilder(words[0]);
        for (int index = 1; index < words.length; index++) {
            camel.append(Character.toUpperCase(words[index].charAt(0))).append(words[index].substring(1));
        }
        String member = camel.toString();
        assertThat(owners.stream().flatMap(owner -> Arrays.stream(owner.getMethods())))
                .as(description)
                .anyMatch(method ->
                        method.getName().equals(member) || method.getName().equals(member + "s"));
    }

    private static OptionKey[] scopeKeys(String scope) {
        return switch (scope) {
            case "topic" -> TopicOptionKey.values();
            case "stream" -> StreamOptionKey.values();
            default -> UserOptionKey.values();
        };
    }

    private static boolean namesKeyWithoutConstant(JsonNode vector) {
        OptionKey[] keys = scopeKeys(vector.get("scope").asString());
        JsonNode reset = vector.get("input").get("reset");
        if (reset == null) {
            return false;
        }
        for (JsonNode name : reset) {
            if (Arrays.stream(keys).noneMatch(key -> key.value().equals(name.asString()))) {
                return true;
            }
        }
        return false;
    }

    private static List<String> futureKeys(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> String.format(Locale.ROOT, "future_%03d", index))
                .toList();
    }

    private static void assertRefusal(ThrowingCallable build, IggyErrorCode expected) {
        assertThatThrownBy(build)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(expected));
    }

    private static ByteBuf encodeUpdate(JsonNode vector) {
        JsonNode target = vector.get("target");
        JsonNode input = vector.get("input");
        return switch (vector.get("scope").asString()) {
            case "topic" -> BytesSerializer.toUpdateResource(streamId(target), topicId(target), topicUpdate(input));
            case "stream" -> BytesSerializer.toUpdateResource(streamId(target), streamUpdate(input));
            default -> BytesSerializer.toUpdateResource(userId(target), userUpdate(input));
        };
    }

    private static ByteBuf responseOf(JsonNode vector) {
        return Unpooled.wrappedBuffer(bytes(vector.get("response_hex").asString()));
    }

    private static String describe(String key, String kind, int flags, String defaultValue) {
        return key + " " + kind + " " + flags + " " + defaultValue;
    }

    private static List<String> catalogKeys(String scope) {
        List<String> keys = new ArrayList<>();
        for (JsonNode row : section("catalog")) {
            boolean inScope = false;
            for (JsonNode rowScope : row.get("scopes")) {
                inScope |= rowScope.asString().equals(scope);
            }
            if (inScope && row.get("described").asBoolean()) {
                keys.add(row.get("key").asString() + " " + row.get("flags").asInt());
            }
        }
        return keys;
    }

    private static List<String> enumKeys(OptionKey[] keys) {
        List<String> described = new ArrayList<>();
        for (OptionKey key : keys) {
            described.add(key.value() + " " + key.flags());
        }
        return described;
    }

    private static void assertNoSecretKeys(String name, Set<String> explicit, Set<String> derived) {
        section("constants").get("never_returned_keys").forEach(secret -> {
            assertThat(explicit).as(name).doesNotContain(secret.asString());
            assertThat(derived).as(name).doesNotContain(secret.asString());
        });
    }
}
