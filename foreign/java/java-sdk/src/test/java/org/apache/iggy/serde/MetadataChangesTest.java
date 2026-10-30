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
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeKind;
import org.apache.iggy.system.MetadataChangePhase;
import org.apache.iggy.system.MetadataChangeStart;
import org.apache.iggy.system.MetadataChangeStatus;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.MetadataCurrentPageToken;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import static org.apache.iggy.MetadataChangesFixture.assertAnswer;
import static org.apache.iggy.MetadataChangesFixture.assertItem;
import static org.apache.iggy.MetadataChangesFixture.assertRecord;
import static org.apache.iggy.MetadataChangesFixture.buffer;
import static org.apache.iggy.MetadataChangesFixture.bytes;
import static org.apache.iggy.MetadataChangesFixture.find;
import static org.apache.iggy.MetadataChangesFixture.query;
import static org.apache.iggy.MetadataChangesFixture.refused;
import static org.apache.iggy.MetadataChangesFixture.section;
import static org.apache.iggy.ResourceOptionsFixture.hex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared metadata changes vectors against this SDK's own encoder and decoder. Every request must match the Rust
 * encoder byte for byte and every response must decode to the values the server renders as JSON.
 */
class MetadataChangesTest {

    @Test
    void givenManyDuplicateGroupsWhenEncodingShouldPreserveEveryRequestedGroup() {
        var groups = new ArrayList<MetadataChangeGroup>();
        for (int index = 0; index < 255; index++) {
            groups.add(MetadataChangeGroup.STREAMS);
        }
        groups.add(MetadataChangeGroup.USERS);
        var actual = BytesSerializer.toGetMetadataChanges(MetadataChangesQuery.builder()
                .groups(groups.toArray(MetadataChangeGroup[]::new))
                .build());
        var expected = BytesSerializer.toGetMetadataChanges(MetadataChangesQuery.builder()
                .groups(MetadataChangeGroup.STREAMS, MetadataChangeGroup.USERS)
                .build());
        try {
            assertThat(actual).isEqualTo(expected);
        } finally {
            actual.release();
            expected.release();
        }
    }

    @Test
    void givenSharedConstantsWhenComparingShouldMatchTheServerCatalog() {
        JsonNode constants = section("constants");
        assertThat(CommandCode.System.GET_METADATA_CHANGES.getValue())
                .isEqualTo(constants.get("command_code").asInt());
        assertThat(MetadataChangeCursor.PREFIX)
                .isEqualTo(constants.get("cursor_prefix").asString());
        assertThat(MetadataCurrentPageToken.PREFIX)
                .isEqualTo(constants.get("page_token_prefix").asString());
        assertThat(MetadataChangeCursor.SIZE)
                .isEqualTo(constants.get("cursor_size").asInt());
        assertThat(MetadataCurrentPageToken.MAX_SIZE)
                .isEqualTo(constants.get("max_page_token_size").asInt());
        assertThat(MetadataCurrentPageToken.MAX_KEY_SIZE)
                .isEqualTo(constants.get("max_page_token_key_size").asInt());
        assertThat(MetadataChangesQuery.DEFAULT_LIMIT)
                .isEqualTo(constants.get("default_limit").asLong());
        assertThat(MetadataChangesQuery.MAX_LIMIT)
                .isEqualTo(constants.get("max_limit").asLong());
        assertThat(MetadataChangesQuery.builder().build().limit()).isEqualTo(MetadataChangesQuery.DEFAULT_LIMIT);
        var zero = new MetadataChangeCursor(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO, 0);
        assertThat(zero.toString()).hasSize(constants.get("cursor_text_length").asInt());
        constants.get("groups").properties().forEach(entry -> {
            var group = MetadataChangeGroup.fromValue(entry.getKey()).orElseThrow();
            assertThat(group.asCode()).isEqualTo(entry.getValue().get("code").asInt());
            List<String> parents = new ArrayList<>();
            entry.getValue().get("parents").forEach(parent -> parents.add(parent.asString()));
            assertThat(group.parents().stream().map(MetadataChangeGroup::value).toList())
                    .isEqualTo(parents);
        });
        assertThat(constants.get("groups").size()).isEqualTo(MetadataChangeGroup.values().length);
        constants.get("kinds").properties().forEach(entry -> {
            var kind = MetadataChangeKind.fromValue(entry.getKey()).orElseThrow();
            assertThat(kind.asCode()).isEqualTo(entry.getValue().get("code").asInt());
            assertThat(kind.group().value())
                    .isEqualTo(entry.getValue().get("group").asString());
        });
        assertThat(constants.get("kinds").size()).isEqualTo(MetadataChangeKind.values().length);
        assertThat(codes(MetadataChangeStatus.values(), MetadataChangeStatus::value, MetadataChangeStatus::asCode))
                .isEqualTo(codes(constants.get("statuses")));
        assertThat(codes(MetadataChangePhase.values(), MetadataChangePhase::value, MetadataChangePhase::asCode))
                .isEqualTo(codes(constants.get("phases")));
        assertThat(codes(
                        MetadataChangeBody.MemberLeftReason.values(),
                        MetadataChangeBody.MemberLeftReason::value,
                        MetadataChangeBody.MemberLeftReason::asCode))
                .isEqualTo(codes(constants.get("member_left_reasons")));
        assertThat(codes(
                        MetadataChangeBody.TokenDeletedReason.values(),
                        MetadataChangeBody.TokenDeletedReason::value,
                        MetadataChangeBody.TokenDeletedReason::asCode))
                .isEqualTo(codes(constants.get("token_deleted_reasons")));
        assertThat(codes(
                        MetadataChangeBody.SessionClosedReason.values(),
                        MetadataChangeBody.SessionClosedReason::value,
                        MetadataChangeBody.SessionClosedReason::asCode))
                .isEqualTo(codes(constants.get("session_closed_reasons")));
        assertThat(codes(
                        MetadataChangeBody.PermissionsUpdatedReason.values(),
                        MetadataChangeBody.PermissionsUpdatedReason::value,
                        MetadataChangeBody.PermissionsUpdatedReason::asCode))
                .isEqualTo(codes(constants.get("permissions_updated_reasons")));
        Map<String, Integer> startModes = Map.of(
                "earliest", MetadataChangeStart.earliest().asCode(),
                "latest", MetadataChangeStart.latest().asCode(),
                "after", MetadataChangeStart.after(zero).asCode(),
                "current", MetadataChangeStart.current().asCode());
        assertThat(new TreeMap<>(startModes)).isEqualTo(codes(constants.get("start_modes")));
    }

    @Test
    void givenSharedRecordsWhenDecodingShouldReachTheServerJson() {
        for (JsonNode vector : section("records")) {
            String name = vector.get("case").asString();
            var record = BytesDeserializer.readMetadataChangeRecord(
                    buffer(vector.get("record_hex").asString()));
            var body = BytesDeserializer.readMetadataChangeBody(
                    vector.get("group").asInt(),
                    vector.get("kind").asInt(),
                    buffer(vector.get("body_hex").asString()));

            assertRecord(vector.get("expected"), record);
            assertThat(body).as(name).isEqualTo(record.body());
            assertThat(record.kind().orElseThrow().asCode())
                    .as(name)
                    .isEqualTo(vector.get("kind").asInt());
        }
    }

    @Test
    void givenSharedItemsWhenDecodingShouldReachTheServerJson() {
        for (JsonNode vector : section("items")) {
            var item = BytesDeserializer.readMetadataCurrentItem(
                    buffer(vector.get("item_hex").asString()));
            var body = BytesDeserializer.readMetadataItemBody(
                    vector.get("group").asInt(), buffer(vector.get("body_hex").asString()));

            assertItem(vector.get("expected"), item);
            assertThat(body).as(vector.get("case").asString()).isEqualTo(item.body());
        }
    }

    @Test
    void givenSharedResponsesWhenDecodingShouldReachTheServerJson() {
        for (JsonNode vector : section("responses")) {
            var answer = BytesDeserializer.readMetadataChanges(
                    buffer(vector.get("response_hex").asString()));

            assertAnswer(vector.get("expected"), answer);
        }
    }

    @Test
    void givenLargeValuesWhenDecodingShouldKeepEveryUnsignedBitExact() {
        JsonNode vector = section("large_values");

        var record = BytesDeserializer.readMetadataChangeRecord(
                buffer(vector.get("record_hex").asString()));

        assertThat(record.op()).isEqualTo(new BigInteger(vector.get("op").asString()));
        assertThat(record.ordinal()).isEqualTo(vector.get("ordinal").asLong());
        assertThat(record.timestamp())
                .isEqualTo(new BigInteger(vector.get("timestamp").asString()));
        assertThat(record.checksum())
                .isEqualTo(new BigInteger(vector.get("checksum").asString(), 16));
        var session = (MetadataChangeBody.Session) record.body();
        assertThat(session.userCreatedAt())
                .isEqualTo(new BigInteger(vector.get("user_created_at").asString()));
        assertThat(session.session())
                .isEqualTo(new BigInteger(vector.get("session").asString()));
        assertThat(session.clientId())
                .isEqualTo(new BigInteger(vector.get("client_id").asString(), 16));
    }

    @Test
    void givenSharedCursorsWhenParsingShouldRoundTripTheirTextAndBytes() {
        for (JsonNode vector : section("cursors")) {
            String text = vector.get("text").asString();

            var cursor = MetadataChangeCursor.parse(text);

            assertThat(cursor.logId())
                    .isEqualTo(new BigInteger(vector.get("log_id").asString(), 16));
            assertThat(cursor.op()).isEqualTo(vector.get("op").bigIntegerValue());
            assertThat(cursor.checksum())
                    .isEqualTo(new BigInteger(vector.get("checksum").asString(), 16));
            assertThat(cursor.ordinal()).isEqualTo(vector.get("ordinal").asLong());
            assertThat(cursor.toString()).isEqualTo(text);
            assertThat(cursor.toBytes()).isEqualTo(bytes(vector.get("hex").asString()));
        }
    }

    @Test
    void givenSharedPageTokensWhenParsingShouldExposeEveryField() {
        for (JsonNode vector : section("page_tokens")) {
            String text = vector.get("text").asString();

            var token = MetadataCurrentPageToken.parse(text);

            assertThat(token.anchor().toString()).isEqualTo(vector.get("anchor").asString());
            assertThat(token.requiredHead())
                    .isEqualTo(vector.get("required_head").bigIntegerValue());
            assertThat(token.groupsMask()).isEqualTo(vector.get("groups_mask").asInt());
            assertThat(token.groups()).isEqualTo(MetadataChangeGroup.fromMask(token.groupsMask()));
            assertThat(token.lastGroup().value()).isEqualTo(vector.get("group").asString());
            assertThat(token.lastKey())
                    .isEqualTo(bytes(vector.get("last_key_hex").asString()));
            assertThat(token.payload()).isEqualTo(bytes(vector.get("hex").asString()));
            assertThat(token.toString()).isEqualTo(text);
            assertThat(token).isEqualTo(MetadataCurrentPageToken.parse(text));
        }
    }

    @Test
    void givenSharedRequestsWhenEncodingShouldMatchTheRustEncoderAndHttpQuery() {
        for (JsonNode vector : section("requests")) {
            String name = vector.get("case").asString();
            var query = query(vector.get("input"));

            query.validate();

            assertThat(hex(BytesSerializer.toGetMetadataChanges(query)))
                    .as(name)
                    .isEqualTo(vector.get("request_hex").asString());
            assertThat(query.httpQuery())
                    .as(name)
                    .isEqualTo(vector.get("http_query").asString());
        }
    }

    @Test
    void givenRefusedCursorsAndTokensWhenParsingShouldRefuseThemAsInvalidCommand() {
        for (JsonNode vector : refused("cursors")) {
            assertRefused(() -> MetadataChangeCursor.parse(vector.get("text").asString()));
        }
        for (JsonNode vector : refused("page_tokens")) {
            assertRefused(
                    () -> MetadataCurrentPageToken.parse(vector.get("text").asString()));
        }
    }

    @Test
    void givenRefusedBodiesAndResponsesWhenDecodingShouldRefuseThemAsMalformed() {
        for (JsonNode vector : refused("record_bodies")) {
            assertThatThrownBy(() -> BytesDeserializer.readMetadataChangeBody(
                            vector.get("group").asInt(),
                            vector.get("kind").asInt(),
                            buffer(vector.get("body_hex").asString())))
                    .as(vector.get("case").asString())
                    .isInstanceOf(IggyMalformedResponseException.class);
        }
        for (JsonNode vector : refused("responses")) {
            assertThatThrownBy(() -> BytesDeserializer.readMetadataChanges(
                            buffer(vector.get("hex").asString())))
                    .as(vector.get("case").asString())
                    .isInstanceOf(IggyMalformedResponseException.class);
        }
    }

    @Test
    void givenRefusedQueriesWhenValidatingShouldRefuseThemBeforeSending() {
        for (JsonNode vector : refused("queries")) {
            var query = query(vector.get("input"));
            assertRefused(query::validate);
        }
        // Of the refused request payloads the SDK can only express a cursor that is not canonical.
        var request = bytes(
                find(refused("requests"), "non_canonical_cursor").get("hex").asString());
        var cursor = MetadataChangeCursor.fromBytes(Arrays.copyOfRange(request, 1, 45), 0);
        assertRefused(() -> MetadataChangesQuery.builder()
                .start(MetadataChangeStart.after(cursor))
                .build()
                .validate());
        assertRefused(() -> MetadataChangesQuery.builder()
                .since(BigInteger.ONE.shiftLeft(64))
                .build()
                .validate());
    }

    @Test
    void givenEveryRecordBodyWhenTruncatingItShouldRefuseEveryPrefixAndATrailingByte() {
        for (JsonNode vector : section("records")) {
            byte[] body = bytes(vector.get("body_hex").asString());
            int group = vector.get("group").asInt();
            int kind = vector.get("kind").asInt();
            for (int length = 0; length < body.length; length++) {
                ByteBuf prefix = Unpooled.wrappedBuffer(body, 0, length);
                assertThatThrownBy(() -> BytesDeserializer.readMetadataChangeBody(group, kind, prefix))
                        .as(vector.get("case").asString() + " truncated to " + length)
                        .isInstanceOf(IggyMalformedResponseException.class);
            }
            ByteBuf trailing = Unpooled.wrappedBuffer(Arrays.copyOf(body, body.length + 1));
            assertThatThrownBy(() -> BytesDeserializer.readMetadataChangeBody(group, kind, trailing))
                    .isInstanceOf(IggyMalformedResponseException.class);
        }
    }

    @Test
    void givenUnknownGroupOrKindWhenDecodingShouldKeepTheCodesAndRawBody() {
        byte[] raw = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};

        var change = BytesDeserializer.readMetadataChangeBody(9, 1, Unpooled.wrappedBuffer(raw));
        var item = BytesDeserializer.readMetadataItemBody(128, Unpooled.wrappedBuffer(raw));

        var unknownChange = (MetadataChangeBody.Unknown) change;
        assertThat(unknownChange.group()).isEqualTo(9);
        assertThat(unknownChange.kind()).contains(1);
        assertThat(unknownChange.body()).isEqualTo(raw);
        var unknownItem = (MetadataChangeBody.Unknown) item;
        assertThat(unknownItem.group()).isEqualTo(128);
        assertThat(unknownItem.kind()).isEmpty();
    }

    @Test
    void givenUnknownBodiesWithEqualBytesWhenComparingShouldBeEqualAndKeepTheirOwnCopy() {
        byte[] raw = {1, 2, 3};
        var unknown = new MetadataChangeBody.Unknown(9, Optional.of(1), raw);
        var same = new MetadataChangeBody.Unknown(9, Optional.of(1), new byte[] {1, 2, 3});

        raw[0] = 7;
        unknown.body()[1] = 7;

        assertThat(unknown).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(unknown.body()).containsExactly(1, 2, 3);
        assertThat(unknown).isNotEqualTo(new MetadataChangeBody.Unknown(9, Optional.empty(), new byte[] {1, 2, 3}));
    }

    @Test
    void givenGroupsInAnyOrderWhenEncodingShouldKeepBinaryOrderAndSortTheHttpNames() {
        var query = MetadataChangesQuery.builder()
                .groups(MetadataChangeGroup.USERS, MetadataChangeGroup.STREAMS, MetadataChangeGroup.USERS)
                .build();

        String request = hex(BytesSerializer.toGetMetadataChanges(query));

        assertThat(request).endsWith("030601060000");
        assertThat(query.httpQuery()).isEqualTo("from=earliest&groups=streams,users&limit=100");
    }

    private static void assertRefused(ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }

    private static <T> Map<String, Integer> codes(T[] values, Function<T, String> name, ToIntFunction<T> code) {
        Map<String, Integer> codes = new TreeMap<>();
        for (T value : values) {
            codes.put(name.apply(value), code.applyAsInt(value));
        }
        return codes;
    }

    private static Map<String, Integer> codes(JsonNode node) {
        Map<String, Integer> codes = new TreeMap<>();
        node.properties()
                .forEach(entry -> codes.put(entry.getKey(), entry.getValue().asInt()));
        return codes;
    }
}
