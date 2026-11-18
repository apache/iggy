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

import org.apache.iggy.ListPagesFixture.Item;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageOrder;
import org.apache.iggy.system.PageQuery;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.iggy.ListPagesFixture.httpQuery;
import static org.apache.iggy.ListPagesFixture.items;
import static org.apache.iggy.ListPagesFixture.namedQuery;
import static org.apache.iggy.ListPagesFixture.query;
import static org.apache.iggy.ListPagesFixture.readPage;
import static org.apache.iggy.ListPagesFixture.refused;
import static org.apache.iggy.ListPagesFixture.section;
import static org.apache.iggy.ListPagesFixture.streamId;
import static org.apache.iggy.ListPagesFixture.topicId;
import static org.apache.iggy.MetadataChangesFixture.buffer;
import static org.apache.iggy.MetadataChangesFixture.bytes;
import static org.apache.iggy.ResourceOptionsFixture.hex;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shared list page vectors against this SDK's own types, encoder and decoder. Every request must match the Rust
 * encoder byte for byte, and every response must decode to the ids, names and next cursor of its vector.
 */
class ListPagesTest {

    /** Order id, ascending and the default limit: the page block before its cursor. */
    private static final String DEFAULT_QUERY_HEX = "000064000000";

    private static final String NO_NAME_HEX = "00";

    @Test
    void givenSharedConstantsWhenComparingShouldMatchTheSdk() {
        JsonNode constants = section("constants");
        JsonNode codes = constants.get("codes");

        assertThat(codes.get("get_streams").asInt()).isEqualTo(CommandCode.Stream.GET_ALL.getValue());
        assertThat(codes.get("get_topics").asInt()).isEqualTo(CommandCode.Topic.GET_ALL.getValue());
        assertThat(codes.get("get_partitions").asInt()).isEqualTo(CommandCode.Partition.GET_ALL.getValue());
        assertThat(codes.get("get_consumer_groups").asInt()).isEqualTo(CommandCode.ConsumerGroup.GET_ALL.getValue());
        assertThat(codes.get("get_users").asInt()).isEqualTo(CommandCode.User.GET_ALL.getValue());
        codes.properties()
                .forEach(code -> assertThat(
                                VsrOperation.operationForCode(code.getValue().asInt()))
                        .as(code.getKey())
                        .isEqualTo(VsrOperation.NON_REPLICATED));
        assertThat(constants.get("orders").get("id").asInt()).isEqualTo(PageOrder.ID.asCode());
        assertThat(constants.get("orders").get("created").asInt()).isEqualTo(PageOrder.CREATED.asCode());
        assertThat(constants.get("cursor_prefix").asString()).isEqualTo(PageCursor.PREFIX);
        assertThat(constants.get("max_cursor_bytes").asInt()).isEqualTo(PageCursor.MAX_SIZE);
        assertThat(constants.get("default_limit").asLong())
                .isEqualTo(PageQuery.DEFAULT_LIMIT)
                .isEqualTo(PageQuery.builder().build().limit());
        assertThat(constants.get("max_limit").asLong()).isEqualTo(PageQuery.MAX_LIMIT);
        assertThat(constants.get("max_name_bytes").asInt()).isEqualTo(NamedPageQuery.MAX_NAME_SIZE);
        assertThat(constants.get("errors").get("invalid_command").asInt())
                .isEqualTo(IggyErrorCode.INVALID_COMMAND.getCode());
        assertThat(constants.get("errors").get("reply_too_large").asInt())
                .isEqualTo(IggyErrorCode.REPLY_TOO_LARGE.getCode());
    }

    @Test
    void givenSharedAndOpaqueCursorsWhenParsingShouldRoundTripTheirTextAndBytes() {
        for (String group : List.of("cursors", "opaque_cursors")) {
            for (JsonNode vector : section(group)) {
                String name = vector.get("case").asString();
                String text = vector.get("text").asString();
                byte[] cursorBytes = bytes(vector.get("hex").asString());

                PageCursor cursor = PageCursor.parse(text);

                assertThat(cursor.toString()).as(name).isEqualTo(text);
                assertThat(cursor.toBytes()).as(name).isEqualTo(cursorBytes);
                assertThat(PageCursor.fromBytes(cursorBytes)).as(name).contains(cursor);
            }
        }
    }

    /** The partitions page methods send their {@link PageQuery} as a query without a name. */
    @Test
    void givenSharedRequestsWhenEncodingShouldMatchTheRustEncoderAndHttpQuery() {
        for (JsonNode vector : section("requests")) {
            String name = vector.get("case").asString();
            JsonNode input = vector.get("input");
            NamedPageQuery query = vector.get("command").asString().equals("get_partitions")
                    ? new NamedPageQuery(query(input))
                    : namedQuery(input);

            query.validate();

            assertThat(hex(BytesSerializer.toPageRequest(query, parents(input))))
                    .as(name)
                    .isEqualTo(vector.get("request_hex").asString());
            assertThat(query.httpQuery()).as(name).isEqualTo(httpQuery(vector));
        }
    }

    @Test
    void givenSharedAndOpaqueResponsesWhenDecodingShouldKeepTheirItemsAndNextCursor() {
        for (String group : List.of("responses", "opaque_responses")) {
            for (JsonNode vector : section(group)) {
                String name = vector.get("case").asString();
                JsonNode next = vector.get("next");

                Page<Item> page = readPage(
                        vector.get("command").asString(),
                        buffer(vector.get("response_hex").asString()));

                assertThat(page.items()).as(name).isEqualTo(items(vector));
                assertThat(page.next().map(PageCursor::toString))
                        .as(name)
                        .isEqualTo(next.isNull() ? Optional.empty() : Optional.of(next.asString()));
            }
        }
    }

    @Test
    void givenOpaqueCursorsAndNextsWhenSendingThemBackShouldKeepTheirBytes() {
        for (JsonNode vector : section("opaque_cursors")) {
            assertSentBack(
                    vector.get("case").asString(),
                    PageCursor.parse(vector.get("text").asString()),
                    vector.get("hex").asString());
        }
        for (JsonNode vector : section("opaque_responses")) {
            String reply = vector.get("response_hex").asString();
            int nextSize = HexFormat.fromHexDigits(reply, 0, 2);
            Page<Item> page = readPage(vector.get("command").asString(), buffer(reply));

            assertSentBack(
                    vector.get("case").asString(), page.next().orElseThrow(), reply.substring(2, 2 + nextSize * 2));
        }
    }

    @Test
    void givenRefusedCursorsWhenParsingShouldRefuseThemAsInvalidCommand() {
        for (JsonNode vector : refused("cursors")) {
            assertRefused(() -> PageCursor.parse(vector.get("text").asString()));
        }
        assertThat(PageCursor.fromBytes(new byte[0])).isEmpty();
        assertThat(PageCursor.fromBytes(new byte[PageCursor.MAX_SIZE + 1])).isEmpty();
    }

    @Test
    void givenRefusedQueriesWhenValidatingShouldRefuseThemBeforeSending() {
        for (JsonNode vector : refused("queries")) {
            JsonNode input = vector.get("input");
            assertRefused(namedQuery(input)::validate);
            if (input.get("name").isNull()) {
                assertRefused(query(input)::validate);
            }
        }
        NamedPageQuery loneSurrogate = new NamedPageQuery(PageQuery.builder().build(), Optional.of("\uD800"));
        assertRefused(loneSurrogate::validate);
    }

    @Test
    void givenRefusedResponsesWhenDecodingShouldRefuseThemAsMalformed() {
        for (JsonNode vector : refused("responses")) {
            assertThatThrownBy(() -> readPage(
                            vector.get("command").asString(),
                            buffer(vector.get("response_hex").asString())))
                    .as(vector.get("case").asString())
                    .isInstanceOf(IggyMalformedResponseException.class);
        }
    }

    private static Identifier[] parents(JsonNode input) {
        return Stream.concat(streamId(input).stream(), topicId(input).stream()).toArray(Identifier[]::new);
    }

    /** The validate-then-encode path of a list method keeps the bytes of a cursor that it cannot read. */
    private static void assertSentBack(String name, PageCursor cursor, String cursorHex) {
        NamedPageQuery query =
                new NamedPageQuery(PageQuery.builder().cursor(cursor).build());

        query.validate();

        assertThat(hex(BytesSerializer.toPageRequest(query)))
                .as(name)
                .isEqualTo(DEFAULT_QUERY_HEX
                        + HexFormat.of().toHexDigits((byte) (cursorHex.length() / 2))
                        + cursorHex
                        + NO_NAME_HEX);
    }

    private static void assertRefused(ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }
}
