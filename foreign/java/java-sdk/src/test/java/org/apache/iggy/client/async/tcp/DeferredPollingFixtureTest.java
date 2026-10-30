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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The deferred polling vectors that the Rust encoders generate, shared with every SDK, so the wire here is the
 * server's contract and not this SDK's encoder run twice.
 */
class DeferredPollingFixtureTest {
    private static final JsonNode ROOT = load();
    private static final HexFormat HEX = HexFormat.of();

    @Test
    void shouldMatchTheConstantsOfTheServer() {
        JsonNode constants = ROOT.get("constants");

        assertThat(constants.get("codes").get("poll_messages_deferred").asInt())
                .isEqualTo(CommandCode.Messages.POLL_DEFERRED.getValue());
        assertThat(constants.get("codes").get("fetch_messages").asInt())
                .isEqualTo(CommandCode.Messages.FETCH_MESSAGES.getValue());
        assertThat(constants.get("codes").get("cancel_deferred_request").asInt())
                .isEqualTo(CommandCode.Messages.CANCEL_DEFERRED_REQUEST.getValue());
        assertThat(constants.get("min_deferred_poll_max_bytes").asLong()).isEqualTo(DeferredPollOptions.MIN_MAX_BYTES);
        assertThat(constants.get("max_record_size").asLong()).isEqualTo(DeferredPollOptions.MAX_RECORD_SIZE);
        assertThat(constants.get("max_record_reply_size").asLong())
                .isEqualTo(AsyncIggyTcpClientBuilder.MIN_MAX_VSR_FRAME_SIZE);
        assertThat(constants.get("fetch_section_header_size").asInt()).isEqualTo(BytesDeserializer.FETCH_SECTION_BYTES);
    }

    @Test
    void shouldEncodeEachDeferredPollOfTheFixture() {
        for (JsonNode vector : ROOT.get("deferred_polls")) {
            JsonNode request = vector.get("request");
            ByteBuf body = MessagesTcpClient.pollBody(
                    streamId(request.get("stream_id")),
                    topicId(request.get("topic_id")),
                    Optional.of(request.get("partition_id").asLong()),
                    consumer(request.get("consumer")),
                    strategy(request.get("strategy")),
                    request.get("count").asLong(),
                    request.get("auto_commit").asBoolean(),
                    options(request));

            assertThat(hex(body))
                    .as(vector.get("case").asString())
                    .isEqualTo(vector.get("body_hex").asString());
        }
    }

    /** This SDK sends the byte limit of a fetch as the cap of each target, so the vector caps are set after it. */
    @Test
    void shouldEncodeEachFetchOfTheFixture() {
        for (JsonNode vector : ROOT.get("fetches")) {
            JsonNode request = vector.get("request");
            Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
            for (JsonNode target : request.get("targets")) {
                PartitionContext fence = new PartitionContext(
                        BigInteger.valueOf(target.get("incarnation").asLong()),
                        BigInteger.valueOf(target.get("owner_generation").asLong()),
                        BigInteger.ZERO);
                targets.put(
                        target.get("partition_id").asLong(),
                        strategy(target.get("strategy")).withContext(fence));
            }
            ByteBuf body = FetchMessages.payload(
                    PollRouter.takeBytes(BytesSerializer.toBytes(consumer(request.get("consumer")))),
                    topic(request),
                    targets,
                    request.get("count").asLong(),
                    request.get("auto_commit").asBoolean(),
                    options(request));
            int first = body.writerIndex() - targets.size() * FetchMessages.TARGET_BYTES;
            int index = 0;
            for (JsonNode target : request.get("targets")) {
                body.setIntLE(
                        first + index++ * FetchMessages.TARGET_BYTES + FetchMessages.TARGET_MAX_BYTES_OFFSET,
                        target.get("max_bytes").asInt());
            }

            assertThat(hex(body))
                    .as(vector.get("case").asString())
                    .isEqualTo(vector.get("body_hex").asString());
        }
    }

    @Test
    void shouldDecodeEachFetchReplyOfTheFixture() {
        for (JsonNode vector : ROOT.get("fetch_replies")) {
            List<FetchedMessages> sections = BytesDeserializer.readFetchedMessages(
                    Unpooled.wrappedBuffer(HEX.parseHex(vector.get("body_hex").asString())));
            JsonNode expected = vector.get("sections");

            assertThat(sections).as(vector.get("case").asString()).hasSize(expected.size());
            for (int index = 0; index < sections.size(); index++) {
                FetchedMessages section = sections.get(index);
                JsonNode fields = expected.get(index);
                JsonNode context = fields.get("context");
                assertThat(List.of(
                                section.partitionId(),
                                section.status(),
                                section.currentOffset().longValue(),
                                section.count(),
                                section.nextOffset().longValue(),
                                (long) section.records().length))
                        .containsExactly(
                                fields.get("partition_id").asLong(),
                                fields.get("status").asLong(),
                                fields.get("current_offset").asLong(),
                                fields.get("messages_count").asLong(),
                                fields.get("next_offset").asLong(),
                                fields.get("records_len").asLong());
                assertThat(section.context())
                        .isEqualTo(new PartitionContext(
                                BigInteger.valueOf(context.get("incarnation").asLong()),
                                BigInteger.valueOf(
                                        context.get("owner_generation").asLong()),
                                BigInteger.valueOf(context.get("metadata_op").asLong())));
                if (section.status() == FetchedMessages.STATUS_OK) {
                    assertMessages(section.decode(), fields.get("messages"));
                }
            }
        }
    }

    @Test
    void shouldRefuseEachRefusedFetchReplyOfTheFixture() {
        for (JsonNode vector : ROOT.get("refused").get("fetch_replies")) {
            byte[] body = HEX.parseHex(vector.get("body_hex").asString());

            assertThatThrownBy(() -> BytesDeserializer.readFetchedMessages(Unpooled.wrappedBuffer(body))
                            .forEach(FetchedMessages::decode))
                    .as(vector.get("case").asString())
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static void assertMessages(PolledMessages polled, JsonNode expected) {
        assertThat(polled.messages()).hasSize(expected.size());
        for (int index = 0; index < expected.size(); index++) {
            Message message = polled.messages().get(index);
            JsonNode fields = expected.get(index);
            assertThat(List.of(
                            message.header().offset().toString(),
                            message.header().id().toBigInteger().toString(),
                            message.header().timestamp().toString(),
                            message.header().originTimestamp().toString(),
                            new String(message.payload(), StandardCharsets.UTF_8)))
                    .containsExactly(
                            fields.get("offset").asString(),
                            fields.get("id").asString(),
                            fields.get("timestamp").asString(),
                            fields.get("origin_timestamp").asString(),
                            fields.get("payload").asString());
        }
    }

    private static StreamId streamId(JsonNode identifier) {
        return numeric(identifier)
                ? StreamId.of(identifier.get("value").asLong())
                : StreamId.of(identifier.get("value").asString());
    }

    private static TopicId topicId(JsonNode identifier) {
        return numeric(identifier)
                ? TopicId.of(identifier.get("value").asLong())
                : TopicId.of(identifier.get("value").asString());
    }

    private static boolean numeric(JsonNode identifier) {
        return "numeric".equals(identifier.get("kind").asString());
    }

    private static byte[] topic(JsonNode request) {
        ByteBuf topic = BytesSerializer.toBytes(streamId(request.get("stream_id")));
        ByteBuf topicPart = BytesSerializer.toBytes(topicId(request.get("topic_id")));
        try {
            topic.writeBytes(topicPart);
        } finally {
            topicPart.release();
        }
        return PollRouter.takeBytes(topic);
    }

    private static Consumer consumer(JsonNode consumer) {
        JsonNode id = consumer.get("id");
        ConsumerId consumerId = "numeric".equals(id.get("kind").asString())
                ? ConsumerId.of(id.get("value").asLong())
                : ConsumerId.of(id.get("value").asString());
        return consumer.get("kind").asInt() == 2 ? Consumer.group(consumerId) : Consumer.of(consumerId);
    }

    private static PollingStrategy strategy(JsonNode strategy) {
        PollingKind kind = PollingKind.values()[strategy.get("kind").asInt() - 1];
        return new PollingStrategy(
                kind, BigInteger.valueOf(strategy.get("value").asLong()));
    }

    private static DeferredPollOptions options(JsonNode request) {
        return new DeferredPollOptions(
                Duration.of(request.get("wait_us").asLong(), ChronoUnit.MICROS),
                request.get("min_count").asLong(),
                request.get("max_bytes").asLong(),
                Duration.of(request.get("request_timeout_us").asLong(), ChronoUnit.MICROS));
    }

    private static String hex(ByteBuf buffer) {
        try {
            byte[] bytes = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), bytes);
            return HEX.formatHex(bytes);
        } finally {
            buffer.release();
        }
    }

    private static JsonNode load() {
        try (InputStream input = DeferredPollingFixtureTest.class.getResourceAsStream("/deferred_polling.json")) {
            return JsonMapper.builder().build().readTree(input);
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }
}
