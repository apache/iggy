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
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession.LaneKey;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.BytesDeserializer;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The fields that the client reads from and writes into the body of a fetch, on the golden request of the contract. */
class FetchMessagesTest {
    // Made by the Rust encoder: consumer group 7, stream 1, topic "orders", wait 5 s, timeout 6 s, min_count 1,
    // count 100, max_bytes 1 MiB and auto commit, then partition 1 at Offset(41) and partition 2 at Next, each with
    // max_bytes 512 KiB, incarnation 3 and owner generation 9.
    static final String GOLDEN_REQUEST =
            "0201040700000001040100000002066f7264657273404b4c0000000000808d5b00000000000100000064000000000010"
                    + "000102000000010000000129000000000000000000080003000000000000000900000000000000020000000500000000"
                    + "000000000000080003000000000000000900000000000000";
    // The consumer, stream and topic of the golden request, which the limits follow.
    static final int GOLDEN_LIMITS = 21;
    static final long GOLDEN_MAX_BYTES = 1024 * 1024;
    private static final long GOLDEN_INCARNATION = 3;
    private static final int GOLDEN_TARGETS = 2;
    // More than one record of the largest size and a whole fetch table.
    private static final long LARGE_MAX_BYTES = 0xFFFF_0000L;

    @Test
    void shouldAcceptOneMicrosecondOfStartupBudgetForAZeroWaitFetch() {
        var options = DeferredPollOptions.defaults()
                .withMaxWait(Duration.ZERO)
                .withRequestTimeout(DeferredPollOptions.REPLY_MARGIN.plusNanos(1000));

        assertThatCode(() -> FetchMessages.validate(Map.of(1L, PollingStrategy.offset(BigInteger.ZERO)), 1, options))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldTakeTheProgressLaneOfEveryTargetOfACommittingFetch() {
        ByteBuf payload = golden();
        setKind(payload, 1, PollingKind.Offset);

        assertThat(FetchMessages.progressLanes(payload))
                .containsExactly(new LaneKey(GOLDEN_INCARNATION, 1, true), new LaneKey(GOLDEN_INCARNATION, 2, true));
    }

    @Test
    void shouldTakeTheProgressLanesOfAFetchThatStartsAPartitionAtItsNextOffset() {
        ByteBuf payload = golden();
        payload.setByte(GOLDEN_LIMITS + FetchMessages.AUTO_COMMIT_OFFSET, 0);

        assertThat(FetchMessages.progressLanes(payload))
                .containsExactly(new LaneKey(GOLDEN_INCARNATION, 1, true), new LaneKey(GOLDEN_INCARNATION, 2, true));
    }

    @Test
    void shouldTakeNoLaneForAFetchThatOnlyReads() {
        ByteBuf payload = golden();
        payload.setByte(GOLDEN_LIMITS + FetchMessages.AUTO_COMMIT_OFFSET, 0);
        setKind(payload, 1, PollingKind.Offset);

        assertThat(FetchMessages.progressLanes(payload)).isEmpty();
    }

    /** The server returns a first message larger than the byte limit alone, so the reply can hold one such record. */
    @Test
    void shouldBoundTheReplyByTheSectionTableAndTheLargestRecord() {
        assertThat(FetchMessages.replyLimit(frame())).isEqualTo(tableBytes() + DeferredPollOptions.MAX_RECORD_SIZE);
    }

    @Test
    void shouldKeepTheByteLimitsOfAFetchWhoseReplyFitsTheLink() {
        ByteBuf frame = frame();

        FetchMessages.fitReply(frame, VsrHeaders.HEADER_SIZE + GOLDEN_LIMITS, FetchMessages.replyLimit(frame));

        assertThat(HexFormat.of().formatHex(body(frame))).isEqualTo(GOLDEN_REQUEST);
    }

    @Test
    void shouldLowerTheByteLimitsUntilTheWholeReplyFitsTheLink() {
        ByteBuf frame = frame();
        setMaxBytes(frame, LARGE_MAX_BYTES);

        FetchMessages.fitReply(
                frame, VsrHeaders.HEADER_SIZE + GOLDEN_LIMITS, tableBytes() + DeferredPollOptions.MAX_RECORD_SIZE);

        ByteBuf body = Unpooled.wrappedBuffer(body(frame));
        assertThat(body.getUnsignedIntLE(GOLDEN_LIMITS + FetchMessages.MAX_BYTES_OFFSET))
                .isEqualTo(DeferredPollOptions.MAX_RECORD_SIZE);
        for (int target = 0; target < GOLDEN_TARGETS; target++) {
            assertThat(body.getUnsignedIntLE(target(target) + FetchMessages.TARGET_MAX_BYTES_OFFSET))
                    .isEqualTo(DeferredPollOptions.MAX_RECORD_SIZE);
        }
        assertThat(FetchMessages.replyLimit(frame)).isEqualTo(tableBytes() + DeferredPollOptions.MAX_RECORD_SIZE);
    }

    @Test
    void shouldRefuseAFetchWhoseLinkCannotHoldTheLargestRecord() {
        ByteBuf frame = frame();

        assertThatThrownBy(() -> FetchMessages.fitReply(
                        frame,
                        VsrHeaders.HEADER_SIZE + GOLDEN_LIMITS,
                        tableBytes() + DeferredPollOptions.MAX_RECORD_SIZE - 1))
                .isInstanceOf(IggyInvalidArgumentException.class);
        FetchMessages.fitReply(
                frame, VsrHeaders.HEADER_SIZE + GOLDEN_LIMITS, tableBytes() + DeferredPollOptions.MAX_RECORD_SIZE);
        assertThat(HexFormat.of().formatHex(body(frame))).isEqualTo(GOLDEN_REQUEST);
    }

    /** Sets the byte limit of the request and of each target of the golden fetch in {@code frame}. */
    private static void setMaxBytes(ByteBuf frame, long maxBytes) {
        frame.setIntLE(VsrHeaders.HEADER_SIZE + GOLDEN_LIMITS + FetchMessages.MAX_BYTES_OFFSET, (int) maxBytes);
        for (int target = 0; target < GOLDEN_TARGETS; target++) {
            frame.setIntLE(
                    VsrHeaders.HEADER_SIZE + target(target) + FetchMessages.TARGET_MAX_BYTES_OFFSET, (int) maxBytes);
        }
    }

    private static ByteBuf golden() {
        return Unpooled.wrappedBuffer(HexFormat.of().parseHex(GOLDEN_REQUEST));
    }

    private static ByteBuf frame() {
        return Unpooled.wrappedBuffer(Unpooled.buffer().writeZero(VsrHeaders.HEADER_SIZE), golden());
    }

    private static byte[] body(ByteBuf frame) {
        byte[] body = new byte[frame.readableBytes() - VsrHeaders.HEADER_SIZE];
        frame.getBytes(VsrHeaders.HEADER_SIZE, body);
        return body;
    }

    private static int target(int index) {
        return GOLDEN_LIMITS + FetchMessages.LIMITS_BYTES + index * FetchMessages.TARGET_BYTES;
    }

    private static void setKind(ByteBuf payload, int index, PollingKind kind) {
        payload.setByte(target(index) + FetchMessages.STRATEGY_KIND_OFFSET, kind.asCode());
    }

    private static long tableBytes() {
        return BytesDeserializer.FETCH_SECTIONS_COUNT_BYTES + GOLDEN_TARGETS * BytesDeserializer.FETCH_SECTION_BYTES;
    }
}
