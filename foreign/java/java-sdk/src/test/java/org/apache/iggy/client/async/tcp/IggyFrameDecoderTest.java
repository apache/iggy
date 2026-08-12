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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyFrameDecoderTest {

    private EmbeddedChannel channel;

    @AfterEach
    void tearDown() {
        if (channel != null) {
            // Release any remaining messages
            Object msg;
            while ((msg = channel.readInbound()) != null) {
                if (msg instanceof ByteBuf) {
                    ((ByteBuf) msg).release();
                }
            }
            channel.finishAndReleaseAll();
        }
    }

    @Nested
    class CompleteFrames {

        @Test
        void shouldDecodeCompleteFrameWithSmallPayload() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0); // status = success
            input.writeIntLE(5); // length = 5 bytes
            input.writeBytes("hello".getBytes()); // payload

            // when
            channel.writeInbound(input);

            // then
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            assertThat(decoded.readableBytes()).isEqualTo(8 + 5); // header + payload
            assertThat(decoded.readIntLE()).isEqualTo(0); // status
            assertThat(decoded.readIntLE()).isEqualTo(5); // length
            byte[] payload = new byte[5];
            decoded.readBytes(payload);
            assertThat(new String(payload)).isEqualTo("hello");
            decoded.release();
        }

        @Test
        void shouldDecodeCompleteFrameWithZeroLengthPayload() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0); // status
            input.writeIntLE(0); // length = 0

            // when
            channel.writeInbound(input);

            // then
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            assertThat(decoded.readableBytes()).isEqualTo(8); // header only
            assertThat(decoded.readIntLE()).isEqualTo(0);
            assertThat(decoded.readIntLE()).isEqualTo(0);
            decoded.release();
        }

        @Test
        void shouldDecodeCompleteFrameWithLargePayload() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            byte[] largePayload = new byte[10000];
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) (i % 256);
            }

            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(200); // error status
            input.writeIntLE(10000); // large length
            input.writeBytes(largePayload);

            // when
            channel.writeInbound(input);

            // then
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            assertThat(decoded.readableBytes()).isEqualTo(8 + 10000);
            assertThat(decoded.readIntLE()).isEqualTo(200);
            assertThat(decoded.readIntLE()).isEqualTo(10000);
            decoded.release();
        }

        @Test
        void shouldDecodeFrameWithVariousStatusCodes() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());

            for (int status = 0; status <= 5; status++) {
                ByteBuf input = Unpooled.buffer();
                input.writeIntLE(status);
                input.writeIntLE(1);
                input.writeByte(42);

                // when
                channel.writeInbound(input);

                // then
                ByteBuf decoded = channel.readInbound();
                assertThat(decoded).isNotNull();
                assertThat(decoded.readIntLE()).isEqualTo(status);
                decoded.skipBytes(4); // length
                decoded.skipBytes(1); // payload
                decoded.release();
            }
        }
    }

    @Nested
    class IncompleteFrames {

        @Test
        void shouldWaitForCompleteHeaderWhenOnlyPartialHeaderAvailable() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0); // only status, missing length (4 bytes total, need 8)

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse(); // No complete frame yet
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNull(); // Nothing to read
        }

        @Test
        void shouldWaitForCompletePayloadWhenOnlyHeaderAvailable() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0); // status
            input.writeIntLE(100); // expects 100 bytes payload
            // But no payload written

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse();
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNull();
        }

        @Test
        void shouldWaitForCompletePayloadWhenPartialPayloadAvailable() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(100); // expects 100 bytes
            input.writeBytes(new byte[50]); // only 50 bytes available

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse();
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldEventuallyDecodeWhenMoreDataArrives() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf firstChunk = Unpooled.buffer();
            firstChunk.writeIntLE(0);
            firstChunk.writeIntLE(10);
            firstChunk.writeBytes(new byte[5]); // only 5 of 10 bytes

            // when - first chunk
            boolean hasMessage1 = channel.writeInbound(firstChunk);
            assertThat(hasMessage1).isFalse();

            // when - second chunk completes the frame
            ByteBuf secondChunk = Unpooled.buffer();
            secondChunk.writeBytes(new byte[5]); // remaining 5 bytes
            boolean hasMessage2 = channel.writeInbound(secondChunk);

            // then
            assertThat(hasMessage2).isTrue();
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            assertThat(decoded.readableBytes()).isEqualTo(8 + 10);
            decoded.release();
        }
    }

    @Nested
    class MultipleFrames {

        @Test
        void shouldDecodeMultipleFramesInSequence() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();

            // Frame 1
            input.writeIntLE(0);
            input.writeIntLE(3);
            input.writeBytes("abc".getBytes());

            // Frame 2
            input.writeIntLE(1);
            input.writeIntLE(2);
            input.writeBytes("de".getBytes());

            // when
            channel.writeInbound(input);

            // then - frame 1
            ByteBuf decoded1 = channel.readInbound();
            assertThat(decoded1).isNotNull();
            assertThat(decoded1.readableBytes()).isEqualTo(8 + 3);
            decoded1.release();

            // then - frame 2
            ByteBuf decoded2 = channel.readInbound();
            assertThat(decoded2).isNotNull();
            assertThat(decoded2.readableBytes()).isEqualTo(8 + 2);
            decoded2.release();

            // No more frames
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldDecodeThreeFramesCorrectly() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();

            for (int i = 0; i < 3; i++) {
                input.writeIntLE(i);
                input.writeIntLE(1);
                input.writeByte(i);
            }

            // when
            channel.writeInbound(input);

            // then
            for (int i = 0; i < 3; i++) {
                ByteBuf decoded = channel.readInbound();
                assertThat(decoded).isNotNull();
                assertThat(decoded.readIntLE()).isEqualTo(i);
                decoded.skipBytes(4 + 1); // skip length and payload
                decoded.release();
            }

            assertThat((ByteBuf) channel.readInbound()).isNull();
        }
    }

    @Nested
    class EdgeCases {

        @Test
        void shouldNotAdvanceReaderIndexWhenPayloadIncomplete() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(100); // expects 100 bytes
            input.writeBytes(new byte[50]); // only 50 bytes
            int readerIndexBefore = input.readerIndex();

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then - decoder should wait for complete payload
            assertThat(hasMessage).isFalse();
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNull();
            assertThat(input.readerIndex()).isEqualTo(readerIndexBefore);
        }

        @Test
        void shouldHandleEmptyInput() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer(); // empty buffer

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse();
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldHandleSingleByteInput() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeByte(0); // only 1 byte

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse();
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldHandleExactlyHeaderSizeWithoutPayload() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(10); // expects payload, but none provided
            // Exactly 8 bytes (header size)

            // when
            boolean hasMessage = channel.writeInbound(input);

            // then
            assertThat(hasMessage).isFalse(); // Waiting for payload
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldDecodeFrameFollowedByPartialNextFrame() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();

            // Complete frame 1
            input.writeIntLE(0);
            input.writeIntLE(5);
            input.writeBytes("hello".getBytes());

            // Partial frame 2 (only header)
            input.writeIntLE(1);
            input.writeIntLE(10);
            // No payload for frame 2

            // when
            channel.writeInbound(input);

            // then - should get frame 1
            ByteBuf decoded1 = channel.readInbound();
            assertThat(decoded1).isNotNull();
            assertThat(decoded1.readableBytes()).isEqualTo(8 + 5);
            decoded1.release();

            // Frame 2 should not be available yet
            assertThat((ByteBuf) channel.readInbound()).isNull();
        }

        @Test
        void shouldHandleMaxIntPayloadLength() {
            // given - test with a reasonably large payload (not actual MAX_INT to avoid OOM)
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            int largeSize = 1000000; // 1MB
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(largeSize);
            input.writeBytes(new byte[largeSize]);

            // when
            channel.writeInbound(input);

            // then
            ByteBuf decoded = channel.readInbound();
            assertThat(decoded).isNotNull();
            assertThat(decoded.readableBytes()).isEqualTo(8 + largeSize);
            decoded.release();
        }
    }

    @Nested
    class BufferManagement {

        @Test
        void shouldCreateNewBufferForEachDecodedFrame() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(5);
            input.writeBytes("hello".getBytes());

            // when
            channel.writeInbound(input);
            ByteBuf decoded = channel.readInbound();

            // then - decoded buffer should be independent
            assertThat(decoded).isNotNull();
            assertThat(decoded).isNotSameAs(input);
            assertThat(decoded.readableBytes()).isEqualTo(13);
            decoded.release();
        }

        @Test
        void shouldAllowManualReleaseOfDecodedBuffers() {
            // given
            channel = new EmbeddedChannel(new IggyFrameDecoder());
            ByteBuf input = Unpooled.buffer();
            input.writeIntLE(0);
            input.writeIntLE(3);
            input.writeBytes("foo".getBytes());

            // when
            channel.writeInbound(input);
            ByteBuf decoded = channel.readInbound();

            // then - should be releasable
            assertThat(decoded.refCnt()).isEqualTo(1);
            decoded.release();
            assertThat(decoded.refCnt()).isEqualTo(0);
        }
    }
}
