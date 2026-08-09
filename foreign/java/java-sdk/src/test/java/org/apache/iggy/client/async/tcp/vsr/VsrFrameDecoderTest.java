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

package org.apache.iggy.client.async.tcp.vsr;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrFrameDecoderTest {

    private final EmbeddedChannel channel = new EmbeddedChannel(new VsrFrameDecoder());

    @AfterEach
    void tearDown() {
        try {
            channel.finishAndReleaseAll();
        } catch (DecoderException ignored) {
            // The desync test leaves the failed decoder in the pipeline and
            // closing the channel replays its exception.
        }
    }

    @Test
    void shouldWaitForMoreBytesOnPartialHeader() {
        ByteBuf partial = Unpooled.buffer();
        partial.writeZero(100);
        channel.writeInbound(partial);

        assertThat((Object) channel.readInbound()).isNull();
    }

    @Test
    void shouldEmitOneFrameFromSplitReads() {
        ByteBuf frame = Unpooled.buffer();
        frame.writeZero(VsrHeaders.HEADER_SIZE);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE + 8);
        frame.writeLongLE(123456789L);

        channel.writeInbound(frame.retainedSlice(0, 200));
        assertThat((Object) channel.readInbound()).isNull();
        channel.writeInbound(frame.retainedSlice(200, frame.readableBytes() - 200));
        frame.release();

        ByteBuf decoded = channel.readInbound();
        try {
            assertThat(decoded.readableBytes()).isEqualTo(VsrHeaders.HEADER_SIZE + 8);
            assertThat(decoded.getLongLE(VsrHeaders.HEADER_SIZE)).isEqualTo(123456789L);
        } finally {
            decoded.release();
        }
    }

    @Test
    void shouldFailConnectionOnInvalidSizeField() {
        ByteBuf frame = Unpooled.buffer();
        frame.writeZero(VsrHeaders.HEADER_SIZE);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, 8);

        assertThatThrownBy(() -> channel.writeInbound(frame)).isInstanceOf(DecoderException.class);
    }

    @Test
    void shouldWaitForLargeFrameWhenConfiguredLimitAllowsIt() {
        int declaredSize = VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE + 1;
        EmbeddedChannel largeFrameChannel = new EmbeddedChannel(new VsrFrameDecoder(declaredSize));
        ByteBuf header = Unpooled.buffer(VsrHeaders.HEADER_SIZE);
        header.writeZero(VsrHeaders.HEADER_SIZE);
        header.setIntLE(VsrHeaders.SIZE_OFFSET, declaredSize);
        try {
            largeFrameChannel.writeInbound(header);

            assertThat((Object) largeFrameChannel.readInbound()).isNull();
        } finally {
            largeFrameChannel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldRejectFrameAboveConfiguredLimit() {
        int configuredLimit = VsrHeaders.HEADER_SIZE + 1;
        EmbeddedChannel smallFrameChannel = new EmbeddedChannel(new VsrFrameDecoder(configuredLimit));
        ByteBuf header = Unpooled.buffer(VsrHeaders.HEADER_SIZE);
        header.writeZero(VsrHeaders.HEADER_SIZE);
        header.setIntLE(VsrHeaders.SIZE_OFFSET, configuredLimit + 1);
        try {
            assertThatThrownBy(() -> smallFrameChannel.writeInbound(header)).isInstanceOf(DecoderException.class);
        } finally {
            try {
                smallFrameChannel.finishAndReleaseAll();
            } catch (DecoderException ignored) {
                // Closing a failed decoder can replay its exception.
            }
        }
    }
}
