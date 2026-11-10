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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.util.List;
import java.util.function.LongUnaryOperator;

/**
 * Decoder for VSR response frames: a 256-byte consensus header whose total
 * frame size (header included) sits at byte offset 48, followed by an
 * optional body. There is no length delimiter outside the header.
 */
public class VsrFrameDecoder extends ByteToMessageDecoder {

    /** Matches the server's default {@code max_message_size} (64 MB). */
    public static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;

    private final int maxFrameSize;
    private final LongUnaryOperator requestLimit;

    public VsrFrameDecoder() {
        this(DEFAULT_MAX_FRAME_SIZE);
    }

    public VsrFrameDecoder(int maxFrameSize) {
        this(maxFrameSize, ignored -> maxFrameSize);
    }

    /**
     * A deferred poll declares how many response bytes it will accept, and that
     * bound has to reach the decoder before it accumulates a body, not after.
     * The limit is looked up per frame by its request id, so one exchange can
     * tighten it while the connection's other replies keep the ceiling.
     *
     * @param maxFrameSize the connection's frame ceiling, never below the header size
     * @param requestLimit the frame limit of the request a reply answers
     */
    public VsrFrameDecoder(int maxFrameSize, LongUnaryOperator requestLimit) {
        if (maxFrameSize < VsrHeaders.HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "Maximum VSR frame size must be at least " + VsrHeaders.HEADER_SIZE + " bytes");
        }
        this.maxFrameSize = maxFrameSize;
        this.requestLimit = requestLimit;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < VsrHeaders.HEADER_SIZE) {
            return;
        }
        long totalSize = VsrHeaders.readSize(in);
        if (totalSize < VsrHeaders.HEADER_SIZE
                || totalSize > Math.min(maxFrameSize, requestLimit.applyAsLong(VsrHeaders.readReplyRequestId(in)))) {
            throw new DecoderException("Invalid VSR frame size " + totalSize + ", connection is desynchronized");
        }
        if (in.readableBytes() < totalSize) {
            return;
        }
        out.add(in.readRetainedSlice((int) totalSize));
    }
}
