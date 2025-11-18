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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Decoder for Iggy protocol responses.
 * Response format: [4-byte status LE] [4-byte length LE] [payload]
 */
public class IggyFrameDecoder extends ByteToMessageDecoder {
    private static final Logger log = LoggerFactory.getLogger(IggyFrameDecoder.class);
    private static final int HEADER_SIZE = 8; // status (4) + length (4)

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Wait until we have at least the header
        if (in.readableBytes() < HEADER_SIZE) {
            return;
        }

        // Mark the current reader index
        in.markReaderIndex();

        // Read status and length
        int status = in.readIntLE();
        int length = in.readIntLE();

        log.trace("Received response with status={}, length={}", status, length);

        // Check if we have the complete payload
        if (in.readableBytes() < length) {
            // Not enough data, reset and wait for more
            in.resetReaderIndex();
            return;
        }

        // Create a new buffer with the complete response
        ByteBuf response = ctx.alloc().buffer(HEADER_SIZE + length);
        response.writeIntLE(status);
        response.writeIntLE(length);

        if (length > 0) {
            response.writeBytes(in, length);
        }

        log.trace("Decoded complete response, forwarding to handler");
        out.add(response);
    }
}
