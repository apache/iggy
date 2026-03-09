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
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IggyFrameEncoder {
    private static final Logger log = LoggerFactory.getLogger(IggyFrameEncoder.class);

    private IggyFrameEncoder() {}

    public static ByteBuf encode(ByteBufAllocator alloc, int commandCode, ByteBuf payload) {

        // Build the request frame exactly like the blocking client
        // Frame format: [payload_size:4][command:4][payload:N]
        // where payload_size = 4 (command size) + N (payload size)
        int payloadSize = payload.readableBytes();
        int framePayloadSize = 4 + payloadSize; // command (4 bytes) + payload
        ByteBuf frame = alloc.buffer(4 + framePayloadSize);
        frame.writeIntLE(framePayloadSize); // Length field (includes command)
        frame.writeIntLE(commandCode); // Command
        frame.writeBytes(payload, payload.readerIndex(), payloadSize); // Payload

        // Debug: print frame bytes
        byte[] frameBytes = new byte[Math.min(frame.readableBytes(), 30)];
        if (log.isTraceEnabled()) {
            frame.getBytes(0, frameBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : frameBytes) {
                hex.append(String.format("%02x ", b));
            }
            log.trace(
                    "Sending frame with command: {}, payload size: {}, frame payload size (with command): {}, total frame size: {}",
                    commandCode,
                    payloadSize,
                    framePayloadSize,
                    frame.readableBytes());
            log.trace("Frame bytes (hex): {}", hex.toString());
        }

        return frame;
    }
}
