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
import io.netty.buffer.ByteBufAllocator;

/**
 * Encodes a (code, payload) command into a VSR request frame:
 * a 256-byte {@code RequestHeader} followed by the unchanged command payload.
 * Login codes are rewritten to the {@code LoginRegister} exchange; every
 * other payload is passed through byte-identical.
 *
 * <p>Mirrors {@code encode_request_header} in {@code core/sdk/src/vsr.rs}.
 */
public final class VsrRequestEncoder {

    private static final int LOGIN_USER_CODE = 38;
    private static final int LOGIN_REGISTER_CODE = 40;
    private static final int LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE = 44;
    private static final int LOGIN_REGISTER_WITH_PAT_CODE = 45;

    private final ConsensusSession session;

    public VsrRequestEncoder(ConsensusSession session) {
        this.session = session;
    }

    /**
     * Builds the full request frame. The caller keeps ownership of
     * {@code payload}; its reader index is not advanced.
     */
    public ByteBuf encode(ByteBufAllocator alloc, int commandCode, ByteBuf payload) {
        int operation;
        long requestId;
        long sessionId;
        ByteBuf body;
        boolean releaseBody = false;

        if (commandCode == LOGIN_USER_CODE || commandCode == LOGIN_REGISTER_CODE) {
            body = VsrLoginCodec.rewriteUserLogin(alloc, payload);
            releaseBody = true;
            operation = VsrOperation.REGISTER;
            requestId = session.beginRegister();
            sessionId = 0;
        } else if (commandCode == LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
                || commandCode == LOGIN_REGISTER_WITH_PAT_CODE) {
            body = VsrLoginCodec.rewritePatLogin(alloc, payload);
            releaseBody = true;
            operation = VsrOperation.REGISTER;
            requestId = session.beginRegister();
            sessionId = 0;
        } else {
            body = payload;
            operation = VsrOperation.operationForCode(commandCode);
            if (operation == VsrOperation.NON_REPLICATED) {
                // Non-replicated ops bypass dedup: read the counter without
                // advancing, and stay sessionless before login (bootstrap
                // ping and cluster metadata work unauthenticated).
                requestId = session.currentRequestId();
                sessionId = session.sessionOrZero();
            } else if (VsrOperation.isPartition(operation)) {
                // Partition ops replicate in their own group with no client
                // table dedup; consuming the counter would gap the next
                // metadata request id.
                sessionId = session.boundSession();
                requestId = session.currentRequestId();
            } else {
                sessionId = session.boundSession();
                requestId = session.nextRequestId();
            }
        }

        try {
            long namespace = VsrNamespace.namespaceFor(operation, commandCode, body);
            int totalSize = VsrHeaders.HEADER_SIZE + body.readableBytes();
            ByteBuf frame = alloc.buffer(totalSize);
            frame.writeZero(VsrHeaders.HEADER_SIZE);
            frame.setIntLE(VsrHeaders.SIZE_OFFSET, totalSize);
            frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_REQUEST);
            frame.setLongLE(VsrHeaders.REQUEST_CLIENT_OFFSET, session.clientIdLow());
            frame.setLongLE(VsrHeaders.REQUEST_CLIENT_OFFSET + 8, session.clientIdHigh());
            frame.setLongLE(VsrHeaders.REQUEST_ID_OFFSET, requestId);
            frame.setByte(VsrHeaders.REQUEST_OPERATION_OFFSET, operation);
            frame.setLongLE(VsrHeaders.REQUEST_NAMESPACE_OFFSET, namespace);
            frame.setLongLE(VsrHeaders.REQUEST_SESSION_OFFSET, sessionId);
            if (operation == VsrOperation.NON_REPLICATED) {
                frame.setIntLE(VsrHeaders.REQUEST_RESERVED_CODE_OFFSET, commandCode);
            }
            frame.writeBytes(body, body.readerIndex(), body.readableBytes());
            return frame;
        } finally {
            if (releaseBody) {
                body.release();
            }
        }
    }
}
