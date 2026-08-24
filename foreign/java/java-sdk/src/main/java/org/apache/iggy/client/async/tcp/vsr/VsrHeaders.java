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
import org.apache.iggy.exception.IggyServerException;

/**
 * Byte offsets and readers for the 256-byte consensus headers, mirroring the
 * {@code #[repr(C)]} layouts in
 * {@code core/binary_protocol/src/consensus/header.rs}. All fields are
 * little-endian; the checksum fields stay zero by protocol contract.
 */
public final class VsrHeaders {

    public static final int HEADER_SIZE = 256;

    // GenericHeader (shared prefix)
    static final int SIZE_OFFSET = 48;
    static final int COMMAND_OFFSET = 60;

    // RequestHeader. The client wire carries no routing group: the server
    // derives it (plane from the operation, partition target from the payload),
    // so every field past the operation sits eight bytes earlier than it did
    // while the header still carried one.
    static final int REQUEST_CLIENT_OFFSET = 128;
    static final int REQUEST_ID_OFFSET = 168;
    static final int REQUEST_OPERATION_OFFSET = 176;
    static final int REQUEST_SESSION_OFFSET = 184;
    static final int REQUEST_RESERVED_CODE_OFFSET = 196;

    // ReplyHeader
    static final int REPLY_REQUEST_OFFSET = 200;
    static final int REPLY_OPERATION_OFFSET = 208;
    static final int REPLY_STATUS_OFFSET = 216;

    // EvictionHeader
    static final int EVICTION_PROTOCOL_VERSION_OFFSET = 144;
    static final int EVICTION_PROTOCOL_VERSION_MIN_OFFSET = 148;
    static final int EVICTION_REASON_OFFSET = 255;

    // Command discriminants
    static final int COMMAND_REQUEST = 5;
    static final int COMMAND_REPLY = 8;
    static final int COMMAND_EVICTION = 13;

    // EvictionReason discriminants
    static final int REASON_NO_SESSION = 1;
    static final int REASON_SESSION_TOO_LOW = 7;
    static final int REASON_SESSION_RELEASE_MISMATCH = 8;
    static final int REASON_INVALID_CREDENTIALS = 9;
    static final int REASON_INVALID_TOKEN = 10;
    static final int REASON_USER_INACTIVE = 11;
    static final int REASON_SESSION_ERROR = 12;
    static final int REASON_STALE_CLIENT = 13;
    static final int REASON_INCOMPATIBLE_PROTOCOL = 14;
    static final int REASON_MALFORMED_LOGIN = 15;

    // IggyError codes the eviction reasons grade to, mirroring
    // core/common/src/error/eviction.rs.
    static final int ERROR_INVALID_COMMAND = 3;
    static final int ERROR_INVALID_FORMAT = 4;
    static final int ERROR_STALE_CLIENT = 30;
    static final int ERROR_UNAUTHENTICATED = 40;
    static final int ERROR_INVALID_CREDENTIALS = 42;
    static final int ERROR_INVALID_PERSONAL_ACCESS_TOKEN = 53;
    static final int ERROR_TRANSIENT_NOT_COMMITTED = 57;
    static final int ERROR_TRANSIENT_NOT_ACCEPTED = 58;
    static final int ERROR_INCOMPATIBLE_PROTOCOL_VERSION = 14003;

    private VsrHeaders() {}

    static int peekCommand(ByteBuf frame) {
        return frame.getUnsignedByte(frame.readerIndex() + COMMAND_OFFSET);
    }

    static long readSize(ByteBuf frame) {
        return frame.getUnsignedIntLE(frame.readerIndex() + SIZE_OFFSET);
    }

    static long readStatus(ByteBuf frame) {
        return frame.getUnsignedIntLE(frame.readerIndex() + REPLY_STATUS_OFFSET);
    }

    static int readReplyOperation(ByteBuf frame) {
        return frame.getUnsignedByte(frame.readerIndex() + REPLY_OPERATION_OFFSET);
    }

    static long readReplyRequestId(ByteBuf frame) {
        return frame.getLongLE(frame.readerIndex() + REPLY_REQUEST_OFFSET);
    }

    static int readRequestOperation(ByteBuf frame) {
        return frame.getUnsignedByte(frame.readerIndex() + REQUEST_OPERATION_OFFSET);
    }

    static long readRequestId(ByteBuf frame) {
        return frame.getLongLE(frame.readerIndex() + REQUEST_ID_OFFSET);
    }

    /**
     * Grades a session-terminal eviction frame to the error the caller sees,
     * mirroring {@code eviction_reason_to_error}. A degenerate protocol
     * window (zero minimum or inverted range) degrades to unauthenticated.
     */
    static IggyServerException evictionToException(ByteBuf frame) {
        int base = frame.readerIndex();
        int reason = frame.getUnsignedByte(base + EVICTION_REASON_OFFSET);
        int errorCode;
        switch (reason) {
            case REASON_INVALID_CREDENTIALS -> errorCode = ERROR_INVALID_CREDENTIALS;
            case REASON_INVALID_TOKEN -> errorCode = ERROR_INVALID_PERSONAL_ACCESS_TOKEN;
            case REASON_STALE_CLIENT -> errorCode = ERROR_STALE_CLIENT;
            case REASON_MALFORMED_LOGIN -> errorCode = ERROR_INVALID_FORMAT;
            case REASON_INCOMPATIBLE_PROTOCOL -> {
                long max = frame.getUnsignedIntLE(base + EVICTION_PROTOCOL_VERSION_OFFSET);
                long min = frame.getUnsignedIntLE(base + EVICTION_PROTOCOL_VERSION_MIN_OFFSET);
                errorCode = (min == 0 || max < min) ? ERROR_UNAUTHENTICATED : ERROR_INCOMPATIBLE_PROTOCOL_VERSION;
            }
            case REASON_NO_SESSION,
                    REASON_SESSION_TOO_LOW,
                    REASON_SESSION_RELEASE_MISMATCH,
                    REASON_USER_INACTIVE,
                    REASON_SESSION_ERROR -> errorCode = ERROR_UNAUTHENTICATED;
            default -> errorCode = ERROR_INVALID_COMMAND;
        }
        return IggyServerException.fromTcpResponse(errorCode, new byte[0]);
    }
}
