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
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Correlates in-flight requests with responses in FIFO order, decodes VSR
 * reply frames, and completes the pending request future with the command
 * payload the typed deserializers expect. Mirrors {@code decode_response} in
 * {@code core/sdk/src/vsr.rs}: eviction frames become typed errors, a nonzero
 * header status is a pre-commit deny, and result-framed bodies have their
 * committed result section stripped (or raised as the typed error).
 */
public class VsrResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(VsrResponseHandler.class);

    private static final int RESULT_COUNT_LEN = 4;
    private static final int RESULT_ENTRY_LEN = 8;
    private static final int REGISTER_BODY_MIN_LEN = 17;

    private final Queue<CompletableFuture<ByteBuf>> responseQueue = new ConcurrentLinkedQueue<>();
    private final ConsensusSession session;
    private final Runnable onEviction;

    public VsrResponseHandler(ConsensusSession session, Runnable onEviction) {
        this.session = session;
        this.onEviction = onEviction;
    }

    public void enqueueRequest(CompletableFuture<ByteBuf> future) {
        responseQueue.add(future);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        failPendingRequests(new IggyConnectionException("Connection closed before a response arrived"));
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        failPendingRequests(cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        CompletableFuture<ByteBuf> future = responseQueue.poll();
        if (future == null) {
            log.error(
                    "Received response on channel {} but no request was waiting!",
                    ctx.channel().id());
            return;
        }
        ByteBuf body;
        try {
            body = decodeReply(msg);
        } catch (RuntimeException error) {
            future.completeExceptionally(error);
            return;
        }
        future.complete(body);
    }

    private ByteBuf decodeReply(ByteBuf frame) {
        int operation = validatedOperation(frame);

        int totalSize = (int) VsrHeaders.readSize(frame);
        int bodyStart = frame.readerIndex() + VsrHeaders.HEADER_SIZE;
        int bodyLength = totalSize - VsrHeaders.HEADER_SIZE;

        boolean resultFramed =
                VsrOperation.isResultFramed(operation) || (operation == VsrOperation.REGISTER && bodyLength > 0);
        if (resultFramed) {
            int sectionLength = resultSectionLength(frame, bodyStart, bodyLength);
            bodyStart += sectionLength;
            bodyLength -= sectionLength;
        }

        applySessionEffects(frame, operation, bodyStart, bodyLength);
        return frame.retainedSlice(bodyStart, bodyLength);
    }

    private int validatedOperation(ByteBuf frame) {
        int command = VsrHeaders.peekCommand(frame);
        if (command == VsrHeaders.COMMAND_EVICTION) {
            // The session is terminal server-side; the next login re-registers.
            session.reset();
            onEviction.run();
            throw VsrHeaders.evictionToException(frame);
        }
        if (command != VsrHeaders.COMMAND_REPLY) {
            throw invalidReply("unexpected consensus command " + command);
        }
        long status = VsrHeaders.readStatus(frame);
        if (status != 0) {
            throw IggyServerException.fromTcpResponse(status, new byte[0]);
        }
        int operation = VsrHeaders.readReplyOperation(frame);
        if (!VsrOperation.isKnown(operation)) {
            throw invalidReply("unknown reply operation " + operation);
        }
        return operation;
    }

    /**
     * Validates the committed result section leading the body and returns its
     * length; a nonzero committed result surfaces as the typed error.
     */
    private static int resultSectionLength(ByteBuf frame, int bodyStart, int bodyLength) {
        if (bodyLength < RESULT_COUNT_LEN) {
            throw invalidReply("result-framed body shorter than its count field");
        }
        long count = frame.getUnsignedIntLE(bodyStart);
        long sectionLength = RESULT_COUNT_LEN + count * RESULT_ENTRY_LEN;
        if (bodyLength < sectionLength) {
            throw invalidReply("result section truncated");
        }
        if (count > 0) {
            long resultCode = frame.getUnsignedIntLE(bodyStart + RESULT_COUNT_LEN + 4);
            if (resultCode != 0) {
                throw IggyServerException.fromTcpResponse(resultCode, new byte[0]);
            }
        }
        return (int) sectionLength;
    }

    private void applySessionEffects(ByteBuf frame, int operation, int bodyStart, int bodyLength) {
        if (operation == VsrOperation.REGISTER) {
            // A terminal register failure ships an empty body (no result
            // section); anything shorter than the typed response is not a
            // successful registration.
            if (bodyLength < REGISTER_BODY_MIN_LEN) {
                throw IggyServerException.fromTcpResponse(VsrHeaders.ERROR_UNAUTHENTICATED, new byte[0]);
            }
            session.bind(frame.getLongLE(bodyStart + 4));
        }
        if (operation == VsrOperation.LOGOUT) {
            session.reset();
        }
    }

    private static IggyServerException invalidReply(String detail) {
        log.error("Malformed VSR reply: {}", detail);
        return IggyServerException.fromTcpResponse(VsrHeaders.ERROR_INVALID_COMMAND, new byte[0]);
    }

    private void failPendingRequests(Throwable cause) {
        CompletableFuture<ByteBuf> pending;
        while ((pending = responseQueue.poll()) != null) {
            pending.completeExceptionally(cause);
        }
    }
}
