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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;

/**
 * Correlates multiplexed requests by operation and request id, decodes VSR
 * reply frames, and completes each pending future with the command payload
 * the typed deserializers expect. Mirrors {@code decode_response} in {@code
 * core/sdk/src/vsr.rs}: eviction frames become typed errors, a nonzero header
 * status is a pre-commit deny, and result-framed bodies have their committed
 * result section stripped (or raised as the typed error).
 */
public class VsrResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(VsrResponseHandler.class);

    private static final int RESULT_COUNT_LEN = 4;
    private static final int RESULT_ENTRY_LEN = 8;
    private static final int REGISTER_BODY_MIN_LEN = 17;

    private final ConcurrentMap<RequestKey, CompletableFuture<ByteBuf>> pendingRequests = new ConcurrentHashMap<>();
    private final ConsensusSession session;
    private final IntConsumer onEviction;
    private final AtomicReference<Throwable> closeCause = new AtomicReference<>();

    public VsrResponseHandler(ConsensusSession session, IntConsumer onEviction) {
        this.session = session;
        this.onEviction = onEviction;
    }

    void registerRequest(CompletableFuture<ByteBuf> future, int operation, long requestId) {
        registerRequest(new RequestKey(operation, requestId), future);
    }

    public void registerRequest(
            Channel channel,
            ByteBuf requestFrame,
            CompletableFuture<ByteBuf> future,
            long deadlineNanos,
            int commandCode) {
        RequestKey key =
                new RequestKey(VsrHeaders.readRequestOperation(requestFrame), VsrHeaders.readRequestId(requestFrame));
        registerRequest(key, future);
        long timeoutNanos = Math.max(0, deadlineNanos - System.nanoTime());
        ScheduledFuture<?> timeoutFuture;
        try {
            timeoutFuture = channel.eventLoop()
                    .schedule(
                            () -> {
                                if (!future.isDone()) {
                                    closeChannel(
                                            channel,
                                            new IggyTimeoutException(
                                                    "Timed out waiting for a response to command code " + commandCode));
                                }
                            },
                            timeoutNanos,
                            TimeUnit.NANOSECONDS);
        } catch (RuntimeException error) {
            pendingRequests.remove(key, future);
            throw error;
        }
        future.whenComplete((response, error) -> {
            pendingRequests.remove(key, future);
            timeoutFuture.cancel(false);
        });
    }

    private void registerRequest(RequestKey key, CompletableFuture<ByteBuf> future) {
        CompletableFuture<ByteBuf> existing = pendingRequests.putIfAbsent(key, future);
        if (existing != null) {
            throw new IllegalStateException(
                    "A request is already pending for operation " + key.operation() + " and request id " + key.id());
        }
    }

    public void closeChannel(Channel channel, Throwable cause) {
        closeCause.compareAndSet(null, cause);
        channel.close();
        failPendingRequests(closeCause.get());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        Throwable cause = closeCause.get();
        if (cause == null) {
            cause = new IggyConnectionException("Connection closed before a response arrived");
        }
        failPendingRequests(cause);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        closeChannel(ctx.channel(), cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
        if (VsrHeaders.peekCommand(msg) == VsrHeaders.COMMAND_EVICTION) {
            handleEviction(ctx, msg);
            return;
        }
        int replyOperation = VsrHeaders.readReplyOperation(msg);
        RequestKey key =
                new RequestKey(VsrOperation.correlationOperation(replyOperation), VsrHeaders.readReplyRequestId(msg));
        CompletableFuture<ByteBuf> future = pendingRequests.remove(key);
        if (future == null) {
            closeChannel(
                    ctx.channel(),
                    invalidReply(
                            "no request was pending for operation " + replyOperation + " and request id " + key.id()));
            return;
        }
        ByteBuf body;
        try {
            body = decodeReply(msg);
        } catch (RuntimeException error) {
            future.completeExceptionally(error);
            return;
        }
        if (!future.complete(body)) {
            body.release();
        }
    }

    private void handleEviction(ChannelHandlerContext ctx, ByteBuf frame) {
        IggyServerException error = VsrHeaders.evictionToException(frame);
        session.reset();
        try {
            // The reason travels with the notification so the listener can drop
            // what belonged to the evicted session and say which eviction it
            // was. None of them ends the sign-in: the next request
            // re-establishes the session.
            onEviction.accept(error.getRawErrorCode());
        } catch (RuntimeException listenerError) {
            log.warn("Eviction listener failed: {}", listenerError.getMessage());
        }
        // The server has already unbound this transport. Closing it before
        // completing requests prevents the pool from recycling the channel
        // and assigning a late reply to a request from the next session.
        closeChannel(ctx.channel(), error);
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
        pendingRequests.forEach((key, pending) -> {
            if (pendingRequests.remove(key, pending)) {
                pending.completeExceptionally(cause);
            }
        });
    }

    private record RequestKey(int operation, long id) {}
}
