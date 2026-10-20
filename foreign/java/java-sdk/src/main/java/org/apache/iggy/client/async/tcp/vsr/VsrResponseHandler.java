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
import org.apache.iggy.exception.IggyErrorCode;
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
 * result section stripped (or raised as the typed error). A reply that fails
 * its proof or its decoding closes the channel, like a lost connection.
 */
public class VsrResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(VsrResponseHandler.class);

    private static final int RESULT_COUNT_LEN = 4;
    private static final int RESULT_ENTRY_LEN = 8;
    private static final int REGISTER_BODY_MIN_LEN = 17;

    private final ConcurrentMap<Long, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final ConsensusSession session;
    private final IntConsumer onEviction;
    private final AtomicReference<CloseCause> closeCause = new AtomicReference<>();

    public VsrResponseHandler(ConsensusSession session, IntConsumer onEviction) {
        this.session = session.owner();
        this.onEviction = onEviction;
        session.onChannelCreated();
    }

    public PendingRequest registerRequest(
            Channel channel,
            ByteBuf requestFrame,
            CompletableFuture<ByteBuf> future,
            long deadlineNanos,
            int commandCode) {
        PendingRequest pending = new PendingRequest(
                future,
                VsrHeaders.readRequestOperation(requestFrame),
                VsrHeaders.readRequestId(requestFrame),
                RequestIdentity.from(requestFrame));
        registerRequest(pending);
        long timeoutNanos = Math.max(0, deadlineNanos - System.nanoTime());
        ScheduledFuture<?> timeoutFuture;
        try {
            timeoutFuture = channel.eventLoop()
                    .schedule(
                            () -> future.completeExceptionally(new IggyTimeoutException(
                                    "Timed out waiting for a response to command code " + commandCode)),
                            timeoutNanos,
                            TimeUnit.NANOSECONDS);
        } catch (RuntimeException error) {
            pendingRequests.remove(pending.requestId, pending);
            throw error;
        }
        future.whenComplete((response, error) -> timeoutFuture.cancel(false));
        return pending;
    }

    private void registerRequest(PendingRequest pending) {
        pendingRequests.compute(pending.requestId, (requestId, previous) -> {
            if (previous != null && !previous.future.isDone()) {
                throw new IllegalStateException(
                        "A request is already pending for request id " + Long.toUnsignedString(requestId));
            }
            // Timeout and cancellation callbacks can replay before the previous entry's cleanup runs.
            return pending;
        });
        pending.future.whenComplete((response, error) -> pendingRequests.remove(pending.requestId, pending));
    }

    public void closeChannel(Channel channel, Throwable cause) {
        closeChannel(channel, cause, false);
    }

    private void closeChannel(Channel channel, Throwable cause, boolean detachedSession) {
        closeCause.compareAndSet(null, new CloseCause(cause, detachedSession));
        channel.close();
        failPendingRequests(closeCause.get());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        CloseCause cause = closeCause.get();
        if (cause == null) {
            cause = new CloseCause(new IggyConnectionException("Connection closed before a response arrived"), false);
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
        long requestId = VsrHeaders.readReplyRequestId(msg);
        PendingRequest pending = pendingRequests.remove(requestId);
        if (pending == null) {
            return;
        }
        ByteBuf body;
        try {
            int replyOperation = VsrHeaders.readReplyOperation(msg);
            if (VsrOperation.correlationOperation(replyOperation) != pending.operation) {
                throw invalidReply("reply operation does not match request " + Long.toUnsignedString(requestId));
            }
            body = decodeReply(msg, pending);
        } catch (IggyConnectionException malformed) {
            // An unproved reply leaves the outcome unknown and the channel
            // untrusted. Close it before failing the request, so the replay
            // cannot get this channel from the pool.
            closeChannel(ctx.channel(), malformed);
            pending.future.completeExceptionally(malformed);
            return;
        } catch (RuntimeException error) {
            pending.future.completeExceptionally(error);
            return;
        }
        if (!pending.future.complete(body)) {
            body.release();
        }
    }

    private void handleEviction(ChannelHandlerContext ctx, ByteBuf frame) {
        IggyServerException error = VsrHeaders.evictionToException(frame);
        int reason = frame.getUnsignedByte(frame.readerIndex() + VsrHeaders.EVICTION_REASON_OFFSET);
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
        closeChannel(
                ctx.channel(),
                error,
                reason == VsrHeaders.REASON_NO_SESSION || reason == VsrHeaders.REASON_STALE_CLIENT);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private ByteBuf decodeReply(ByteBuf frame, PendingRequest pending) {
        int operation = validatedOperation(frame);

        int totalSize = (int) VsrHeaders.readSize(frame);
        int bodyStart = frame.readerIndex() + VsrHeaders.HEADER_SIZE;
        int bodyLength = totalSize - VsrHeaders.HEADER_SIZE;

        boolean resultFramed =
                VsrOperation.isResultFramed(operation) || (operation == VsrOperation.REGISTER && bodyLength > 0);
        long resultCode = 0;
        if (resultFramed) {
            int sectionLength = resultSectionLength(frame, bodyStart, bodyLength);
            if (frame.getUnsignedIntLE(bodyStart) > 0) {
                resultCode = frame.getUnsignedIntLE(bodyStart + RESULT_COUNT_LEN + Integer.BYTES);
            }
            bodyStart += sectionLength;
            bodyLength -= sectionLength;
        }
        boolean marked = frame.getUnsignedByte(frame.readerIndex() + VsrHeaders.REPLY_COMMITTED_RESULT_OFFSET)
                == VsrHeaders.COMMITTED_RESULT;
        if (marked && !pending.identity.proves(frame)) {
            throw invalidReply("committed result does not match the pending request");
        }
        pending.committed = marked
                && resultCode != VsrHeaders.ERROR_TRANSIENT_NOT_COMMITTED
                && resultCode != VsrHeaders.ERROR_TRANSIENT_NOT_ACCEPTED
                && resultCode != IggyErrorCode.NOT_PRIMARY.getCode();
        synchronized (session) {
            if (pending.identity.matches(session)) {
                if (pending.committed
                        && (VsrOperation.isMetadata(operation)
                                || operation == VsrOperation.REGISTER
                                || operation == VsrOperation.LOGOUT)) {
                    session.observeMetadata(frame.getLongLE(frame.readerIndex() + VsrHeaders.REPLY_COMMIT_OFFSET));
                }
                if (resultCode == 0) {
                    applySessionEffects(frame, operation, bodyStart, bodyLength);
                }
            }
        }
        resetLogoutSession(pending, operation, resultCode);
        if (resultCode != 0) {
            throw IggyServerException.fromTcpResponse(resultCode, new byte[0]);
        }

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
        return (int) sectionLength;
    }

    private void resetLogoutSession(PendingRequest pending, int operation, long resultCode) {
        if (resultCode == 0 && operation == VsrOperation.LOGOUT) {
            session.resetIfCurrent(pending.identity.clientLow, pending.identity.clientHigh);
        }
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
    }

    private static IggyConnectionException invalidReply(String detail) {
        log.error("Malformed VSR reply: {}", detail);
        return new IggyConnectionException("Malformed VSR reply: " + detail);
    }

    private void failPendingRequests(CloseCause cause) {
        pendingRequests.forEach((key, pending) -> {
            if (pendingRequests.remove(key, pending)) {
                pending.failedByLink = true;
                pending.detachedSession = cause.detachedSession();
                pending.future.completeExceptionally(cause.error());
            }
        });
    }

    public static final class PendingRequest {
        private final CompletableFuture<ByteBuf> future;
        private final int operation;
        private final long requestId;
        private final RequestIdentity identity;
        private volatile boolean committed;
        private volatile boolean failedByLink;
        private volatile boolean detachedSession;

        private PendingRequest(
                CompletableFuture<ByteBuf> future, int operation, long requestId, RequestIdentity identity) {
            this.future = future;
            this.operation = operation;
            this.requestId = requestId;
            this.identity = identity;
        }

        public boolean isCommitted() {
            return committed;
        }

        /** Whether the link failed this request, so no reply answered it. */
        public boolean isFailedByLink() {
            return failedByLink;
        }

        public boolean isSessionDetached() {
            return detachedSession;
        }
    }

    private record RequestIdentity(
            long clientLow, long clientHigh, long checksumLow, long checksumHigh, long incarnation) {
        static RequestIdentity from(ByteBuf frame) {
            int base = frame.readerIndex();
            return new RequestIdentity(
                    frame.getLongLE(base + VsrHeaders.REQUEST_CLIENT_OFFSET),
                    frame.getLongLE(base + VsrHeaders.REQUEST_CLIENT_OFFSET + Long.BYTES),
                    frame.getLongLE(base + VsrHeaders.REQUEST_CHECKSUM_OFFSET),
                    frame.getLongLE(base + VsrHeaders.REQUEST_CHECKSUM_OFFSET + Long.BYTES),
                    frame.getLongLE(base + VsrHeaders.REQUEST_INCARNATION_OFFSET));
        }

        boolean matches(ConsensusSession session) {
            return session.clientIdLow() == clientLow && session.clientIdHigh() == clientHigh;
        }

        boolean proves(ByteBuf frame) {
            int base = frame.readerIndex();
            long op = frame.getLongLE(base + VsrHeaders.REPLY_OP_OFFSET);
            return frame.getUnsignedByte(base + VsrHeaders.REPLY_COMMITTED_RESULT_OFFSET) == VsrHeaders.COMMITTED_RESULT
                    && frame.getLongLE(base + VsrHeaders.REPLY_CLIENT_OFFSET) == clientLow
                    && frame.getLongLE(base + VsrHeaders.REPLY_CLIENT_OFFSET + Long.BYTES) == clientHigh
                    && frame.getLongLE(base + VsrHeaders.REPLY_CHECKSUM_OFFSET) == checksumLow
                    && frame.getLongLE(base + VsrHeaders.REPLY_CHECKSUM_OFFSET + Long.BYTES) == checksumHigh
                    && frame.getLongLE(base + VsrHeaders.REPLY_INCARNATION_OFFSET) == incarnation
                    && op != 0
                    && op == frame.getLongLE(base + VsrHeaders.REPLY_COMMIT_OFFSET);
        }
    }

    private record CloseCause(Throwable error, boolean detachedSession) {}
}
