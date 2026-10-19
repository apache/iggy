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
import org.apache.iggy.exception.IggyAuthenticationException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrResponseHandlerTest {

    private static final int LOGIN_USER_CODE = 38;
    private static final int SEND_MESSAGES_CODE = 101;

    private final ConsensusSession session = new ConsensusSession();
    private final AtomicInteger evictions = new AtomicInteger();
    private final AtomicInteger lastEvictionReason = new AtomicInteger();
    private final VsrResponseHandler handler = new VsrResponseHandler(session, errorCode -> {
        evictions.incrementAndGet();
        lastEvictionReason.set(errorCode);
    });
    private final EmbeddedChannel channel = new EmbeddedChannel(handler);

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailWithStatusCodeOnDeniedReply() {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.NON_REPLICATED, 7);
        ByteBuf frame = replyFrame(VsrOperation.NON_REPLICATED, 7, Unpooled.EMPTY_BUFFER);
        frame.setIntLE(VsrHeaders.REPLY_STATUS_OFFSET, 40);

        channel.writeInbound(frame);

        assertThat(rawErrorCode(future)).isEqualTo(40);
    }

    @Test
    void shouldPassNonReplicatedBodyThrough() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.NON_REPLICATED, 7);
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(1234);
        channel.writeInbound(replyFrame(VsrOperation.NON_REPLICATED, 7, body));

        ByteBuf response = future.get();
        try {
            assertThat(response.readIntLE()).isEqualTo(1234);
        } finally {
            response.release();
        }
    }

    @Test
    void shouldReleaseResponseBodyWhenRequestWasCancelled() {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.NON_REPLICATED, 7);
        future.cancel(false);
        ByteBuf frame =
                replyFrame(VsrOperation.NON_REPLICATED, 7, Unpooled.buffer().writeIntLE(1234));

        channel.writeInbound(frame);

        assertThat(frame.refCnt()).isZero();
    }

    @Test
    void shouldStripResultSectionFromMetadataReply() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.CREATE_STREAM, 7);
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(777);
        channel.writeInbound(replyFrame(VsrOperation.CREATE_STREAM, 7, body));

        ByteBuf response = future.get();
        try {
            assertThat(response.readIntLE()).isEqualTo(777);
        } finally {
            response.release();
        }
    }

    @Test
    void shouldObserveTruncateCommitWithoutUsingPartitionCommitAsMetadata() throws Exception {
        CompletableFuture<ByteBuf> truncated = enqueue(VsrOperation.DELETE_SEGMENTS, 7);
        ByteBuf truncateReply =
                replyFrame(VsrOperation.TRUNCATE_PARTITION, 7, Unpooled.buffer().writeIntLE(0));
        truncateReply.setLongLE(VsrHeaders.REPLY_COMMIT_OFFSET, 11);
        truncateReply.setLongLE(VsrHeaders.REPLY_OP_OFFSET, 11);
        channel.writeInbound(truncateReply);
        truncated.get().release();
        assertThat(session.metadataWatermark()).isEqualTo(11);

        CompletableFuture<ByteBuf> sent = enqueue(VsrOperation.SEND_MESSAGES, 8);
        ByteBuf partitionReply = replyFrame(VsrOperation.SEND_MESSAGES, 8, Unpooled.EMPTY_BUFFER);
        partitionReply.setLongLE(VsrHeaders.REPLY_COMMIT_OFFSET, 99);
        partitionReply.setLongLE(VsrHeaders.REPLY_OP_OFFSET, 99);
        channel.writeInbound(partitionReply);
        sent.get().release();
        assertThat(session.metadataWatermark()).isEqualTo(11);
    }

    @Test
    void shouldSurfaceCommittedErrorFromMetadataRejection() {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.DELETE_STREAM, 7);
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(1);
        body.writeIntLE(0);
        body.writeIntLE(1010);
        channel.writeInbound(replyFrame(VsrOperation.DELETE_STREAM, 7, body));

        assertThat(rawErrorCode(future)).isEqualTo(1010);
    }

    @Test
    void shouldBindSessionOnRegisterReply() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.REGISTER, 0);
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0); // result section: success
        body.writeIntLE(1); // user id
        body.writeLongLE(99); // session epoch
        body.writeIntLE(VsrLoginCodec.PROTOCOL_VERSION);
        body.writeByte(3);
        body.writeBytes("0.1".getBytes());
        channel.writeInbound(replyFrame(VsrOperation.REGISTER, 0, body));

        ByteBuf response = future.get();
        try {
            assertThat(response.readUnsignedIntLE()).isEqualTo(1);
            assertThat(session.isBound()).isTrue();
            assertThat(session.boundSession()).isEqualTo(99);
        } finally {
            response.release();
        }
    }

    @Test
    void shouldResetSessionOnLogoutReply() throws Exception {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.LOGOUT, 7);
        channel.writeInbound(replyFrame(VsrOperation.LOGOUT, 7, Unpooled.EMPTY_BUFFER));

        future.get().release();
        assertThat(session.isBound()).isFalse();
    }

    @Test
    void shouldReleaseLogoutWaitersOutsideTheSessionMonitor() throws Exception {
        session.bind(42);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        var first = session.acquire(
                        ConsensusSession.LaneKey.METADATA, true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                .join();
        var waiting = session.acquire(
                ConsensusSession.LaneKey.METADATA, true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
        var heldMonitor = new AtomicBoolean();
        var continued = waiting.whenComplete((ticket, error) -> heldMonitor.set(Thread.holdsLock(session)));
        var logout = enqueue(VsrOperation.LOGOUT, 7);

        channel.writeInbound(replyFrame(VsrOperation.LOGOUT, 7, Unpooled.EMPTY_BUFFER));

        logout.get().release();
        continued.get().complete();
        first.complete();
        assertThat(heldMonitor).isFalse();
    }

    @Test
    void shouldIgnoreLogoutResetAfterAnotherIdentityWasEstablished() {
        session.bind(42);
        long oldLow = session.clientIdLow();
        long oldHigh = session.clientIdHigh();
        session.reset();
        session.bind(43);

        session.resetIfCurrent(oldLow, oldHigh);

        assertThat(session.boundSession()).isEqualTo(43);
    }

    @Test
    void shouldMapEvictionReasonAndRetainSessionUntilBindIsRefused() {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.NON_REPLICATED, 7);
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_EVICTION);
        frame.setByte(VsrHeaders.EVICTION_REASON_OFFSET, VsrHeaders.REASON_INVALID_CREDENTIALS);
        channel.writeInbound(frame);

        assertThat(rawErrorCode(future)).isEqualTo(42);
        assertThat(session.isBound()).isTrue();
        assertThat(evictions).hasValue(1);
        // The reason reaches the listener, which has to tell an eviction the
        // server decided on from a transport-shaped one.
        assertThat(lastEvictionReason).hasValue(42);
    }

    @Test
    void shouldCloseChannelOnEvictionWithoutPendingRequest() {
        session.beginRegister();
        session.bind(42);
        ByteBuf frame = evictionFrame(VsrHeaders.REASON_STALE_CLIENT);

        channel.writeInbound(frame);

        assertThat(session.isBound()).isTrue();
        assertThat(evictions).hasValue(1);
        assertThat(lastEvictionReason).hasValue(VsrHeaders.ERROR_STALE_CLIENT);
        assertThat(frame.refCnt()).isZero();
        assertThat(channel.isActive()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {VsrHeaders.REASON_STALE_CLIENT, VsrHeaders.REASON_NO_SESSION})
    void shouldFailEveryPendingRequestOnEviction(int reason) {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> first = enqueue(VsrOperation.NON_REPLICATED, 7);
        CompletableFuture<ByteBuf> second = enqueue(VsrOperation.CREATE_STREAM, 8);

        channel.writeInbound(evictionFrame(reason));

        int errorCode = reason == VsrHeaders.REASON_NO_SESSION
                ? VsrHeaders.ERROR_UNAUTHENTICATED
                : VsrHeaders.ERROR_STALE_CLIENT;
        assertThat(rawErrorCode(first)).isEqualTo(errorCode);
        assertThat(rawErrorCode(second)).isEqualTo(errorCode);
        if (reason == VsrHeaders.REASON_NO_SESSION) {
            assertThatThrownBy(first::join).hasCauseInstanceOf(IggyAuthenticationException.class);
            assertThatThrownBy(second::join).hasCauseInstanceOf(IggyAuthenticationException.class);
        }
        assertThat(evictions).hasValue(1);
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldFailPendingRequestsWhenChannelBecomesInactive() {
        CompletableFuture<ByteBuf> pending = enqueue(VsrOperation.NON_REPLICATED, 7);

        channel.close();

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyConnectionException.class);
    }

    @Test
    void shouldFailPendingRequestsOnPipelineException() {
        CompletableFuture<ByteBuf> pending = enqueue(VsrOperation.NON_REPLICATED, 7);

        var failure = new IllegalStateException("broken pipe");
        channel.pipeline().fireExceptionCaught(failure);

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCause(failure);
    }

    @Test
    void shouldExpireOnlyOneRequestAndIgnoreItsLateReply() {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        ByteBuf request = requestFrame(VsrOperation.NON_REPLICATED, 7);
        handler.registerRequest(channel, request, pending, System.nanoTime(), 1);
        request.release();

        CompletableFuture<ByteBuf> other = enqueue(VsrOperation.NON_REPLICATED, 8);
        channel.runScheduledPendingTasks();
        channel.writeInbound(replyFrame(VsrOperation.NON_REPLICATED, 7, Unpooled.EMPTY_BUFFER));
        channel.writeInbound(
                replyFrame(VsrOperation.NON_REPLICATED, 8, Unpooled.buffer().writeByte(42)));

        assertThat(channel.isActive()).isTrue();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyTimeoutException.class);
        ByteBuf response = other.join();
        assertThat(response.readByte()).isEqualTo((byte) 42);
        response.release();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldRegisterReplayFromTimeoutOrCancellationCallback(boolean cancel) {
        long requestId = 7;
        CompletableFuture<ByteBuf> attempt = new CompletableFuture<>();
        ByteBuf request = requestFrame(VsrOperation.SEND_MESSAGES, requestId);
        handler.registerRequest(channel, request, attempt, System.nanoTime(), SEND_MESSAGES_CODE);
        request.release();
        var replay = attempt.handle((response, error) -> enqueue(VsrOperation.SEND_MESSAGES, requestId))
                .thenCompose(future -> future);

        if (cancel) {
            attempt.cancel(false);
        } else {
            channel.runScheduledPendingTasks();
        }
        assertThat(replay).isNotDone();
        channel.writeInbound(replyFrame(
                VsrOperation.SEND_MESSAGES, requestId, Unpooled.buffer().writeByte(42)));

        ByteBuf response = replay.join();
        try {
            assertThat(response.readByte()).isEqualTo((byte) 42);
        } finally {
            response.release();
        }
        assertThat(channel.isActive()).isTrue();
    }

    @Test
    void shouldRejectDuplicateRegistrationWhileTheRequestIsPending() {
        long requestId = 7;
        var original = enqueue(VsrOperation.SEND_MESSAGES, requestId);
        ByteBuf duplicate = requestFrame(VsrOperation.SEND_MESSAGES, requestId);
        try {
            assertThatThrownBy(() -> handler.registerRequest(
                            channel, duplicate, new CompletableFuture<>(), System.nanoTime(), SEND_MESSAGES_CODE))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            duplicate.release();
        }

        channel.writeInbound(replyFrame(VsrOperation.SEND_MESSAGES, requestId, Unpooled.EMPTY_BUFFER));
        original.join().release();
    }

    @Test
    void shouldCorrelateRepliesArrivingInReverseOrder() throws Exception {
        CompletableFuture<ByteBuf> first = enqueue(VsrOperation.SEND_MESSAGES, 7);
        CompletableFuture<ByteBuf> second = enqueue(VsrOperation.SEND_MESSAGES, 8);

        channel.writeInbound(replyFrame(VsrOperation.SEND_MESSAGES, 8, Unpooled.wrappedBuffer(new byte[] {2})));
        channel.writeInbound(replyFrame(VsrOperation.SEND_MESSAGES, 7, Unpooled.wrappedBuffer(new byte[] {1})));

        ByteBuf firstResponse = first.get();
        ByteBuf secondResponse = second.get();
        try {
            assertThat(firstResponse.readByte()).isEqualTo((byte) 1);
            assertThat(secondResponse.readByte()).isEqualTo((byte) 2);
        } finally {
            firstResponse.release();
            secondResponse.release();
        }
    }

    @Test
    void shouldCorrelateASendInFlightAcrossReLogin() throws Exception {
        // A re-login re-arms the session while an earlier send is still
        // pending. Replies correlate by (operation, request id), so the first
        // send of the new session must not claim the in-flight one's key:
        // registering it would be refused and a late reply for the older send
        // would be handed to the newer one.
        VsrRequestEncoder encoder = new VsrRequestEncoder(session);
        session.beginRegister();
        session.bind(42);

        CompletableFuture<ByteBuf> inFlight = new CompletableFuture<>();
        long beforeLoginId = registerEncodedSend(encoder, inFlight);
        long[] beforeIdentity = session.identity();

        session.reset();
        ByteBuf loginPayload = loginUserPayload();
        encoder.encode(channel.alloc(), LOGIN_USER_CODE, loginPayload).release();
        loginPayload.release();
        session.bind(43);

        CompletableFuture<ByteBuf> afterLogin = new CompletableFuture<>();
        long afterLoginId = registerEncodedSend(encoder, afterLogin);

        assertThat(afterLoginId).isNotEqualTo(beforeLoginId);
        assertThat(channel.isActive()).isTrue();

        ByteBuf afterReply =
                replyFrame(VsrOperation.SEND_MESSAGES, afterLoginId, Unpooled.wrappedBuffer(new byte[] {2}));
        afterReply.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET, session.identity()[0]);
        afterReply.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET + Long.BYTES, session.identity()[1]);
        ByteBuf beforeReply =
                replyFrame(VsrOperation.SEND_MESSAGES, beforeLoginId, Unpooled.wrappedBuffer(new byte[] {1}));
        beforeReply.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET, beforeIdentity[0]);
        beforeReply.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET + Long.BYTES, beforeIdentity[1]);
        channel.writeInbound(afterReply);
        channel.writeInbound(beforeReply);

        ByteBuf inFlightResponse = inFlight.get();
        ByteBuf afterLoginResponse = afterLogin.get();
        try {
            assertThat(inFlightResponse.readByte()).isEqualTo((byte) 1);
            assertThat(afterLoginResponse.readByte()).isEqualTo((byte) 2);
        } finally {
            inFlightResponse.release();
            afterLoginResponse.release();
        }
    }

    @Test
    void shouldPreserveMetadataFloorAndFenceOldSessionReplyEffectsAfterReset() throws Exception {
        session.bind(42);
        session.observeMetadata(11);
        var metadata = enqueue(VsrOperation.CREATE_STREAM, 7);
        var logout = enqueue(VsrOperation.LOGOUT, 8);
        ByteBuf oldMetadata =
                replyFrame(VsrOperation.CREATE_STREAM, 7, Unpooled.buffer().writeIntLE(0));
        oldMetadata.setLongLE(VsrHeaders.REPLY_OP_OFFSET, 22);
        oldMetadata.setLongLE(VsrHeaders.REPLY_COMMIT_OFFSET, 22);
        ByteBuf oldLogout = replyFrame(VsrOperation.LOGOUT, 8, Unpooled.EMPTY_BUFFER);
        session.reset();
        session.bind(43);
        channel.writeInbound(oldMetadata);
        channel.writeInbound(oldLogout);
        metadata.get().release();
        logout.get().release();
        assertThat(session.boundSession()).isEqualTo(43);
        assertThat(session.metadataWatermark()).isEqualTo(11);
    }

    @Test
    void shouldCorrelateRepliesForServerRewrittenOperations() throws Exception {
        int[][] rewrittenOperations = {
            {VsrOperation.CREATE_TOPIC, VsrOperation.CREATE_TOPIC_WITH_ASSIGNMENTS},
            {VsrOperation.CREATE_PARTITIONS, VsrOperation.CREATE_PARTITIONS_WITH_ASSIGNMENTS},
            {VsrOperation.DELETE_SEGMENTS, VsrOperation.TRUNCATE_PARTITION}
        };

        for (int index = 0; index < rewrittenOperations.length; index++) {
            int requestOperation = rewrittenOperations[index][0];
            int replyOperation = rewrittenOperations[index][1];
            long requestId = index + 1;
            CompletableFuture<ByteBuf> future = enqueue(requestOperation, requestId);
            ByteBuf body = Unpooled.buffer();
            body.writeIntLE(0);
            body.writeIntLE(100 + index);

            channel.writeInbound(replyFrame(replyOperation, requestId, body));

            ByteBuf response = future.get();
            try {
                assertThat(response.readIntLE()).isEqualTo(100 + index);
            } finally {
                response.release();
            }
        }
    }

    @Test
    void shouldRejectUnrelatedReplyOperation() {
        CompletableFuture<ByteBuf> pending = enqueue(VsrOperation.CREATE_TOPIC, 7);
        CompletableFuture<ByteBuf> other = enqueue(VsrOperation.CREATE_STREAM, 8);

        channel.writeInbound(
                replyFrame(VsrOperation.DELETE_TOPIC, 7, Unpooled.buffer().writeIntLE(0)));

        assertThat(channel.isActive()).isFalse();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyConnectionException.class);
        assertThatThrownBy(other::join).hasCauseInstanceOf(IggyConnectionException.class);
    }

    @Test
    void shouldProveCommittedBusinessRejectionOnlyForTheExactRequest() {
        int[] mismatchedOffsets = {
            VsrHeaders.REPLY_CLIENT_OFFSET, VsrHeaders.REPLY_CHECKSUM_OFFSET,
            VsrHeaders.REPLY_INCARNATION_OFFSET, VsrHeaders.REPLY_OP_OFFSET,
            VsrHeaders.REPLY_COMMITTED_RESULT_OFFSET
        };
        for (int index = 0; index <= mismatchedOffsets.length; index++) {
            boolean unproved = index < mismatchedOffsets.length - 1;
            VsrResponseHandler link = new VsrResponseHandler(session, errorCode -> {});
            EmbeddedChannel linkChannel = new EmbeddedChannel(link);
            try {
                long requestId = index + 1;
                CompletableFuture<ByteBuf> result = new CompletableFuture<>();
                ByteBuf request = requestFrame(VsrOperation.DELETE_STREAM, requestId);
                request.setLongLE(0, 99);
                request.setLongLE(VsrHeaders.REQUEST_CHECKSUM_OFFSET, 13);
                VsrResponseHandler.PendingRequest pending = link.registerRequest(
                        linkChannel, request, result, System.nanoTime() + TimeUnit.MINUTES.toNanos(1), 203);
                request.release();
                ByteBuf body = Unpooled.buffer().writeIntLE(1).writeIntLE(0).writeIntLE(1010);
                ByteBuf reply = replyFrame(VsrOperation.DELETE_STREAM, requestId, body);
                reply.setLongLE(VsrHeaders.REPLY_CHECKSUM_OFFSET, 13);
                if (index < mismatchedOffsets.length) {
                    int offset = mismatchedOffsets[index];
                    reply.setByte(offset, reply.getByte(offset) + 1);
                }
                linkChannel.writeInbound(reply);
                if (unproved) {
                    assertThatThrownBy(result::join)
                            .as("proof mismatch case %s", index)
                            .hasCauseInstanceOf(IggyConnectionException.class);
                } else {
                    assertThat(rawErrorCode(result)).isEqualTo(1010);
                }
                assertThat(linkChannel.isActive())
                        .as("proof mismatch case %s", index)
                        .isEqualTo(!unproved);
                assertThat(pending.isCommitted())
                        .as("proof mismatch case %s", index)
                        .isEqualTo(index == mismatchedOffsets.length);
            } finally {
                linkChannel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void shouldIgnoreCancelledMetadataReplyWithoutAdvancingTheFrontier() {
        CompletableFuture<ByteBuf> cancelled = enqueue(VsrOperation.CREATE_STREAM, 7);
        CompletableFuture<ByteBuf> other = enqueue(VsrOperation.NON_REPLICATED, 8);
        cancelled.cancel(false);
        channel.writeInbound(
                replyFrame(VsrOperation.CREATE_STREAM, 7, Unpooled.buffer().writeIntLE(0)));
        channel.writeInbound(replyFrame(VsrOperation.NON_REPLICATED, 8, Unpooled.EMPTY_BUFFER));
        other.join().release();
        assertThat(channel.isActive()).isTrue();
        assertThat(session.metadataWatermark()).isZero();
    }

    private CompletableFuture<ByteBuf> enqueue(int operation, long requestId) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        ByteBuf request = requestFrame(operation, requestId);
        handler.registerRequest(channel, request, future, System.nanoTime() + TimeUnit.MINUTES.toNanos(1), 1);
        request.release();
        return future;
    }

    /**
     * Encodes a partition send off the live session and registers it the way
     * the connection does, returning the request id the encoder minted.
     */
    private long registerEncodedSend(VsrRequestEncoder encoder, CompletableFuture<ByteBuf> future) {
        ByteBuf frame = encoder.encode(channel.alloc(), SEND_MESSAGES_CODE, Unpooled.EMPTY_BUFFER);
        try {
            handler.registerRequest(
                    channel, frame, future, System.nanoTime() + TimeUnit.MINUTES.toNanos(1), SEND_MESSAGES_CODE);
            return VsrHeaders.readRequestId(frame);
        } finally {
            frame.release();
        }
    }

    private static ByteBuf loginUserPayload() {
        ByteBuf payload = Unpooled.buffer();
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeByte(4);
        payload.writeBytes("iggy".getBytes(StandardCharsets.UTF_8));
        payload.writeIntLE(0);
        payload.writeIntLE(0);
        return payload;
    }

    private static ByteBuf emptyFrame() {
        ByteBuf frame = Unpooled.buffer(VsrHeaders.HEADER_SIZE);
        frame.writeZero(VsrHeaders.HEADER_SIZE);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE);
        return frame;
    }

    private ByteBuf requestFrame(int operation, long requestId) {
        ByteBuf frame = emptyFrame();
        frame.setLongLE(VsrHeaders.REQUEST_CLIENT_OFFSET, session.clientIdLow());
        frame.setLongLE(VsrHeaders.REQUEST_CLIENT_OFFSET + Long.BYTES, session.clientIdHigh());
        frame.setByte(VsrHeaders.REQUEST_OPERATION_OFFSET, operation);
        frame.setLongLE(VsrHeaders.REQUEST_ID_OFFSET, requestId);
        return frame;
    }

    private ByteBuf replyFrame(int operation, long requestId, ByteBuf body) {
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_REPLY);
        frame.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET, session.clientIdLow());
        frame.setLongLE(VsrHeaders.REPLY_CLIENT_OFFSET + Long.BYTES, session.clientIdHigh());
        frame.setByte(VsrHeaders.REPLY_OPERATION_OFFSET, operation);
        frame.setLongLE(VsrHeaders.REPLY_REQUEST_OFFSET, requestId);
        frame.setByte(VsrHeaders.REPLY_COMMITTED_RESULT_OFFSET, VsrHeaders.COMMITTED_RESULT);
        frame.setLongLE(VsrHeaders.REPLY_OP_OFFSET, 1);
        frame.setLongLE(VsrHeaders.REPLY_COMMIT_OFFSET, 1);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE + body.readableBytes());
        frame.writeBytes(body);
        body.release();
        return frame;
    }

    private static ByteBuf evictionFrame(int reason) {
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_EVICTION);
        frame.setByte(VsrHeaders.EVICTION_REASON_OFFSET, reason);
        return frame;
    }

    private static int rawErrorCode(CompletableFuture<ByteBuf> future) {
        try {
            future.get().release();
            throw new AssertionError("Expected the response future to fail");
        } catch (ExecutionException e) {
            assertThat(e.getCause()).isInstanceOf(IggyServerException.class);
            return ((IggyServerException) e.getCause()).getRawErrorCode();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
