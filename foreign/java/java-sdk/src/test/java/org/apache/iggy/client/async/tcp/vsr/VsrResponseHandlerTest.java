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
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrResponseHandlerTest {

    private final ConsensusSession session = new ConsensusSession();
    private final AtomicInteger evictions = new AtomicInteger();
    private final VsrResponseHandler handler = new VsrResponseHandler(session, evictions::incrementAndGet);
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
    void shouldMapEvictionReasonAndResetSession() {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> future = enqueue(VsrOperation.NON_REPLICATED, 7);
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_EVICTION);
        frame.setByte(VsrHeaders.EVICTION_REASON_OFFSET, VsrHeaders.REASON_INVALID_CREDENTIALS);
        channel.writeInbound(frame);

        assertThat(rawErrorCode(future)).isEqualTo(42);
        assertThat(session.isBound()).isFalse();
        assertThat(evictions).hasValue(1);
    }

    @Test
    void shouldCloseChannelOnEvictionWithoutPendingRequest() {
        session.beginRegister();
        session.bind(42);
        ByteBuf frame = evictionFrame(VsrHeaders.REASON_STALE_CLIENT);

        channel.writeInbound(frame);

        assertThat(session.isBound()).isFalse();
        assertThat(evictions).hasValue(1);
        assertThat(frame.refCnt()).isZero();
        assertThat(channel.isActive()).isFalse();
    }

    @Test
    void shouldFailEveryPendingRequestOnEviction() {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> first = enqueue(VsrOperation.NON_REPLICATED, 7);
        CompletableFuture<ByteBuf> second = enqueue(VsrOperation.CREATE_STREAM, 8);

        channel.writeInbound(evictionFrame(VsrHeaders.REASON_STALE_CLIENT));

        assertThat(rawErrorCode(first)).isEqualTo(VsrHeaders.ERROR_STALE_CLIENT);
        assertThat(rawErrorCode(second)).isEqualTo(VsrHeaders.ERROR_STALE_CLIENT);
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
    void shouldCloseChannelWhenResponseDeadlineExpires() {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        ByteBuf request = requestFrame(VsrOperation.NON_REPLICATED, 7);
        handler.registerRequest(channel, request, pending, System.nanoTime(), 1);
        request.release();

        channel.runScheduledPendingTasks();

        assertThat(channel.isActive()).isFalse();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyTimeoutException.class);
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

        channel.writeInbound(
                replyFrame(VsrOperation.DELETE_TOPIC, 7, Unpooled.buffer().writeIntLE(0)));

        assertThat(channel.isActive()).isFalse();
        assertThat(rawErrorCode(pending)).isEqualTo(VsrHeaders.ERROR_INVALID_COMMAND);
    }

    private CompletableFuture<ByteBuf> enqueue(int operation, long requestId) {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        handler.registerRequest(future, operation, requestId);
        return future;
    }

    private static ByteBuf emptyFrame() {
        ByteBuf frame = Unpooled.buffer(VsrHeaders.HEADER_SIZE);
        frame.writeZero(VsrHeaders.HEADER_SIZE);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE);
        return frame;
    }

    private static ByteBuf requestFrame(int operation, long requestId) {
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.REQUEST_OPERATION_OFFSET, operation);
        frame.setLongLE(VsrHeaders.REQUEST_ID_OFFSET, requestId);
        return frame;
    }

    private static ByteBuf replyFrame(int operation, long requestId, ByteBuf body) {
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_REPLY);
        frame.setByte(VsrHeaders.REPLY_OPERATION_OFFSET, operation);
        frame.setLongLE(VsrHeaders.REPLY_REQUEST_OFFSET, requestId);
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
