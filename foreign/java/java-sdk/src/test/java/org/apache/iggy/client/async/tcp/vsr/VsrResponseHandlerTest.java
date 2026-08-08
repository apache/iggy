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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VsrResponseHandlerTest {

    private final ConsensusSession session = new ConsensusSession();
    private final AtomicBoolean evicted = new AtomicBoolean();
    private final VsrResponseHandler handler = new VsrResponseHandler(session, () -> evicted.set(true));
    private final EmbeddedChannel channel = new EmbeddedChannel(handler);

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldFailWithStatusCodeOnDeniedReply() {
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf frame = replyFrame(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
        frame.setIntLE(VsrHeaders.REPLY_STATUS_OFFSET, 40);

        channel.writeInbound(frame);

        assertThat(rawErrorCode(future)).isEqualTo(40);
    }

    @Test
    void shouldPassNonReplicatedBodyThrough() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(1234);
        channel.writeInbound(replyFrame(VsrOperation.NON_REPLICATED, body));

        ByteBuf response = future.get();
        try {
            assertThat(response.readIntLE()).isEqualTo(1234);
        } finally {
            response.release();
        }
    }

    @Test
    void shouldReleaseResponseBodyWhenRequestWasCancelled() {
        CompletableFuture<ByteBuf> future = enqueue();
        future.cancel(false);
        ByteBuf frame =
                replyFrame(VsrOperation.NON_REPLICATED, Unpooled.buffer().writeIntLE(1234));

        channel.writeInbound(frame);

        assertThat(frame.refCnt()).isZero();
    }

    @Test
    void shouldStripResultSectionFromMetadataReply() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(777);
        channel.writeInbound(replyFrame(VsrOperation.CREATE_STREAM, body));

        ByteBuf response = future.get();
        try {
            assertThat(response.readIntLE()).isEqualTo(777);
        } finally {
            response.release();
        }
    }

    @Test
    void shouldSurfaceCommittedErrorFromMetadataRejection() {
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(1);
        body.writeIntLE(0);
        body.writeIntLE(1010);
        channel.writeInbound(replyFrame(VsrOperation.DELETE_STREAM, body));

        assertThat(rawErrorCode(future)).isEqualTo(1010);
    }

    @Test
    void shouldBindSessionOnRegisterReply() throws Exception {
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0); // result section: success
        body.writeIntLE(1); // user id
        body.writeLongLE(99); // session epoch
        body.writeIntLE(VsrLoginCodec.PROTOCOL_VERSION);
        body.writeByte(3);
        body.writeBytes("0.1".getBytes());
        channel.writeInbound(replyFrame(VsrOperation.REGISTER, body));

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
        CompletableFuture<ByteBuf> future = enqueue();
        channel.writeInbound(replyFrame(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER));

        future.get().release();
        assertThat(session.isBound()).isFalse();
    }

    @Test
    void shouldMapEvictionReasonAndResetSession() {
        session.beginRegister();
        session.bind(42);
        CompletableFuture<ByteBuf> future = enqueue();
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_EVICTION);
        frame.setByte(VsrHeaders.EVICTION_REASON_OFFSET, VsrHeaders.REASON_INVALID_CREDENTIALS);
        channel.writeInbound(frame);

        assertThat(rawErrorCode(future)).isEqualTo(42);
        assertThat(session.isBound()).isFalse();
        assertThat(evicted).isTrue();
    }

    @Test
    void shouldFailPendingRequestsWhenChannelBecomesInactive() {
        CompletableFuture<ByteBuf> pending = enqueue();

        channel.close();

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyConnectionException.class);
    }

    @Test
    void shouldFailPendingRequestsOnPipelineException() {
        CompletableFuture<ByteBuf> pending = enqueue();

        var failure = new IllegalStateException("broken pipe");
        channel.pipeline().fireExceptionCaught(failure);

        assertThat(pending).isCompletedExceptionally();
        assertThatThrownBy(pending::join).hasCause(failure);
    }

    @Test
    void shouldCloseChannelWhenResponseDeadlineExpires() {
        CompletableFuture<ByteBuf> pending = new CompletableFuture<>();
        handler.enqueueRequest(channel, pending, System.nanoTime(), 1);

        channel.runScheduledPendingTasks();

        assertThat(channel.isActive()).isFalse();
        assertThatThrownBy(pending::join).hasCauseInstanceOf(IggyTimeoutException.class);
    }

    private CompletableFuture<ByteBuf> enqueue() {
        CompletableFuture<ByteBuf> future = new CompletableFuture<>();
        handler.enqueueRequest(future);
        return future;
    }

    private static ByteBuf emptyFrame() {
        ByteBuf frame = Unpooled.buffer(VsrHeaders.HEADER_SIZE);
        frame.writeZero(VsrHeaders.HEADER_SIZE);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE);
        return frame;
    }

    private static ByteBuf replyFrame(int operation, ByteBuf body) {
        ByteBuf frame = emptyFrame();
        frame.setByte(VsrHeaders.COMMAND_OFFSET, VsrHeaders.COMMAND_REPLY);
        frame.setByte(VsrHeaders.REPLY_OPERATION_OFFSET, operation);
        frame.setIntLE(VsrHeaders.SIZE_OFFSET, VsrHeaders.HEADER_SIZE + body.readableBytes());
        frame.writeBytes(body);
        body.release();
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
