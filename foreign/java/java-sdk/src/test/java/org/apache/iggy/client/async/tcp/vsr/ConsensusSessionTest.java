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
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsensusSessionTest {
    private static final long DEADLINE = TimeUnit.MINUTES.toNanos(1);

    @Test
    void shouldAdmitLaneWaitersInOrderAfterThreeAdmissionChanges() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var lane = new ConsensusSession.LaneKey(7, 1);
        var first = session.acquire(
                        lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                .join();
        first.startWrite();
        var second =
                session.acquire(lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        var third =
                session.acquire(lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        var fourth =
                session.acquire(lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        try {
            var independent = session.acquire(
                            new ConsensusSession.LaneKey(8, 1),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join();
            independent.complete();
            first.finishWrite();
            first.complete();

            assertThat(second).isDone();
            assertThat(third).isNotDone();
            assertThat(fourth).isNotDone();
            second.join().complete();
            assertThat(third).isDone();
            assertThat(fourth).isNotDone();
            third.join().complete();
            assertThat(fourth).isDone();
            fourth.join().complete();
        } finally {
            second.cancel(false);
            third.cancel(false);
            fourth.cancel(false);
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepRetainedLaneBlockedWhileAnotherPartitionProgresses() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            var lane = new ConsensusSession.LaneKey(7, 1);
            var ticket = session.acquire(
                            lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                    .join();
            freezeAndSend(ticket, 9);
            var uncertain = (IggyOperationException) ticket.retain();
            assertThat(uncertain.getOperationId()).isEqualTo(9);
            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
            assertThatThrownBy(() -> session.acquire(
                                    lane,
                                    true,
                                    VsrHeaders.HEADER_SIZE,
                                    System.nanoTime() + DEADLINE,
                                    channel.eventLoop())
                            .join())
                    .hasCauseInstanceOf(IggyOperationException.class)
                    .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                            .isEqualTo(IggyErrorCode.LANE_BLOCKED));
            var independent = session.acquire(
                            new ConsensusSession.LaneKey(8, 1),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join();
            independent.complete();
            assertThat(session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop())
                            .join())
                    .isSameAs(ticket);
            ticket.complete();
            var next = session.acquire(
                            lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                    .join();
            next.complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepWriterBytesChargedAfterAbandonAndAllocateNoQueuedId() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            var ticket = session.acquire(
                            ConsensusSession.LaneKey.METADATA,
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join();
            freezeAndSend(ticket, 9);
            ticket.startWrite();
            ticket.retain();
            session.abandon(9);
            long before = session.currentRequestId();
            CompletableFuture<ConsensusSession.Ticket> cancelled = session.acquire(
                    ConsensusSession.LaneKey.METADATA,
                    true,
                    VsrHeaders.HEADER_SIZE,
                    System.nanoTime() + DEADLINE,
                    channel.eventLoop());
            CompletableFuture<ConsensusSession.Ticket> queued = session.acquire(
                    ConsensusSession.LaneKey.METADATA,
                    true,
                    VsrHeaders.HEADER_SIZE,
                    System.nanoTime() + DEADLINE,
                    channel.eventLoop());
            assertThat(cancelled).isNotDone();
            assertThat(queued).isNotDone();
            assertThat(session.currentRequestId()).isEqualTo(before);
            cancelled.cancel(false);
            assertThat(queued).isNotDone();
            ticket.finishWrite();
            queued.join().complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldReleaseTransientProgressWithoutRetainingItsIdentity() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            var lane = new ConsensusSession.LaneKey(7, 1);
            var poll = session.acquire(
                            lane, false, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                    .join();
            freezeAndSend(poll, 9);
            assertThat(((org.apache.iggy.exception.IggyServerException) poll.retain()).getRawErrorCode())
                    .isEqualTo(57);
            assertOperationError(
                    session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop()),
                    IggyErrorCode.OPERATION_NOT_RETAINED);
            session.acquire(lane, true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                    .join()
                    .complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldPreserveAllUnsignedRequestBitsAndRefuseCounterReuseAcrossReset() throws Exception {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        var counter = ConsensusSession.class.getDeclaredField("requestCounter");
        counter.setAccessible(true);
        counter.setLong(session, Long.MAX_VALUE);
        assertThat(session.nextRequestId()).isEqualTo(Long.MAX_VALUE);
        assertThat(session.nextRequestId()).isEqualTo(Long.MIN_VALUE);
        counter.setLong(session, -1L);
        assertThat(session.nextRequestId()).isEqualTo(-1L);
        session.reset();
        session.bind(43);
        assertThatThrownBy(session::nextRequestId).isInstanceOf(org.apache.iggy.exception.IggyServerException.class);
    }

    @Test
    void shouldMakeAResolverWaitForTheRunningReplayAndClaimWhatItLeavesUncertain() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            var ticket = retainWrite(session, channel, 9);
            var running = session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop());
            var waiting = session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop());
            var expiring = session.resume(9, System.nanoTime(), channel.eventLoop());
            assertThat(running.join()).isSameAs(ticket);
            assertThat(waiting).isNotDone();

            channel.runScheduledPendingTasks();
            assertOperationError(expiring, IggyErrorCode.OPERATION_UNCERTAIN);
            assertThat(waiting).isNotDone();

            ticket.retain();
            assertThat(waiting.join()).isSameAs(ticket);
            var afterProof = session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop());
            ticket.complete();
            assertOperationError(afterProof, IggyErrorCode.OPERATION_NOT_RETAINED);
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepOnlyTheNewestProvedOutcomesAcrossAResetAsCopies() {
        int limit = 2;
        int rejection = IggyErrorCode.STREAM_NAME_NOT_FOUND.getCode();
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(limit, limit * (long) VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        ByteBuf pooledReply = PooledByteBufAllocator.DEFAULT.buffer().writeBytes(new byte[] {7, 8, 9});
        try {
            prove(session, channel, 9, Unpooled.EMPTY_BUFFER, null);
            prove(session, channel, 10, null, IggyServerException.fromTcpResponse(rejection, new byte[0]));
            prove(session, channel, 11, pooledReply, null);
            pooledReply.release();

            session.reset();

            assertThat(pooledReply.refCnt()).isZero();
            assertThatThrownBy(() -> session.provedOutcome(9))
                    .isInstanceOfSatisfying(
                            IggyOperationException.class,
                            notRetained -> assertThat(notRetained.getErrorCode())
                                    .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
            assertThatThrownBy(() -> session.provedOutcome(10))
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            proved -> assertThat(proved.getRawErrorCode()).isEqualTo(rejection));
            session.provedOutcome(11)[0] = 0;
            assertThat(session.provedOutcome(11)).containsExactly(7, 8, 9);
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepTheOutcomeThatAReplayProvesAfterAnAbandon() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            var ticket = retainWrite(session, channel, 9);
            session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop()).join();

            session.abandon(9);
            ticket.recordOutcome(Unpooled.wrappedBuffer(new byte[] {1}), null);
            ticket.complete();

            assertThat(session.provedOutcome(9)).containsExactly(1);
            retainWrite(session, channel, 10).complete();
            assertThatThrownBy(() -> session.provedOutcome(10)).isInstanceOf(IggyOperationException.class);
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldLeaveTheLaneToARunningReplayAfterAnAbandonAndFreeItsSlotOnce() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, 2L * VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var partition = new ConsensusSession.LaneKey(7, 1);
        try {
            var ticket = retainWrite(session, channel, 9);
            session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop()).join();
            var waiting = session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop());
            ByteBuf inFlight = ticket.frame();
            ByteBuf allocation = inFlight;
            while (allocation.unwrap() != null) {
                allocation = allocation.unwrap();
            }

            session.abandon(9);

            var sameLane = acquireMutation(session, channel, ConsensusSession.LaneKey.METADATA, VsrHeaders.HEADER_SIZE);
            assertThat(sameLane).isNotDone();
            var otherLane = acquireMutation(session, channel, partition, VsrHeaders.HEADER_SIZE);
            assertThat(otherLane).isDone();
            ticket.startWrite();
            ticket.finishWrite();
            ticket.frame().release();
            ticket.finishWrite();
            assertThat(waiting).isNotDone();

            ticket.recordOutcome(Unpooled.wrappedBuffer(new byte[] {1}), null);
            ticket.complete();

            assertOperationError(waiting, IggyErrorCode.OPERATION_NOT_RETAINED);
            assertThat(session.provedOutcome(9)).containsExactly(1);
            inFlight.release();
            ticket.finishWrite();
            assertThat(allocation.refCnt()).isZero();
            assertThat(sameLane).isNotDone();
            otherLane.join().complete();
            sameLane.join().complete();
            acquireMutation(session, channel, partition, 2L * VsrHeaders.HEADER_SIZE)
                    .join()
                    .complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    private static CompletableFuture<ConsensusSession.Ticket> acquireMutation(
            ConsensusSession session, EmbeddedChannel channel, ConsensusSession.LaneKey lane, long bytes) {
        return session.acquire(lane, true, bytes, System.nanoTime() + DEADLINE, channel.eventLoop());
    }

    /** Replays a retained write and keeps the outcome that its replay proved. */
    private static void prove(
            ConsensusSession session,
            EmbeddedChannel channel,
            long requestId,
            ByteBuf reply,
            IggyServerException rejection) {
        var ticket = retainWrite(session, channel, requestId);
        session.resume(requestId, System.nanoTime() + DEADLINE, channel.eventLoop())
                .join()
                .recordOutcome(reply, rejection);
        ticket.complete();
    }

    private static ConsensusSession.Ticket retainWrite(
            ConsensusSession session, EmbeddedChannel channel, long requestId) {
        var ticket = session.acquire(
                        ConsensusSession.LaneKey.METADATA,
                        true,
                        VsrHeaders.HEADER_SIZE,
                        System.nanoTime() + DEADLINE,
                        channel.eventLoop())
                .join();
        freezeAndSend(ticket, requestId);
        ticket.retain();
        return ticket;
    }

    private static void assertOperationError(CompletableFuture<?> future, IggyErrorCode code) {
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(IggyOperationException.class)
                .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                        .isEqualTo(code));
    }

    private static void freezeAndSend(ConsensusSession.Ticket ticket, long requestId) {
        ByteBuf frame = Unpooled.buffer(VsrHeaders.HEADER_SIZE).writeZero(VsrHeaders.HEADER_SIZE);
        frame.setLongLE(VsrHeaders.REQUEST_ID_OFFSET, requestId);
        ticket.freeze(frame, 202, null);
        frame.release();
        ticket.finishWrite();
        ticket.startWrite();
        ticket.finishWrite();
    }
}
