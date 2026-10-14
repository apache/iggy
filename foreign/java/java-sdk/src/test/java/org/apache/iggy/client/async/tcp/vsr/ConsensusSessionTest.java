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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsensusSessionTest {
    private static final long DEADLINE = TimeUnit.MINUTES.toNanos(1);

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
            assertThat(session.resume(9)).isSameAs(ticket);
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
            CompletableFuture<ConsensusSession.Ticket> queued = session.acquire(
                    ConsensusSession.LaneKey.METADATA,
                    true,
                    VsrHeaders.HEADER_SIZE,
                    System.nanoTime() + DEADLINE,
                    channel.eventLoop());
            assertThat(queued).isNotDone();
            assertThat(session.currentRequestId()).isEqualTo(before);
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
            assertThatThrownBy(() -> session.resume(9))
                    .isInstanceOf(IggyOperationException.class)
                    .satisfies(error -> assertThat(((IggyOperationException) error).getErrorCode())
                            .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
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
