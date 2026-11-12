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
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsensusSessionTest {
    private static final long DEADLINE = TimeUnit.MINUTES.toNanos(1);
    private static final int TIMEOUT_SECONDS = 5;

    /**
     * One link holds its session and waits for the owner, as a reply or a frame on that link does. Another link holds
     * the owner and reads the lineage of a call that started on the first link. Neither waits for the other.
     */
    @Test
    void shouldReadTheLineageOfACallFromAnotherLinkWhileThatLinkWaitsForTheOwner() throws Exception {
        ConsensusSession owner = new ConsensusSession();
        ConsensusSession started = new ConsensusSession();
        started.follow(owner);
        CompletableFuture<Void> linkHeld = new CompletableFuture<>();
        CompletableFuture<Void> ownerHeld = new CompletableFuture<>();
        CompletableFuture<Void> linkDone = new CompletableFuture<>();
        CompletableFuture<Long> lineage = new CompletableFuture<>();
        startDaemon(() -> {
            synchronized (started) {
                linkHeld.complete(null);
                ownerHeld.join();
                synchronized (started.owner()) {
                    linkDone.complete(null);
                }
            }
        });
        linkHeld.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        startDaemon(() -> {
            synchronized (owner) {
                ownerHeld.complete(null);
                lineage.complete(started.lineage());
            }
        });

        assertThat(lineage.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo(owner.lineage());
        linkDone.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void shouldReportAnUnsentSessionWaitAsDisconnectedAfterReset() {
        ConsensusSession session = new ConsensusSession();
        EmbeddedChannel channel = new EmbeddedChannel();
        long deadline = System.nanoTime() + DEADLINE;
        try {
            var ticket = session.acquire(
                            List.of(ConsensusSession.LaneKey.METADATA),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            deadline,
                            channel.eventLoop())
                    .join();
            var waiting = ticket.awaitSession(session.lineage(), deadline, channel.eventLoop());
            assertThat(waiting).isNotDone();

            session.reset();

            assertThatThrownBy(waiting::join).hasCauseInstanceOf(IggyNotConnectedException.class);
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldAdmitLaneWaitersInOrderAfterThreeAdmissionChanges() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var lane = new ConsensusSession.LaneKey(7, 1);
        var first = session.acquire(
                        List.of(lane), true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop())
                .join();
        first.startWrite();
        var second = session.acquire(
                List.of(lane), true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        var third = session.acquire(
                List.of(lane), true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        var fourth = session.acquire(
                List.of(lane), true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        try {
            var independent = session.acquire(
                            List.of(new ConsensusSession.LaneKey(8, 1)),
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

    /**
     * A fetch takes the progress lanes of all its partitions together. It waits behind the requests ahead of it on any
     * of them, and a later store on one of them waits behind the fetch.
     */
    @Test
    void shouldAdmitAFetchAfterTheRequestsAheadOfItOnEachOfItsLanes() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var first = new ConsensusSession.LaneKey(7, 1, true);
        var second = new ConsensusSession.LaneKey(7, 2, true);
        var poll = session.acquire(
                        List.of(second),
                        false,
                        VsrHeaders.HEADER_SIZE,
                        System.nanoTime() + DEADLINE,
                        channel.eventLoop())
                .join();
        var fetch = session.acquire(
                List.of(first, second),
                false,
                VsrHeaders.HEADER_SIZE,
                System.nanoTime() + DEADLINE,
                channel.eventLoop());
        var store = session.acquire(
                List.of(first), true, VsrHeaders.HEADER_SIZE, System.nanoTime() + DEADLINE, channel.eventLoop());
        try {
            assertThat(fetch).isNotDone();
            assertThat(store).isNotDone();

            poll.complete();

            assertThat(fetch).isDone();
            assertThat(store).isNotDone();
            fetch.join().complete();
            assertThat(store).isDone();
            store.join().complete();
        } finally {
            fetch.cancel(false);
            store.cancel(false);
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldNameARetainedPartitionAndReleaseEarlierAdmissionsUntilRepair() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var first = new ConsensusSession.LaneKey(7, 1, true);
        var second = new ConsensusSession.LaneKey(7, 0xffff_ffffL, true);
        long deadline = System.nanoTime() + DEADLINE;
        try {
            var holder = session.acquire(List.of(second), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            long operation = Long.MIN_VALUE;
            freezeAndSend(holder, operation);
            var fetch = session.acquire(
                    List.of(first, second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var earlier = session.acquire(List.of(first), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(fetch).isNotDone();
            assertThat(earlier).isNotDone();

            holder.retain();

            assertThatThrownBy(fetch::join)
                    .hasCauseInstanceOf(IggyOperationException.class)
                    .satisfies(error -> {
                        var blocked = (IggyOperationException) error.getCause();
                        assertThat(blocked.getErrorCode()).isEqualTo(IggyErrorCode.LANE_BLOCKED);
                        assertThat(blocked.getOperationId()).isEqualTo(operation);
                        assertThat(blocked.getPartitionId()).hasValue(second.partition());
                        assertThat(blocked.getReason()).isEqualTo("Operation " + Long.toUnsignedString(operation));
                    });
            assertThat(earlier).isDone();
            earlier.join().complete();
            assertThat(session.resume(operation, deadline, channel.eventLoop()).join())
                    .isSameAs(holder);
            holder.complete();
            var repaired = session.acquire(
                    List.of(first, second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(repaired).isDone();
            repaired.join().complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Two fetches that name the same partitions in opposite orders both get their lanes, one after the other, because
     * every request takes its lanes in ascending partition order.
     */
    @Test
    void shouldAdmitFetchesThatNameTheirPartitionsInOppositeOrders() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var first = new ConsensusSession.LaneKey(7, 1, true);
        var second = new ConsensusSession.LaneKey(7, 2, true);
        var firstHolder = session.acquire(
                        List.of(first),
                        false,
                        VsrHeaders.HEADER_SIZE,
                        System.nanoTime() + DEADLINE,
                        channel.eventLoop())
                .join();
        var secondHolder = session.acquire(
                        List.of(second),
                        false,
                        VsrHeaders.HEADER_SIZE,
                        System.nanoTime() + DEADLINE,
                        channel.eventLoop())
                .join();
        var ascending = session.acquire(
                List.of(first, second),
                false,
                VsrHeaders.HEADER_SIZE,
                System.nanoTime() + DEADLINE,
                channel.eventLoop());
        var descending = session.acquire(
                List.of(second, first),
                false,
                VsrHeaders.HEADER_SIZE,
                System.nanoTime() + DEADLINE,
                channel.eventLoop());
        try {
            firstHolder.complete();
            secondHolder.complete();

            assertThat(ascending).isDone();
            assertThat(descending).isNotDone();
            ascending.join().complete();
            assertThat(descending).isDone();
            descending.join().complete();
        } finally {
            ascending.cancel(false);
            descending.cancel(false);
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
                            List.of(lane),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join();
            freezeAndSend(ticket, 9);
            var uncertain = (IggyOperationException) ticket.retain();
            assertThat(uncertain.getOperationId()).isEqualTo(9);
            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
            assertThat(uncertain.getPartitionId()).isEmpty();
            assertThatThrownBy(() -> session.acquire(
                                    List.of(lane),
                                    true,
                                    VsrHeaders.HEADER_SIZE,
                                    System.nanoTime() + DEADLINE,
                                    channel.eventLoop())
                            .join())
                    .hasCauseInstanceOf(IggyOperationException.class)
                    .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                            .isEqualTo(IggyErrorCode.LANE_BLOCKED));
            var independent = session.acquire(
                            List.of(new ConsensusSession.LaneKey(8, 1)),
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
                            List.of(lane),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
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
                            List.of(ConsensusSession.LaneKey.METADATA),
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
                    List.of(ConsensusSession.LaneKey.METADATA),
                    true,
                    VsrHeaders.HEADER_SIZE,
                    System.nanoTime() + DEADLINE,
                    channel.eventLoop());
            CompletableFuture<ConsensusSession.Ticket> queued = session.acquire(
                    List.of(ConsensusSession.LaneKey.METADATA),
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
                            List.of(lane),
                            false,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join();
            freezeAndSend(poll, 9);
            assertThat(((org.apache.iggy.exception.IggyServerException) poll.retain()).getRawErrorCode())
                    .isEqualTo(57);
            assertOperationError(
                    session.resume(9, System.nanoTime() + DEADLINE, channel.eventLoop()),
                    IggyErrorCode.OPERATION_NOT_RETAINED);
            session.acquire(
                            List.of(lane),
                            true,
                            VsrHeaders.HEADER_SIZE,
                            System.nanoTime() + DEADLINE,
                            channel.eventLoop())
                    .join()
                    .complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldAdmitProducersWhileAnOffsetStoreWaitsForProgressAndRemoveBothCancelledAdmissions() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var mutation = new ConsensusSession.LaneKey(7, 1);
        var progress = new ConsensusSession.LaneKey(7, 1, true);
        long deadline = System.nanoTime() + DEADLINE;
        try {
            var poll = session.acquire(List.of(progress), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            var store = session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(store).isNotDone();
            var producer =
                    session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(producer).isDone();

            poll.complete();
            var laterProducer =
                    session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var offsetRead =
                    session.acquire(List.of(progress), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(store).isNotDone();
            assertThat(laterProducer).isNotDone();
            assertThat(offsetRead).isNotDone();
            producer.join().complete();
            assertThat(store).isDone();
            assertThat(laterProducer).isNotDone();
            assertThat(offsetRead).isNotDone();
            store.join().complete();
            laterProducer.join().complete();
            offsetRead.join().complete();

            var blockedMutation = session.acquire(
                            List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            var cancelledStore =
                    session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var queuedRead =
                    session.acquire(List.of(progress), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var queuedProducer =
                    session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(queuedRead).isNotDone();
            assertThat(queuedProducer).isNotDone();
            cancelledStore.cancel(false);
            assertThat(queuedRead).isDone();
            queuedRead.join().complete();
            blockedMutation.complete();
            queuedProducer.join().complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldKeepBothOffsetLanesUntilAnAbandonedReplayEnds() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, 2L * VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var mutation = new ConsensusSession.LaneKey(7, 1);
        var progress = new ConsensusSession.LaneKey(7, 1, true);
        long deadline = System.nanoTime() + DEADLINE;
        try {
            var store = session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            freezeAndSend(store, 9);
            store.retain();
            for (var lane : new ConsensusSession.LaneKey[] {mutation, progress}) {
                assertOperationError(
                        session.acquire(
                                List.of(lane), lane == mutation, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop()),
                        IggyErrorCode.LANE_BLOCKED);
            }
            assertThat(session.resume(9, deadline, channel.eventLoop()).join()).isSameAs(store);
            ByteBuf alias = store.frame();
            session.abandon(9);
            var poll = session.acquire(List.of(progress), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var send = session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(poll).isNotDone();
            assertThat(send).isNotDone();
            var independent = session.acquire(
                    List.of(new ConsensusSession.LaneKey(8, 1)),
                    true,
                    VsrHeaders.HEADER_SIZE,
                    deadline,
                    channel.eventLoop());
            assertThat(independent).isDone();
            independent.join().complete();
            store.complete();
            poll.join().complete();
            send.join().complete();
            assertThat(alias.refCnt()).isPositive();
            alias.release();
            store.finishWrite();
            session.acquire(List.of(mutation), true, 2L * VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join()
                    .complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldAllowProgressPastARetainedSendAndReleaseBothOffsetLanesOnReset() {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var mutation = new ConsensusSession.LaneKey(7, 1);
        var progress = new ConsensusSession.LaneKey(7, 1, true);
        long deadline = System.nanoTime() + DEADLINE;
        try {
            var send = session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            freezeAndSend(send, 9);
            send.retain();
            var poll = session.acquire(List.of(progress), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            assertThat(poll).isDone();
            poll.join().complete();
            assertOperationError(
                    session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop()),
                    IggyErrorCode.LANE_BLOCKED);
            session.abandon(9);
            var store = session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            freezeAndSend(store, 10);
            store.retain();
            session.reset();
            session.bind(43);
            session.acquire(List.of(progress), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join()
                    .complete();
            session.acquire(List.of(mutation), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
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
        return session.acquire(List.of(lane), true, bytes, System.nanoTime() + DEADLINE, channel.eventLoop());
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
                        List.of(ConsensusSession.LaneKey.METADATA),
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

    @Test
    void shouldKeepEveryUnknownFetchTargetAndItsBudgetAcrossSessionReset() {
        ConsensusSession session = new ConsensusSession();
        session.configurePendingOperations(1, VsrHeaders.HEADER_SIZE);
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var first = new ConsensusSession.LaneKey(7, 1, true);
        var second = new ConsensusSession.LaneKey(7, 0xffff_ffffL, true);
        var independent = new ConsensusSession.LaneKey(7, 3, true);
        long deadline = System.nanoTime() + DEADLINE;
        long operation = Long.MIN_VALUE;
        try {
            var ticket = session.acquire(
                            List.of(first, second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            ticket.reserveFetch();
            freezeAndSend(ticket, operation);
            var waitingFirst =
                    session.acquire(List.of(first), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());
            var waitingSecond =
                    session.acquire(List.of(second), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop());

            session.reset();
            session.bind(43);

            assertOperationError(waitingFirst, IggyErrorCode.PARTITION_LANE_BLOCKED);
            assertOperationError(waitingSecond, IggyErrorCode.PARTITION_LANE_BLOCKED);
            assertThat(session.unresolvedFetch().getOperationId()).isEqualTo(operation);
            assertOperationError(
                    session.resume(operation, deadline, channel.eventLoop()), IggyErrorCode.FETCH_PROGRESS_UNCERTAIN);
            session.acquire(List.of(second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop(), true)
                    .join()
                    .complete();
            session.acquire(
                            List.of(new ConsensusSession.LaneKey(8, 1, true)),
                            false,
                            VsrHeaders.HEADER_SIZE,
                            deadline,
                            channel.eventLoop())
                    .join()
                    .complete();
            var fresh = session.acquire(
                            List.of(independent), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            assertThatThrownBy(fresh::reserveFetch)
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            error -> assertThat(error.getErrorCode()).isEqualTo(IggyErrorCode.TRANSIENT_NOT_ACCEPTED));
            session.abandon(operation);
            fresh.reserveFetch();
            fresh.complete();
            assertThat(session.unresolvedFetch()).isNull();
            var next = session.acquire(
                            List.of(first, second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            next.reserveFetch();
            next.complete();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldKeepOnlyUncertainSectionsWhenExplicitSessionIsolationEndsAClassifiedFetch(boolean partial) {
        ConsensusSession session = new ConsensusSession();
        session.bind(42);
        EmbeddedChannel channel = new EmbeddedChannel();
        var lane = new ConsensusSession.LaneKey(7, 1, true);
        var second = new ConsensusSession.LaneKey(7, 2, true);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        try {
            var ticket = session.acquire(
                            List.of(lane, second), false, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join();
            ticket.reserveFetch();
            freezeAndSend(ticket, 9);
            ticket.settleFetch(partial ? List.of(2L) : List.of());

            assertThat(ticket.retain())
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            error -> assertThat(error.getErrorCode()).isEqualTo(IggyErrorCode.TRANSIENT_NOT_COMMITTED));
            session.reset();
            session.bind(43);

            session.acquire(List.of(lane), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop())
                    .join()
                    .complete();
            if (partial) {
                assertOperationError(
                        session.acquire(List.of(second), true, VsrHeaders.HEADER_SIZE, deadline, channel.eventLoop()),
                        IggyErrorCode.PARTITION_LANE_BLOCKED);
                assertOperationError(
                        session.resume(9, deadline, channel.eventLoop()), IggyErrorCode.FETCH_PROGRESS_UNCERTAIN);
                session.abandon(9);
            }
            assertThat(session.unresolvedFetch()).isNull();
        } finally {
            session.reset();
            channel.finishAndReleaseAll();
        }
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

    private static void startDaemon(Runnable task) {
        Thread thread = new Thread(task);
        // A deadlocked thread must not keep the test JVM alive.
        thread.setDaemon(true);
        thread.start();
    }
}
