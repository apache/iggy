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

package org.apache.iggy.client.async.reader;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.reader.FakeTransport.Read;
import org.apache.iggy.client.async.reader.FakeTransport.Store;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.refused;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reader against a transport whose reads the test answers. */
class PartitionReaderTest {
    private static final Duration QUIET = Duration.ofMillis(300);
    private static final Duration RETRY_INTERVAL = Duration.ofMillis(100);
    private static final PollOptions MANUAL = PollOptions.waitFor(Duration.ofSeconds(1));
    private static final PollOptions ON_READ = MANUAL.withCommit(CommitPolicy.onRead());

    @Test
    void shouldReadEachGroupWithOneRequestAndHandOverBatchesInArrivalOrder() throws Exception {
        FakeTransport transport =
                new FakeTransport(false).groupedBy(partitions -> List.of(List.of(1L, 3L), List.of(2L)));
        PartitionReader reader = standalone(transport, MANUAL, 1L, 2L, 3L);
        try {
            Read first = transport.nextRead();
            Read second = transport.nextRead();
            assertThat(List.of(first.targets().keySet(), second.targets().keySet()))
                    .containsExactlyInAnyOrder(Set.of(1L, 3L), Set.of(2L));
            Read groupOfTwo = first.targets().containsKey(2L) ? first : second;
            Read groupOfOneAndThree = groupOfTwo == first ? second : first;

            groupOfTwo.answer(section(2, 5, 6));
            groupOfOneAndThree.answer(section(3, 0), section(1, 9));

            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
            assertThat(offsets(next(reader))).containsExactly(0L);
            assertThat(offsets(next(reader))).containsExactly(9L);
            Read again = transport.nextRead();
            assertThat(again.targets().get(2L))
                    .as("a partition continues after its last message, bound to it")
                    .isEqualTo(new PollingStrategy(
                            PollingKind.Offset,
                            BigInteger.valueOf(7),
                            Optional.of(FakeTransport.CONTEXT),
                            Optional.of(2L)));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldGroupThePartitionsAgainAfterAPrimaryRefusesARead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L, 2L);
        try {
            transport.nextRead().reply().completeExceptionally(serverError(IggyErrorCode.NOT_PRIMARY));
            transport.nextRead().answer(section(1, 0));

            assertThat(offsets(next(reader))).containsExactly(0L);
            assertThat(transport.groupings).hasSize(2);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Under OnRead a read stores its offset, so the reader reads only for a waiting call. After a lost reply, the next
     * read starts after what the lost read stored, and the reader reads those messages again without a store.
     */
    @Test
    void shouldReadOnDemandUnderOnReadAndReadAgainWhatALostReplyStored() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            CompletableFuture<PolledMessages> first = reader.next();
            Read read = transport.nextRead();
            assertThat(read.commit()).isTrue();
            read.answer(section(1, 0, 1));
            assertThat(offsets(first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            CompletableFuture<PolledMessages> second = reader.next();
            transport
                    .nextRead()
                    .reply()
                    .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            Read afterLoss = transport.nextRead();
            assertThat(afterLoss.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            assertThat(transport.abandoned).containsExactly(42L);
            afterLoss.answer(section(1, 5, 6));
            Read gap = transport.nextRead();
            assertThat(gap.commit()).isFalse();
            assertThat(gap.targets().get(1L).kind()).isEqualTo(PollingKind.Offset);
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.TWO);
            assertThat(gap.count()).isEqualTo(3);
            gap.answer(section(1, 2, 3, 4, 5));

            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L, 4L);
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldStoreACommittedCheckpointUnderManualAndRefuseOneUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader manual = standalone(transport, MANUAL, 1L);
        try {
            transport.nextRead().answer(section(1, 3, 4));
            PolledMessages batch = next(manual);

            manual.commit(batch.checkpointAt(0).orElseThrow()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            manual.commit(batch.checkpointAt(0).orElseThrow()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(transport.stores).containsExactly(new Store(1, 3));
        } finally {
            manual.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        FakeTransport onReadTransport = new FakeTransport(false);
        onReadTransport.starts.put(1L, 3L);
        PartitionReader onRead = standalone(onReadTransport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> polled = onRead.next();
            onReadTransport.nextRead().answer(section(1, 3));
            var checkpoint = polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .checkpoint()
                    .orElseThrow();

            assertInvalidCommand(onRead.commit(checkpoint));
            assertInvalidCommand(onRead.mark(checkpoint));
        } finally {
            onRead.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldCountABatchOnceTheNextIsAskedForAndStoreTheCountedPositionOnClose() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        AutoCommit everyThousand =
                new AutoCommit(Optional.empty(), Optional.of(CommitEvery.messages(1000)), CommitAfter.PROCESSING);
        PartitionReader reader = standalone(transport, MANUAL.withCommit(CommitPolicy.auto(everyThousand)), 1L);
        transport.nextRead().answer(section(1, 0, 1));
        next(reader);
        transport.nextRead().answer(section(1, 2));
        next(reader);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores)
                .as("the last batch counts at the close, after the first counted at the second call")
                .containsExactly(new Store(1, 2));
    }

    @Test
    void shouldStoreEachBatchAtDeliveryUnderAutoCommitEveryBatch() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        AutoCommit eachBatch = new AutoCommit(Optional.empty(), Optional.of(CommitEvery.batch()), CommitAfter.DELIVERY);
        PartitionReader reader = standalone(transport, MANUAL.withCommit(CommitPolicy.auto(eachBatch)), 1L);
        try {
            transport.nextRead().answer(section(1, 0, 1));
            next(reader);

            awaitStores(transport, 1);
            assertThat(transport.stores).containsExactly(new Store(1, 1));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldCountOnlyMarkedCheckpointsUnderAutoCommitAfterMarks() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        AutoCommit marked = new AutoCommit(Optional.empty(), Optional.of(CommitEvery.message()), CommitAfter.MARKED);
        PartitionReader reader = standalone(transport, MANUAL.withCommit(CommitPolicy.auto(marked)), 1L);
        try {
            transport.nextRead().answer(section(1, 0, 1, 2));
            PolledMessages batch = next(reader);
            assertThat(transport.stores).isEmpty();

            reader.mark(batch.checkpointAt(1).orElseThrow()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            awaitStores(transport, 1);
            assertThat(transport.stores).containsExactly(new Store(1, 1));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** Under OnRead the server stored what the reader holds undelivered, so the close moves the offset back. */
    @Test
    void shouldMoveTheOffsetOfUndeliveredBatchesBackOnCloseUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false).groupedBy(partitions -> List.of(List.of(1L), List.of(2L)));
        transport.starts.put(2L, 4L);
        PartitionReader reader = standalone(transport, ON_READ, 1L, 2L);
        CompletableFuture<PolledMessages> polled = reader.next();
        Read first = transport.nextRead();
        Read second = transport.nextRead();
        Read partitionOne = first.targets().containsKey(1L) ? first : second;
        Read partitionTwo = partitionOne == first ? second : first;
        partitionOne.answer(section(1, 0, 1));
        assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                .containsExactly(0L, 1L);
        partitionTwo.answer(section(2, 4, 5));
        assertThat(transport.readsNothingFor(QUIET)).isTrue();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores)
                .as("partition 2 started at its next offset and delivered nothing")
                .containsExactly(new Store(2, 3));
    }

    @Test
    void shouldStopAPartitionWhoseHistoryIsGoneUntilASeek() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            transport.nextRead().answer(section(1, 0));
            next(reader);
            transport.nextRead().answer(refused(1, IggyErrorCode.HISTORY_UNAVAILABLE.getCode()));

            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(IggyErrorCode.HISTORY_UNAVAILABLE.getCode()));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();

            reader.seek(1L, PollingStrategy.first()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Read afterSeek = transport.nextRead();
            assertThat(afterSeek.targets().get(1L).value()).isZero();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldHandOverTheMessagesBeforeOneThatDoesNotDecodeAndNameTheContinuationAfterIt() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            byte[] records = ByteBufUtil.getBytes(
                    Unpooled.wrappedBuffer(FakeTransport.record(4), Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));
            transport
                    .nextRead()
                    .answer(new FetchedMessages(
                            1,
                            FetchedMessages.STATUS_OK,
                            BigInteger.valueOf(6),
                            2,
                            FakeTransport.CONTEXT,
                            BigInteger.valueOf(6),
                            records));

            assertThat(offsets(next(reader))).containsExactly(4L);
            ReadException error = readError(reader);
            assertThat(error.skip()).map(PollingStrategy::value).contains(BigInteger.valueOf(6));
            assertThat(error.offset()).isEmpty();
            assertThat(error.record()).containsExactly(1, 2, 3);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldCarryAMalformedSingleRecordReplyAsReceivedWithoutAnOffset() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            // A batch header whose length runs past the reply.
            byte[] records = ByteBufUtil.getBytes(FakeTransport.record(4).setLongLE(32, 1_000));
            transport
                    .nextRead()
                    .answer(new FetchedMessages(
                            1,
                            FetchedMessages.STATUS_OK,
                            BigInteger.valueOf(5),
                            1,
                            FakeTransport.CONTEXT,
                            BigInteger.valueOf(5),
                            records));

            ReadException error = readError(reader);
            assertThat(error.offset()).isEmpty();
            assertThat(error.record()).isEqualTo(records);
            assertThat(error.skip()).map(PollingStrategy::value).contains(BigInteger.valueOf(5));
            assertThat(error.skip()).map(PollingStrategy::partitionId).contains(Optional.of(1L));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldHandOverTheHealthyFramesOfABatchRecordBeforeOneWithANonzeroReservedField() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            byte[] bad = FakeTransport.frame(1, "message 5", 1);
            byte[] rest =
                    ByteBufUtil.getBytes(Unpooled.wrappedBuffer(bad, ByteBufUtil.getBytes(FakeTransport.record(6))));
            byte[] records = ByteBufUtil.getBytes(Unpooled.wrappedBuffer(
                    FakeTransport.batch(4, FakeTransport.frame(0, "message 4", 0), bad), FakeTransport.record(6)));
            transport
                    .nextRead()
                    .answer(new FetchedMessages(
                            1,
                            FetchedMessages.STATUS_OK,
                            BigInteger.valueOf(7),
                            3,
                            FakeTransport.CONTEXT,
                            BigInteger.valueOf(7),
                            records));

            assertThat(offsets(next(reader))).containsExactly(4L);
            ReadException error = readError(reader);
            // The frame is refused by its header, so no offset is known and the record runs to the section end.
            assertThat(error.offset()).isEmpty();
            assertThat(error.record()).isEqualTo(rest);
            assertThat(error.skip()).map(PollingStrategy::value).contains(BigInteger.valueOf(6));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();

            reader.seek(1L, error.skip().orElseThrow()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.nextRead().targets().get(1L).value()).isEqualTo(BigInteger.valueOf(6));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldRefuseAContinuationOfAnotherPartitionAndAPartitionItDoesNotRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L, 2L);
        try {
            transport.nextRead().answer(section(1, 0));
            PolledMessages batch = next(reader);

            assertThatThrownBy(() -> reader.seek(2L, batch.continuation().orElseThrow()))
                    .isInstanceOf(IggyValidationException.class);
            assertInvalidCommand(reader.seek(3L, PollingStrategy.first()));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A rebalance takes a partition away only after its counted position is stored. */
    @Test
    void shouldFollowTheAssignmentOfItsMemberAndStoreTheCountedPositionOfAPartitionThatLeaves() throws Exception {
        FakeTransport transport = new FakeTransport(true)
                .groupedBy(partitions -> partitions.stream().map(List::of).toList());
        AutoCommit atDelivery =
                new AutoCommit(Optional.empty(), Optional.of(CommitEvery.messages(1000)), CommitAfter.DELIVERY);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(MANUAL.withCommit(CommitPolicy.auto(atDelivery)))
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L, 2L);
            Read first = transport.nextRead();
            Read second = transport.nextRead();
            Read partitionOne = first.targets().containsKey(1L) ? first : second;
            partitionOne.answer(section(1, 7));
            next(reader);
            assertThat(reader.partitions()).containsExactly(1L, 2L);

            assignment(transport, 2L);

            awaitStores(transport, 1);
            assertThat(transport.stores).containsExactly(new Store(1, 7));
            assertThat(reader.partitions()).containsExactly(2L);
        } finally {
            closeGroupReader(reader, transport);
        }
        assertThat(transport.membership).containsExactly("join", "leave");
    }

    /** A read that reaches its count serves its first targets, so the partitions served last go last in the next read. */
    @Test
    void shouldPutThePartitionsServedLastAtTheEndOfTheNextRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L, 3L, 5L);
        try {
            List<Long> delivered = new ArrayList<>();
            for (int turn = 0; turn < 3; turn++) {
                Read read = transport.nextRead();
                Map.Entry<Long, PollingStrategy> first =
                        read.targets().entrySet().iterator().next();
                read.answer(section(first.getKey(), first.getValue().value().longValue()));
                delivered.add(next(reader).partitionId());
            }

            assertThat(delivered).containsExactly(1L, 3L, 5L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** Under OnRead the server stored the batches that the reader holds, so a partition that leaves hands them over. */
    @Test
    void shouldHandOverAQueuedBatchOfAPartitionThatLeavesUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            transport.nextRead().answer(section(1, 0, 1), section(2, 4, 5));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            CompletableFuture<PolledMessages> stored = reader.next();

            assertThat(transport.readsNothingFor(QUIET))
                    .as("the queued batch of the partition that left is handed over without a read")
                    .isTrue();
            PolledMessages batch = stored.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(batch.partitionId()).isEqualTo(2L);
            assertThat(offsets(batch)).containsExactly(4L, 5L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /** Under OnRead the server stored a read in flight, so its reply reaches the application after its partition left. */
    @Test
    void shouldHandOverAReadThatEndsAfterItsPartitionLeftUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true)
                .groupedBy(partitions -> partitions.stream().map(List::of).toList());
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            Read first = transport.nextRead();
            Read second = transport.nextRead();
            Read partitionOne = first.targets().containsKey(1L) ? first : second;
            Read partitionTwo = partitionOne == first ? second : first;
            partitionOne.answer(section(1, 0, 1));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            partitionTwo.answer(section(2, 4, 5));
            CompletableFuture<PolledMessages> stored = reader.next();

            assertThat(transport.readsNothingFor(QUIET))
                    .as("the reply of the partition that left is handed over without a read")
                    .isTrue();
            PolledMessages batch = stored.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(batch.partitionId()).isEqualTo(2L);
            assertThat(offsets(batch)).containsExactly(4L, 5L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * Under OnRead a partition that leaves while it reads again what a lost read stored reports those messages as lost
     * and still hands over the batch that the server stored after them.
     */
    @Test
    void shouldHandOverTheBatchAfterALostReadWhenItsPartitionLeavesUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<PolledMessages> second = reader.next();
            transport
                    .nextRead()
                    .reply()
                    .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            transport.nextRead().answer(section(1, 5, 6));
            assertThat(transport.nextRead().commit()).isFalse();

            assignment(transport, 2L);

            assertThatThrownBy(() -> second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode()));
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /** A failed grouping waits the retry interval, however often the application asks for a batch. */
    @Test
    void shouldWaitTheRetryIntervalBeforeItGroupsAgainAfterAFailedGrouping() throws Exception {
        List<Long> attempts = new CopyOnWriteArrayList<>();
        FakeTransport transport = new FakeTransport(false).groupedBy(partitions -> {
            attempts.add(System.nanoTime());
            if (attempts.size() <= 2) {
                throw new IggyClientException("No route to the partition yet");
            }
            return List.of(partitions);
        });
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            reader.next();
            reader.next();

            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (attempts.size() < 3 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(attempts).hasSizeGreaterThanOrEqualTo(3);
            assertThat(attempts.get(1) - attempts.get(0)).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            assertThat(attempts.get(2) - attempts.get(1)).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            transport.nextRead().answer(section(1, 0));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldHandOverAQueuedBatchOfAPartitionAssignedAgainUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            transport.nextRead().answer(section(1, 0, 1), section(2, 4, 5));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            assignment(transport, 1L, 2L);
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> stored = reader.next();

            assertThat(transport.readsNothingFor(QUIET))
                    .as("the queued batch of the partition that left is handed over without a read")
                    .isTrue();
            PolledMessages batch = stored.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(batch.partitionId()).isEqualTo(2L);
            assertThat(offsets(batch)).containsExactly(4L, 5L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    @Test
    void shouldHandOverAReadThatEndsAfterItsPartitionIsAssignedAgainUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true)
                .groupedBy(partitions -> partitions.stream().map(List::of).toList());
        transport.starts.put(2L, 4L);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            CompletableFuture<PolledMessages> polled = reader.next();
            Read first = transport.nextRead();
            Read second = transport.nextRead();
            Read partitionOne = first.targets().containsKey(1L) ? first : second;
            Read partitionTwo = partitionOne == first ? second : first;
            partitionOne.answer(section(1, 0, 1));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(0L, 1L);

            assignment(transport, 1L);
            assignment(transport, 1L, 2L);
            assignment(transport, 1L, 2L);
            partitionTwo.answer(section(2, 4, 5));
            CompletableFuture<PolledMessages> stored = reader.next();

            assertThat(transport.readsNothingFor(QUIET))
                    .as("the reply of the partition that left is handed over without a read")
                    .isTrue();
            PolledMessages batch = stored.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(batch.partitionId()).isEqualTo(2L);
            assertThat(offsets(batch)).containsExactly(4L, 5L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    @Test
    void shouldWaitTheRetryIntervalAfterASectionlessReplyWithZeroWait() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, PollOptions.defaults(), 1L);
        try {
            Read read = transport.nextRead();
            long started = System.nanoTime();
            read.answer();
            transport.nextRead();
            assertThat(System.nanoTime() - started).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldDropAnOldStoredReplyAfterASeekAndReassignment() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read old = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            assignment(transport, 2L);
            assignment(transport, 1L);
            assignment(transport, 1L);
            reader.seek(1L, PollingStrategy.offset(BigInteger.TEN)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.starts.put(1L, 10L);
            assignment(transport, 2L);
            assignment(transport, 1L);
            assignment(transport, 1L);
            old.answer(section(1, 0, 1));
            CompletableFuture<PolledMessages> resumed = reader.next();
            transport.nextRead().answer(section(1, 10));
            assertThat(offsets(resumed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(10L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    @Test
    void shouldStopReadingUnderOnReadWhenTheWaitingCallIsCancelled() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read read = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            read.answer();
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 5})
    void shouldRecoverTheFirstLostOnReadReply(long start) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.starts.put(1L, start);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            Read first = transport.nextRead();
            assertThat(first.commit()).isTrue();
            first.reply().completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            transport.nextRead().answer(section(1, start + 2));
            Read gap = transport.nextRead();
            assertThat(gap.commit()).isFalse();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(start));
            gap.answer(section(1, start, start + 1));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(start, start + 1);
            assertThat(offsets(next(reader))).containsExactly(start + 2);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 5})
    void shouldRewindTheFirstUndeliveredOnReadToItsBaseline(long start) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.starts.put(1L, start);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();
        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (start == 0) {
            assertThat(transport.deleted).containsExactly(1L);
        } else {
            assertThat(transport.stores).containsExactly(new Store(1, start - 1));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 5})
    void shouldRecoverTheFirstLostOnReadReplyWhenItConsumedTheWholeBacklog(long start) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.starts.put(1L, start);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            transport
                    .nextRead()
                    .reply()
                    .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            transport
                    .nextRead()
                    .answer(new FetchedMessages(
                            1,
                            FetchedMessages.STATUS_OK,
                            BigInteger.valueOf(start + 2),
                            0,
                            FakeTransport.CONTEXT,
                            BigInteger.valueOf(start + 2),
                            new byte[0]));
            Read gap = transport.nextRead();
            assertThat(gap.commit()).isFalse();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(start));
            gap.answer(section(1, start, start + 1));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(start, start + 1);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A gap read that the server refused goes out again, so the messages that the lost read stored still arrive. */
    @Test
    void shouldReadTheGapAgainAfterTheServerRefusedItsRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 5, 6));
            Read refusedGap = transport.nextRead();
            assertThat(refusedGap.commit()).isFalse();
            refusedGap.reply().completeExceptionally(serverError(IggyErrorCode.NOT_PRIMARY));

            Read gap = transport.nextRead();
            assertThat(gap.commit()).isFalse();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.TWO);
            gap.answer(section(1, 2, 3, 4));
            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L, 4L);
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** An empty gap section moves the gap to its next offset, past messages that are gone. */
    @Test
    void shouldMoveTheGapPastAnEmptyGapSectionAndReadTheRest() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 5, 6));
            transport.nextRead().answer(FakeTransport.empty(1, 3));

            Read rest = transport.nextRead();
            assertThat(rest.commit()).isFalse();
            assertThat(rest.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(3));
            rest.answer(section(1, 3, 4));
            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(3L, 4L);
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A partition that another owner holds drops its position, whose owner generation is stale after a rejoin, and
     * resolves its start again. Refusals that last are reported, and the partition keeps reading.
     */
    @Test
    void shouldResolveThePositionAgainAfterTheServerSaysAnotherOwnerHoldsThePartition() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            assignment(transport, 1L);
            transport.nextRead().answer(section(1, 0, 1));
            next(reader);
            Read refusedRead = transport.nextRead();
            assertThat(refusedRead.targets().get(1L).context()).contains(FakeTransport.CONTEXT);
            int notOwned = IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode();
            refusedRead.answer(refused(1, notOwned));

            Read again = transport.nextRead();
            assertThat(again.targets().get(1L)).isEqualTo(PollingStrategy.next());
            again.answer(refused(1, notOwned));
            for (int refusal = 2; refusal < 6; refusal++) {
                transport.nextRead().answer(refused(1, notOwned));
            }

            assertThat(readError(reader).errorCode()).hasValue(notOwned);
            assertThat(transport.nextRead().targets().get(1L)).isEqualTo(PollingStrategy.next());
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    @Test
    void shouldStoreACommitAgainAfterItsFirstStoreFailed() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        try {
            transport.nextRead().answer(section(1, 3, 4));
            Checkpoint last = next(reader).checkpoint().orElseThrow();
            transport.storeFailures.add(new IggyClientException("The connection dropped"));

            assertThatThrownBy(() -> reader.commit(last).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
            reader.commit(last).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(transport.stores).containsExactly(new Store(1, 4));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** The server raises the metadata op of a section after any metadata change, in the same history. */
    @Test
    void shouldCommitAnEarlierCheckpointAfterTheServerRaisedTheMetadataOp() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L);
        PartitionContext raised = new PartitionContext(
                FakeTransport.CONTEXT.incarnation(), FakeTransport.CONTEXT.ownerGeneration(), BigInteger.TWO);
        try {
            transport.nextRead().answer(section(1, 0, 1));
            PolledMessages first = next(reader);
            transport.nextRead().answer(section(1, raised, 2, 3));
            next(reader);

            reader.commit(first.checkpointAt(0).orElseThrow()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(transport.stores).containsExactly(new Store(1, 0));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** The server can have stored a read whose reply was lost, so the close moves the offset back over it. */
    @Test
    void shouldMoveTheOffsetBackOverALostReadOnCloseUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(1, 0, 1));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read lost = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        lost.answer(FakeTransport.lost(1, 42));
        assertThat(transport.readsNothingFor(QUIET)).isTrue();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 1));
    }

    /** The server stored the whole read, so the close moves the offset back before the message that did not decode. */
    @Test
    void shouldMoveTheOffsetBackBeforeAMessageThatDidNotDecodeOnCloseUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> first = reader.next();
        byte[] records = ByteBufUtil.getBytes(
                Unpooled.wrappedBuffer(FakeTransport.record(0), Unpooled.wrappedBuffer(new byte[] {1, 2, 3})));
        transport
                .nextRead()
                .answer(new FetchedMessages(
                        1,
                        FetchedMessages.STATUS_OK,
                        BigInteger.valueOf(3),
                        3,
                        FakeTransport.CONTEXT,
                        BigInteger.valueOf(3),
                        records));
        assertThat(offsets(first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                .containsExactly(0L);
        readError(reader);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 0));
    }

    /** A lost read is abandoned also when a seek replaced its generation, or its lane stays blocked. */
    @Test
    void shouldAbandonALostReadWhoseReplyArrivesAfterASeek() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            reader.next();
            Read lost = transport.nextRead();
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));

            lost.answer(FakeTransport.lost(1, 42));
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(transport.abandoned).containsExactly(42L);
            assertThat(transport.stores).containsExactly(new Store(1, 9));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A seek that waits for a read in flight when the close starts still applies, and its store goes before the rewind. */
    @Test
    void shouldStoreAPendingSeekOnCloseBeforeTheRewind() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L, 2L);
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(2, 0));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        reader.next();
        transport.nextRead();
        CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.stores).containsExactly(new Store(1, 9), new Store(2, 0));
    }

    /** A refused store of a seek that applies at close fails the close, and the other partitions still rewind. */
    @Test
    void shouldFailTheCloseWhenTheStoreOfAPendingSeekIsRefusedAndStillRewindTheOthers() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L, 2L);
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(2, 0));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        reader.next();
        transport.nextRead();
        CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));
        transport.storeFailures.add(new IggyClientException("The connection dropped"));

        assertThatThrownBy(() -> reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);

        assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
        assertThat(transport.stores).containsExactly(new Store(2, 0));
    }

    /**
     * The server can store until it answers the cancel of a committing read, and that reply lifts the block of its
     * partitions, so the stores of the close wait for it.
     */
    @Test
    void shouldStoreOnCloseOnlyAfterTheCancelledReadsAnswered() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(1, 0));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        reader.next();
        transport.nextRead();
        transport.settled = new CompletableFuture<>();

        CompletableFuture<Void> closed = reader.closeAsync();

        assertThat(transport.readsNothingFor(QUIET)).isTrue();
        assertThat(closed).isNotDone();
        assertThat(transport.stores).isEmpty();
        transport.settled.complete(null);
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.stores).containsExactly(new Store(1, 0));
    }

    /**
     * A committing read whose reply never comes during the close leaves its partition blocked on the client, so the
     * close abandons it, before its rewind store of that partition, since no reader is left to recover it.
     */
    @Test
    void shouldAbandonAReadLostDuringTheCloseBeforeTheRewind() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(1, 0));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        reader.next();
        transport.nextRead();
        transport.lost.add(42L);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly("abandon 42", "store 1 0");
    }

    /** The offset before a seek is the last position of the new generation, so the close moves back to it. */
    @Test
    void shouldMoveTheOffsetBackBeforeTheSeekOffsetOnCloseWhileAReadIsInFlight() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.seek(1L, PollingStrategy.offset(BigInteger.TEN)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        reader.next();
        assertThat(transport.nextRead().targets().get(1L).kind()).isEqualTo(PollingKind.Next);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 9), new Store(1, 9));
    }

    /** There is no offset before 0, and the other strategies name no offset, so such a seek stores nothing. */
    @ParameterizedTest
    @MethodSource("startsThatStoreNothing")
    void shouldStoreNothingForASeekThatNamesNoOffsetAboveZeroUnderOnRead(PollingStrategy start) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            reader.seek(1L, start).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            reader.next();

            Read read = transport.nextRead();
            PollingKind kind = start.kind() == PollingKind.First ? PollingKind.Offset : start.kind();
            assertThat(read.targets().get(1L).kind()).isEqualTo(kind);
            assertThat(read.commit()).isTrue();
            assertThat(transport.stores).isEmpty();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<PollingStrategy> startsThatStoreNothing() {
        return List.of(
                PollingStrategy.first(),
                PollingStrategy.offset(BigInteger.ZERO),
                PollingStrategy.last(),
                PollingStrategy.next(),
                PollingStrategy.timestamp(BigInteger.TEN));
    }

    @Test
    void shouldFailASeekWhoseStoreIsRefusedUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            transport.storeFailures.add(new IggyClientException("The connection dropped"));

            assertThatThrownBy(() -> reader.seek(1L, PollingStrategy.offset(BigInteger.TEN))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldFailAPendingSeekOfAPartitionThatLeavesTheAssignment() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, ON_READ);
        try {
            assignment(transport, 1L, 2L);
            reader.next();
            Read read = transport.nextRead();
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));

            assignment(transport, 2L);

            assertInvalidCommand(sought);
            read.answer();
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /** A reader that abandons a lost read waits its retry interval before it reads that partition again. */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWaitTheRetryIntervalBeforeItReadsAgainAfterALostRead(boolean wholeRead) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            reader.next();
            Read lost = transport.nextRead();
            long lostAt = System.nanoTime();
            if (wholeRead) {
                lost.reply()
                        .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
            } else {
                lost.answer(FakeTransport.lost(1, 42));
            }

            transport.nextRead();

            assertThat(System.nanoTime() - lostAt).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            assertThat(transport.abandoned).containsExactly(42L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Only a lost read explains a read that starts past the expected offset. Once a later read passed the gap check, a
     * forward jump comes from a store of the application or another session, and is delivered as it is.
     */
    @Test
    void shouldCatchUpAForwardJumpOnlyWhileALostReadCanHaveStoredIt() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 2, 3));
            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L);

            CompletableFuture<PolledMessages> third = reader.next();
            Read jump = transport.nextRead();
            assertThat(jump.commit()).isTrue();
            jump.answer(section(1, 9));

            assertThat(offsets(third.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(9L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** The read of a partition that left and came back ends without freeing the partition for a second read. */
    @Test
    void shouldKeepOneReadOfAPartitionThatLeftAndCameBackWhileItsOldReadRuns() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            assignment(transport, 1L);
            Read old = transport.nextRead();
            assignment(transport);
            assignment(transport, 1L);
            Read current = transport.nextRead();

            old.answer(section(1, 0));

            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            current.answer(section(1, 0, 1));
            assertThat(offsets(next(reader))).containsExactly(0L, 1L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /** The server can end a membership before the close, which then has nothing to leave. */
    @Test
    void shouldCloseAGroupReaderWhoseMembershipTheServerAlreadyEnded() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.leaveFailure = serverError(IggyErrorCode.CONSUMER_GROUP_MEMBER_NOT_FOUND);
        PartitionReader reader = member(transport, MANUAL);
        assignment(transport, 1L);

        closeGroupReader(reader, transport);

        assertThat(transport.membership).containsExactly("join", "leave");
    }

    @Test
    void shouldReturnTheFailureOfTheFirstCloseToALaterClose() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.starts.put(1L, 5L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();
        transport.storeFailures.add(new IggyClientException("The connection dropped"));

        assertThatThrownBy(() -> reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
        assertThatThrownBy(() -> reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
    }

    /** A first message that does not decode stops its partition with the skip past it, so a seek moves on. */
    @Test
    void shouldNameTheSkipOfAFirstMessageThatDoesNotDecode() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.probes.put(
                1L,
                new FetchedMessages(
                        1,
                        FetchedMessages.STATUS_OK,
                        BigInteger.valueOf(4),
                        1,
                        FakeTransport.CONTEXT,
                        BigInteger.valueOf(4),
                        new byte[] {1, 2, 3}));
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            ReadException error = readError(reader);

            assertThat(error.skip()).map(PollingStrategy::value).contains(BigInteger.valueOf(4));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A lost first read from an offset can have stored from there, so the close moves the offset back before it. */
    @Test
    void shouldMoveTheOffsetBackBeforeALostReadFromTheStartOffsetOnCloseUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = PartitionReader.builder(transport)
                .partitions(List.of(1L))
                .start(PollingStrategy.offset(BigInteger.valueOf(5)))
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read lost = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        lost.reply().completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
        assertThat(transport.readsNothingFor(QUIET)).isTrue();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 4));
    }

    /**
     * A read of a partition with a backlog returns at once, so the free slots spread the backlogged partitions of a
     * group over reads of their own, each with the whole limits, and a quiet one keeps the read that may wait.
     */
    @Test
    void shouldSpreadTheBacklogOfAGroupOverReadsOfTheirOwn() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, MANUAL, 1L, 2L, 3L, 4L);
        try {
            Read first = transport.nextRead();
            assertThat(first.targets()).containsOnlyKeys(1L, 2L, 3L, 4L);

            first.answer(
                    FakeTransport.backlogged(1, 100, 0),
                    FakeTransport.backlogged(2, 100, 0),
                    FakeTransport.backlogged(3, 100, 0),
                    section(4, 0));

            List<Read> next =
                    List.of(transport.nextRead(), transport.nextRead(), transport.nextRead(), transport.nextRead());
            assertThat(next)
                    .extracting(read -> read.targets().keySet())
                    .containsExactlyInAnyOrder(Set.of(1L), Set.of(2L), Set.of(3L), Set.of(4L));
            assertThat(next).extracting(Read::count).containsOnly(MANUAL.count());
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Under OnRead a waiting call fills the free slots too: the backlogged partitions of a group take committing reads
     * of their own, each with the whole limits, beside the one read of its quiet partitions that may wait, and a
     * partition is in one committing read at a time.
     */
    @Test
    void shouldSpreadTheBacklogOfAGroupOverCommittingReadsOfTheirOwnUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L, 2L, 3L, 4L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            Read read = transport.nextRead();
            assertThat(read.targets()).containsOnlyKeys(1L, 2L, 3L, 4L);
            read.answer(
                    FakeTransport.backlogged(1, 100, 0),
                    FakeTransport.backlogged(2, 100, 0),
                    FakeTransport.backlogged(3, 100, 0),
                    section(4, 0));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            next(reader);
            next(reader);
            next(reader);
            assertThat(transport.readsNothingFor(QUIET))
                    .as("the reader reads for a waiting call only")
                    .isTrue();

            CompletableFuture<PolledMessages> waiting = reader.next();
            List<Read> spread =
                    List.of(transport.nextRead(), transport.nextRead(), transport.nextRead(), transport.nextRead());
            assertThat(spread)
                    .extracting(sent -> sent.targets().keySet())
                    .containsExactlyInAnyOrder(Set.of(1L), Set.of(2L), Set.of(3L), Set.of(4L));
            assertThat(spread).extracting(Read::commit).containsOnly(true);
            assertThat(spread).extracting(Read::count).containsOnly(ON_READ.count());

            Read quiet = spread.stream()
                    .filter(sent -> sent.targets().containsKey(4L))
                    .findFirst()
                    .orElseThrow();
            for (Read sent : spread) {
                if (sent != quiet) {
                    long partition = sent.targets().keySet().iterator().next();
                    sent.answer(FakeTransport.backlogged(partition, 100, 1));
                }
            }
            waiting.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            next(reader);
            next(reader);
            reader.next();

            List<Read> again = List.of(transport.nextRead(), transport.nextRead(), transport.nextRead());
            assertThat(again)
                    .as("the backlog reads again beside the read that waits, which keeps its partition")
                    .extracting(sent -> sent.targets().keySet())
                    .containsExactlyInAnyOrder(Set.of(1L), Set.of(2L), Set.of(3L));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** Delivers offsets 0 and 1, then loses the reply of the next read, and returns the call that waits for it. */
    private static CompletableFuture<PolledMessages> loseTheSecondRead(FakeTransport transport, PartitionReader reader)
            throws Exception {
        CompletableFuture<PolledMessages> first = reader.next();
        transport.nextRead().answer(section(1, 0, 1));
        first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CompletableFuture<PolledMessages> second = reader.next();
        transport
                .nextRead()
                .reply()
                .completeExceptionally(new IggyOperationException(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, 42));
        return second;
    }

    private static PartitionReader standalone(FakeTransport transport, PollOptions options, Long... partitions)
            throws Exception {
        return PartitionReader.builder(transport)
                .partitions(List.of(partitions))
                .options(options)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static PartitionReader member(FakeTransport transport, PollOptions options) throws Exception {
        return PartitionReader.builder(transport)
                .options(options)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static PolledMessages next(PartitionReader reader) throws Exception {
        return reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static ReadException readError(PartitionReader reader) throws Exception {
        try {
            next(reader);
        } catch (ExecutionException failed) {
            assertThat(failed.getCause()).isInstanceOf(ReadException.class);
            return (ReadException) failed.getCause();
        }
        throw new AssertionError("The read did not fail");
    }

    private static List<Long> offsets(PolledMessages batch) {
        return batch.messages().stream()
                .map(Message::header)
                .map(header -> header.offset().longValue())
                .toList();
    }

    private static IggyServerException serverError(IggyErrorCode code) {
        return IggyServerException.fromTcpResponse(code.getCode(), new byte[0]);
    }

    private static void assertInvalidCommand(CompletableFuture<?> call) {
        assertThatThrownBy(() -> call.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyValidationException.class)
                .satisfies(error -> assertThat(((IggyValidationException) error.getCause()).getErrorCode())
                        .isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }

    private static void awaitStores(FakeTransport transport, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (transport.stores.size() < count && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }

    private static void assignment(FakeTransport transport, Long... partitions) throws InterruptedException {
        CompletableFuture<Optional<ConsumerGroupAssignment>> sync =
                transport.assignments.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(sync).as("the reader reads its assignment").isNotNull();
        sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(partitions))));
    }

    /** Answers an assignment read that the close leaves waiting with the same assignment. */
    private static void closeGroupReader(PartitionReader reader, FakeTransport transport) throws Exception {
        CompletableFuture<Void> closed = reader.closeAsync();
        transport.assignments.forEach(
                sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, reader.partitions()))));
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
