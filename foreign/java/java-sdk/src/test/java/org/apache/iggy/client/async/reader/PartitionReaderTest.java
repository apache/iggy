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
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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
