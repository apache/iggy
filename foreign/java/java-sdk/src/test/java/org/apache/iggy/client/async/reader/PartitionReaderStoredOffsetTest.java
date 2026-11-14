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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.LongStream;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.ON_READ;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.RETRY_INTERVAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.assignment;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.closeGroupReader;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.loseTheSecondRead;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.member;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.next;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.offsets;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.serverError;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.standalone;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The reader under OnRead against the offset that the server stores for its consumer: its start, seek and close. Also
 * the polls of the positions that a fetch does not take.
 */
class PartitionReaderStoredOffsetTest {
    // The owner that a rebalance by another member installs for the member, at an op that no reply of the client named.
    static final PartitionContext INSTALLED =
            new PartitionContext(FakeTransport.CONTEXT.incarnation(), BigInteger.valueOf(5), BigInteger.valueOf(5));
    // The first message that the poll of a timestamp or Last start finds.
    private static final long POLLED_AT = 7;
    // The read of the gap that loseTheSecondRead() leaves once a later read serves offset 5: from 2, without a store.
    private static final PollingStrategy GAP_FROM_TWO =
            new PollingStrategy(PollingKind.Offset, BigInteger.TWO, Optional.of(FakeTransport.CONTEXT));
    // A refusal that a retry does not change stops its partition once it came this often in a row.
    private static final int REFUSALS_BEFORE_THE_STOP = 6;

    /** Under OnRead a start applies only to a partition without a stored offset. One with it resumes after it. */
    @ParameterizedTest
    @MethodSource("startsOtherThanNext")
    void shouldResumeAfterTheStoredOffsetInsteadOfTheStartUnderOnRead(PollingStrategy start) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 5L);
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            Read read = transport.nextRead();
            assertThat(read.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            read.answer(section(1, 6, 7));

            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(6L, 7L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<PollingStrategy> startsOtherThanNext() {
        return List.of(
                PollingStrategy.first(),
                PollingStrategy.offset(BigInteger.ZERO),
                PollingStrategy.last(),
                PollingStrategy.timestamp(BigInteger.TEN));
    }

    @Test
    void shouldReadFromTheStartOffsetOfAPartitionWithoutAStoredOffsetUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PollingStrategy start = PollingStrategy.offset(BigInteger.valueOf(5));
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            Read read = transport.nextRead();
            assertThat(read.targets()).containsEntry(1L, start);
            read.answer(section(1, 5, 6));

            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L, 6L);
            assertThat(transport.lookups).containsExactly(1L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A failed lookup of the stored offset fails as a read does, and the lookup runs again after the retry interval. */
    @Test
    void shouldFailAsAReadAndLookUpAgainAfterTheRetryIntervalWhenTheLookupOfTheStoredOffsetFailsUnderOnRead()
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyClientException dropped = new IggyClientException("The connection dropped");
        transport.lookupReply = CompletableFuture.failedFuture(dropped);
        PollingStrategy start = PollingStrategy.offset(BigInteger.valueOf(5));
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        try {
            long failedAt = System.nanoTime();
            CompletableFuture<PolledMessages> failed = reader.next();
            assertThatThrownBy(() -> failed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .cause()
                    .isInstanceOf(ReadException.class)
                    .cause()
                    .isSameAs(dropped);
            transport.lookupReply = CompletableFuture.completedFuture(null);
            CompletableFuture<PolledMessages> polled = reader.next();
            Read read = transport.nextRead();

            assertThat(System.nanoTime() - failedAt).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            assertThat(read.targets()).containsEntry(1L, start);
            read.answer(section(1, 5, 6));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L, 6L);
            assertThat(transport.lookups).containsExactly(1L, 1L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A member looks the stored offset up at each assignment, so it resumes instead of reading from its start again. */
    @Test
    void shouldResumeAfterTheStoredOffsetAtEachAssignmentOfAMemberUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.offsets.put(1L, 5L);
        PartitionReader reader = PartitionReader.builder(transport)
                .start(PollingStrategy.offset(BigInteger.ZERO))
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            for (long resume : new long[] {6, 7}) {
                assignment(transport, 1L);
                CompletableFuture<PolledMessages> polled = reader.next();
                Read read = transport.nextRead();
                assertThat(read.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
                read.answer(section(1, resume));
                assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                        .containsExactly(resume);
                assignment(transport);
            }
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * Nothing was delivered, so the close moves the offset before the first undelivered batch. A Last start without a
     * stored offset keeps the offset before its batch and deletes nothing.
     */
    @ParameterizedTest
    @MethodSource("startsAndTheirFirstBatch")
    void shouldMoveTheOffsetBeforeAnUndeliveredFirstBatchOfAStartOnCloseUnderOnRead(PollingStrategy start, long first)
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read read = fetchFrom(transport, first);
        assertThat(cancelled.cancel(false)).isTrue();
        read.answer(section(1, first, first + 1));
        assertThat(transport.readsNothingFor(QUIET)).isTrue();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.offsets).isEqualTo(first == 0 ? Map.of() : Map.of(1L, first - 1));
    }

    static List<Arguments> startsAndTheirFirstBatch() {
        return List.of(
                Arguments.of(PollingStrategy.offset(BigInteger.ZERO), 0L),
                Arguments.of(PollingStrategy.offset(BigInteger.valueOf(5)), 5L),
                Arguments.of(PollingStrategy.last(), 500L));
    }

    /**
     * The server can store a read that the close cancels before its reply. So the close moves the offset before the
     * start offset of that read. A timestamp or Last start reads from the first message of its poll, so the close
     * moves the offset before that message.
     */
    @ParameterizedTest
    @MethodSource("startsAndTheirRewind")
    void shouldMoveTheOffsetBeforeAFirstReadWithoutAReplyOnCloseUnderOnRead(
            PollingStrategy start, Map<Long, Long> rewound) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        reader.next();
        Read first = fetchFrom(transport, POLLED_AT);
        assertThat(first.commit()).isTrue();
        assertThat(first.targets().get(1L).kind()).isEqualTo(PollingKind.Offset);
        // The server stored the read, and its outcome stays unknown after the cancel.
        transport.offsets.put(1L, 9L);
        transport.lost.add(42L);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.offsets).isEqualTo(rewound);
    }

    /**
     * A read from a start offset past the last message stores nothing, and the server refuses a store past the last
     * message. So without a batch, the close looks the offset up and leaves it as it is.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldStoreNothingOnCloseAfterAReadFromAStartOffsetPastTheEndUnderOnRead(boolean lostReply) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.ends.put(1L, 3L);
        PartitionReader reader = standalone(transport, PollingStrategy.offset(BigInteger.TEN), ON_READ, 1L);
        CompletableFuture<PolledMessages> polled = reader.next();
        Read read = transport.nextRead();
        if (lostReply) {
            assertThat(polled.cancel(false)).isTrue();
            read.answer(FakeTransport.lost(1, 42));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
        } else {
            transport.lost.add(42L);
        }

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).isEmpty();
        assertThat(transport.offsets).isEmpty();
    }

    static List<Arguments> startsAndTheirRewind() {
        return List.of(
                Arguments.of(PollingStrategy.offset(BigInteger.ZERO), Map.of()),
                Arguments.of(PollingStrategy.offset(BigInteger.valueOf(5)), Map.of(1L, 4L)),
                Arguments.of(PollingStrategy.last(), Map.of(1L, POLLED_AT - 1)),
                Arguments.of(PollingStrategy.timestamp(BigInteger.TEN), Map.of(1L, POLLED_AT - 1)));
    }

    /**
     * A timestamp or Last start reads from the first message of its poll. So after a lost first read, the close moves
     * the offset before that message, as for an offset start.
     */
    @ParameterizedTest
    @MethodSource("startsWithoutAnOffset")
    void shouldMoveTheOffsetBeforeThePolledStartOfALostFirstReadOnCloseUnderOnRead(PollingStrategy start)
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read lost = fetchFrom(transport, POLLED_AT);
        assertThat(cancelled.cancel(false)).isTrue();
        transport.offsets.put(1L, 9L);
        lost.answer(FakeTransport.lost(1, 42));
        assertThat(transport.readsNothingFor(QUIET)).isTrue();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.offsets).isEqualTo(Map.of(1L, POLLED_AT - 1));
    }

    static List<PollingStrategy> startsWithoutAnOffset() {
        return List.of(PollingStrategy.last(), PollingStrategy.timestamp(BigInteger.TEN));
    }

    /** The server refuses a delete of a missing offset, so a close before offset 0 without one sends only the lookup. */
    @Test
    void shouldSendNoDeleteOnCloseBeforeOffsetZeroWithoutAStoredOffsetUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.lookups).containsExactly(1L);
        assertThat(transport.deleted).isEmpty();
    }

    /**
     * A read never moves the stored offset back, so a seek to a timestamp behind the stored offset stores the offset
     * before the message that its probe finds, and the reader goes on from there without a jump.
     */
    @Test
    void shouldSeekToTheOffsetOfATimestampBehindTheStoredOffsetUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 9L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            PollingStrategy timestamp = PollingStrategy.timestamp(BigInteger.TEN);
            CompletableFuture<Void> sought = reader.seek(1L, timestamp);
            Read probe = transport.nextRead();
            assertThat(probe.commit()).isFalse();
            assertThat(probe.targets()).containsEntry(1L, timestamp);
            probe.answer(section(1, 3));
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.stores).containsExactly(new Store(1, 2));

            for (long[] batch : new long[][] {{3, 4}, {5, 6}}) {
                CompletableFuture<PolledMessages> polled = reader.next();
                Read read = transport.nextRead();
                assertThat(read.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
                // The server starts a Next read after its stored offset.
                assertThat(transport.offsets.get(1L) + 1).isEqualTo(batch[0]);
                read.answer(section(1, batch));
                assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                        .containsExactly(batch[0], batch[1]);
            }
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Without a message at the timestamp, every later one lies after the stored offset, so the seek stores nothing. The
     * partition then polls the timestamp and fetches from the message that the poll finds.
     */
    @Test
    void shouldReadFromATimestampWithoutAMessageAndStoreNothingUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            PollingStrategy timestamp = PollingStrategy.timestamp(BigInteger.TEN);
            CompletableFuture<Void> sought = reader.seek(1L, timestamp);
            transport.nextRead().answer(FakeTransport.empty(1, 4));
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            reader.next();

            Read check = transport.nextRead();
            assertThat(check.poll()).isTrue();
            assertThat(check.targets()).containsEntry(1L, timestamp);
            check.answer(section(1, 12));
            Read read = transport.nextRead();
            assertThat(read.commit()).isTrue();
            assertThat(read.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(12));
            assertThat(transport.stores).isEmpty();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** The partition reads nothing until the probe of a seek ends, so the probe answers at once. */
    @Test
    void shouldProbeASeekToATimestampWithoutAWaitUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.timestamp(BigInteger.TEN));
            Read probe = transport.nextRead();
            probe.answer(FakeTransport.empty(1, 4));
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(probe.commit()).isFalse();
            assertThat(probe.options().maxWait()).isZero();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldFailASeekToATimestampWhoseProbeFailsAndReadOnFromTheStoredOffsetUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.timestamp(BigInteger.TEN));
            transport.nextRead().reply().completeExceptionally(new IggyClientException("The connection dropped"));

            assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
            reader.next();
            assertThat(transport.nextRead().targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            assertThat(transport.stores).isEmpty();
            assertThat(transport.deleted).isEmpty();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A seek stores before it moves its partition, and the partition starts no read meanwhile, also for a call that
     * waits for a batch. So the first read after the seek starts at the seek point.
     */
    @ParameterizedTest
    @MethodSource("seeksToTen")
    void shouldStartNoReadOfThePartitionWhileTheStoreOfASeekRunsUnderOnRead(PollingStrategy seek) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<Void> sought = reader.seek(1L, seek);
            if (seek.kind() == PollingKind.Timestamp) {
                transport.nextRead().answer(section(1, 10));
            }
            int requests = transport.requested.size();
            CompletableFuture<PolledMessages> polled = reader.next();

            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            assertThat(transport.requested).hasSize(requests);
            assertThat(sought).isNotDone();
            transport.storeReply.complete(null);
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Read read = transport.nextRead();
            assertThat(read.commit()).isTrue();
            assertThat(read.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            // The server starts a Next read after its stored offset.
            assertThat(transport.offsets.get(1L) + 1).isEqualTo(10L);
            read.answer(section(1, 10, 11));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(10L, 11L);
        } finally {
            transport.storeReply.complete(null);
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<PollingStrategy> seeksToTen() {
        return List.of(PollingStrategy.offset(BigInteger.TEN), PollingStrategy.timestamp(BigInteger.valueOf(100)));
    }

    /** A seek whose write fails leaves its partition as it was: no queued batch is lost, and the reads go on. */
    @ParameterizedTest
    @MethodSource("seeksThatWrite")
    void shouldKeepThePositionAndTheQueuedBatchWhenTheWriteOfASeekFailsUnderOnRead(PollingStrategy seek)
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read queued = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            queued.answer(section(1, 2, 3));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            (seek.kind() == PollingKind.First ? transport.deleteFailures : transport.storeFailures)
                    .add(new IggyClientException("The connection dropped"));

            CompletableFuture<Void> sought = reader.seek(1L, seek);
            if (seek.kind() == PollingKind.Timestamp) {
                transport.nextRead().answer(section(1, 1));
            }

            assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
            assertThat(offsets(reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L);
            CompletableFuture<PolledMessages> polled = reader.next();
            Read read = transport.nextRead();
            assertThat(read.targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            assertThat(transport.offsets.get(1L) + 1).isEqualTo(4L);
            read.answer(section(1, 4));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(4L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<PollingStrategy> seeksThatWrite() {
        return List.of(
                PollingStrategy.offset(BigInteger.TEN),
                PollingStrategy.first(),
                PollingStrategy.timestamp(BigInteger.TEN));
    }

    /** Seeks of one partition run in call order, each after the writes of the one before it. */
    @Test
    void shouldRunEachSeekOfAPartitionAfterTheOneBeforeItUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            reader.next();
            Read read = transport.nextRead();
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<Void> toTen = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));
            CompletableFuture<Void> toTwenty = reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(20)));

            read.answer();

            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            assertThat(transport.sent).containsExactly(new Store(1, 9));
            transport.storeReply.complete(null);
            toTen.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            toTwenty.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.stores).containsExactly(new Store(1, 9), new Store(1, 19));
        } finally {
            transport.storeReply.complete(null);
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The close runs the seeks that wait for a read in flight in call order. A seek that its caller cancels during its
     * probe holds the next one until it ends, and its cancel does not fail the close.
     */
    @Test
    void shouldRunTheWaitingSeeksInOrderOnCloseAfterOneWasCancelledUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();
        CompletableFuture<Void> cancelled = reader.seek(1L, PollingStrategy.timestamp(BigInteger.valueOf(100)));
        CompletableFuture<Void> toTen = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));

        CompletableFuture<Void> closed = reader.closeAsync();
        Read probe = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();

        assertThat(probe.poll()).isTrue();
        assertThat(transport.readsNothingFor(QUIET)).isTrue();
        assertThat(transport.sent).isEmpty();
        probe.answer(section(1, 5));
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        toTen.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.stores).containsExactly(new Store(1, 9));
    }

    /** A seek that its caller cancelled while it waited stops at its turn, before its probe or lookup. */
    @ParameterizedTest
    @MethodSource("seeksThatReadBeforeTheirWrite")
    void shouldStopASeekThatItsCallerCancelledWhileItWaitedBeforeAnyRequestOnCloseUnderOnRead(PollingStrategy target)
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();
        CompletableFuture<Void> cancelled = reader.seek(1L, target);
        assertThat(cancelled.cancel(false)).isTrue();

        CompletableFuture<Void> closed = reader.closeAsync();

        assertThat(transport.readsNothingFor(QUIET)).isTrue();
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        // The rewind of the read in flight looks the offset up, and the seek adds no lookup.
        assertThat(transport.lookups).containsExactly(1L);
        assertThat(transport.events).isEmpty();
    }

    static List<PollingStrategy> seeksThatReadBeforeTheirWrite() {
        return List.of(PollingStrategy.timestamp(BigInteger.valueOf(100)), PollingStrategy.offset(BigInteger.ZERO));
    }

    /**
     * After a seek store with an unknown outcome, the close stores the offset from before that store. A store that
     * moved the offset back would otherwise make a restart deliver the messages in between again.
     */
    @Test
    void shouldStoreTheLastDeliveredOffsetOnCloseAfterASeekStoreWithAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> polled = reader.next();
        fetchFrom(transport, 18).answer(section(1, 18, 19));
        assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                .containsExactly(18L, 19L);
        failSeekWithUnknownStore(transport, reader);
        assertThat(transport.offsets).containsEntry(1L, 9L);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly("store 1 19");
        assertThat(transport.offsets).containsEntry(1L, 19L);
    }

    /**
     * A fetch takes no timestamp or Last target, so the reader polls such a start without a store and fetches from the
     * first message of the poll. Without a message, it polls again. A Last poll asks for the count of the reader.
     */
    @ParameterizedTest
    @MethodSource("startsThatAFetchDoesNotTake")
    void shouldPollAStartThatAFetchDoesNotTakeAndFetchFromItsFirstMessage(PollingStrategy start, PollOptions options)
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, start, options, 1L);
        try {
            CompletableFuture<PolledMessages> polled = reader.next();
            Read empty = transport.nextRead();
            assertThat(empty.poll()).isTrue();
            assertThat(empty.targets()).containsEntry(1L, start);
            empty.answer(FakeTransport.empty(1, 0));
            Read check = transport.nextRead();
            assertThat(check.poll()).isTrue();
            check.answer(section(1, POLLED_AT, POLLED_AT + 1));

            Read fetch = transport.nextRead();
            assertThat(fetch.poll()).isFalse();
            assertThat(fetch.targets().get(1L).kind()).isEqualTo(PollingKind.Offset);
            assertThat(fetch.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(POLLED_AT));
            assertThat(check.count()).isEqualTo(start.kind() == PollingKind.Last ? fetch.count() : 1);
            fetch.answer(section(1, POLLED_AT, POLLED_AT + 1));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(POLLED_AT, POLLED_AT + 1);
            assertThat(transport.stores).isEmpty();
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<Arguments> startsThatAFetchDoesNotTake() {
        return List.of(
                Arguments.of(PollingStrategy.timestamp(BigInteger.TEN), MANUAL),
                Arguments.of(PollingStrategy.last(), MANUAL),
                Arguments.of(PollingStrategy.timestamp(BigInteger.TEN), ON_READ),
                Arguments.of(PollingStrategy.last(), ON_READ));
    }

    /** A read in flight ends when the client released it. For a lost read that is its abandon, which the seek waits for. */
    @Test
    void shouldStoreASeekAfterTheAbandonOfALostReadUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read read = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            transport.abandonReply = new CompletableFuture<>();
            read.answer(FakeTransport.lost(1, 42));
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));

            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            assertThat(transport.abandoned).containsExactly(42L);
            assertThat(transport.sent).isEmpty();
            transport.abandonReply.complete(null);
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.stores).containsExactly(new Store(1, 9));
        } finally {
            transport.abandonReply.complete(null);
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A seek that the caller cancels before its first write stops and changes nothing. */
    @Test
    void shouldStopASeekThatIsCancelledBeforeItsFirstWriteUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 3L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            transport.lookupReply = new CompletableFuture<>();
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.ZERO));
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            assertThat(transport.lookups).containsExactly(1L);

            assertThat(sought.cancel(false)).isTrue();
            transport.lookupReply.complete(null);
            reader.next();

            assertThat(transport.nextRead().targets().get(1L).kind()).isEqualTo(PollingKind.Next);
            assertThat(transport.deleted).isEmpty();
            assertThat(transport.offsets).isEqualTo(Map.of(1L, 3L));
        } finally {
            transport.lookupReply.complete(null);
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A seek that its caller cancelled after the first write ends in the background, and a failed write is logged. */
    @Test
    void shouldLogTheFailedWriteOfASeekThatItsCallerCancelledAfterTheWriteUnderOnRead() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(PartitionReader.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (transport.sent.isEmpty() && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            assertThat(sought.cancel(false)).isTrue();
            transport.storeFailures.add(new IggyClientException("The connection dropped"));
            transport.storeReply.complete(null);

            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(transport.sent).containsExactly(new Store(1, 9));
            assertThat(appender.list)
                    .filteredOn(event -> event.getLevel() == Level.WARN)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .anySatisfy(message ->
                            assertThat(message).contains("partition 1").contains("The connection dropped"));
        } finally {
            logger.detachAppender(appender);
            transport.storeReply.complete(null);
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A seek store whose outcome stays unknown fails the seek and keeps the position. The seek abandons the store, so a
     * later seek writes, and the next read reads again what the store could skip.
     */
    @Test
    void shouldReadAgainWhatASeekStoreWithAnUnknownOutcomeCouldSkipUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> first = reader.next();
            transport.nextRead().answer(section(1, 0, 1));
            first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));

            assertThatThrownBy(() -> reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(5)))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyOperationException.class);
            assertThat(transport.abandoned).containsExactly(42L);
            CompletableFuture<PolledMessages> polled = reader.next();
            // The store took effect, so the server reads on after offset 4.
            transport.nextRead().answer(section(1, 5, 6));
            Read gap = transport.nextRead();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.TWO);
            gap.answer(section(1, 2, 3, 4));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L, 4L);
            reader.seek(1L, PollingStrategy.offset(BigInteger.TEN)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(transport.stores).containsExactly(new Store(1, 9));
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Before its first read, a Next position names no offset. So a seek looks the stored offset up before its store,
     * and after a store with an unknown outcome the next read starts after the offset from before the seek.
     */
    @Test
    void shouldReadAfterTheOldStoredOffsetWhenASeekStoreBeforeAnyReadHasAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 4L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            failSeekWithUnknownStore(transport, reader);

            CompletableFuture<PolledMessages> polled = reader.next();
            // The store took effect, so the server reads on after offset 9.
            transport.nextRead().answer(section(1, 10, 11));
            Read gap = transport.nextRead();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(5));
            gap.answer(section(1, 5, 6, 7, 8, 9));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L, 6L, 7L, 8L, 9L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Before its lookup, an offset start names no stored offset. After a seek store with an unknown outcome, the start
     * applies as the lookup before the store found it, and the read after it catches up what the store could skip.
     */
    @Test
    void shouldApplyTheStartAndCatchUpWhenASeekStoreBeforeTheStartLookupHasAnUnknownOutcomeUnderOnRead()
            throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PollingStrategy start = PollingStrategy.offset(BigInteger.valueOf(3));
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        try {
            failSeekWithUnknownStore(transport, reader);

            CompletableFuture<PolledMessages> first = reader.next();
            Read read = transport.nextRead();
            assertThat(read.targets()).containsEntry(1L, start);
            read.answer(section(1, 3, 4));
            assertThat(offsets(first.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(3L, 4L);
            CompletableFuture<PolledMessages> second = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            Read gap = transport.nextRead();
            assertThat(gap.targets().get(1L).value()).isEqualTo(BigInteger.valueOf(5));
            gap.answer(section(1, 5, 6, 7, 8, 9));
            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L, 6L, 7L, 8L, 9L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Retention removed the segments below the first kept offset after the lost read of offsets 2 to 4, and a read
     * below that segment starts at it. So the reader reports the gone offsets once and hands over the rest.
     */
    @ParameterizedTest(name = "first kept offset: {0}")
    @ValueSource(longs = {4, 5})
    void shouldReportTheGoneStartOfAGapOnceAndHandOverTheRestUnderOnRead(long firstKept) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 5, 6));
            Read gap = transport.nextRead();
            assertThat(gap.targets()).isEqualTo(Map.of(1L, GAP_FROM_TWO));
            gap.answer(section(1, LongStream.rangeClosed(firstKept, 6).toArray()));

            assertThat(second)
                    .failsWithin(TIMEOUT)
                    .withThrowableThat()
                    .withCauseInstanceOf(ReadException.class)
                    .withMessageContaining("Offsets 2 to " + (firstKept - 1) + " were stored by a read");
            if (firstKept < 5) {
                assertThat(offsets(next(reader))).containsExactly(firstKept);
            }
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
            CompletableFuture<PolledMessages> after = reader.next();
            transport.nextRead().answer(section(1, 7));
            assertThat(offsets(after.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(7L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A server that could not serve the read of a gap answers with no messages and a zero context, which says nothing
     * about the gap. So the reader reports nothing, waits the retry interval and reads the same range again.
     */
    @ParameterizedTest(name = "next offset of the zero reply: {0}")
    @ValueSource(longs = {0, 5})
    void shouldReadTheSameRangeAgainAfterAZeroReplyToTheReadOfAGapUnderOnRead(long nextOffset) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 5, 6));
            transport
                    .nextRead()
                    .answer(new FetchedMessages(
                            1,
                            FetchedMessages.STATUS_OK,
                            BigInteger.ZERO,
                            0,
                            PartitionContext.EMPTY,
                            BigInteger.valueOf(nextOffset),
                            new byte[0]));

            assertThat(transport.readsNothingFor(RETRY_INTERVAL.dividedBy(2)))
                    .as("the pause before the read again")
                    .isTrue();
            Read again = transport.nextRead();
            assertThat(again.targets()).isEqualTo(Map.of(1L, GAP_FROM_TWO));
            assertThat(second).isNotDone();
            again.answer(section(1, 2, 3, 4, 5, 6));
            assertThat(offsets(second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(2L, 3L, 4L);
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Only a seek resumes a stopped partition, and it drops the gap. So a partition that stops during the read of a gap
     * first reports the offsets that it did not read again and hands over the batch after them, and reports the
     * refusal that stopped it last.
     */
    @ParameterizedTest
    @MethodSource("refusalsThatStopAGap")
    void shouldReportAGapAndHandOverItsBatchBeforeTheRefusalThatStopsThePartitionUnderOnRead(
            IggyErrorCode refusal, int refusals) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = PartitionReader.builder(transport)
                .partitions(List.of(1L))
                .options(ON_READ)
                .retryInterval(PartitionReaderPacingTest.SHORT_RETRY)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(section(1, 5, 6));
            for (int refused = 1; refused <= refusals; refused++) {
                Read gap = transport.nextRead();
                assertThat(gap.targets()).isEqualTo(Map.of(1L, GAP_FROM_TWO));
                gap.reply().completeExceptionally(serverError(refusal));
            }

            assertThatThrownBy(() -> second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).lost())
                            .contains(new ReadException.OffsetRange(BigInteger.TWO, BigInteger.valueOf(4))));
            assertThat(offsets(next(reader))).containsExactly(5L, 6L);
            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(refusal.getCode()));
            CompletableFuture<PolledMessages> afterSeek = reader.next();
            assertThat(transport.readsNothingFor(QUIET))
                    .as("the stopped partition reads nothing")
                    .isTrue();

            reader.seek(1L, PollingStrategy.next()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.nextRead().answer(section(1, 7));
            assertThat(offsets(afterSeek.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(7L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static List<Arguments> refusalsThatStopAGap() {
        // The read of a gap names a history, so a history that is gone stops the partition at once.
        return List.of(
                Arguments.of(IggyErrorCode.UNAUTHORIZED, REFUSALS_BEFORE_THE_STOP),
                Arguments.of(IggyErrorCode.HISTORY_UNAVAILABLE, 1));
    }

    /**
     * An empty read can show what a lost read stored, so no batch follows the gap. A partition that stops during the
     * read of that gap reports its offsets, and the error that stopped it follows them at once.
     */
    @Test
    void shouldReportAGapWithoutABatchBeforeTheGoneHistoryThatStopsThePartitionUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            CompletableFuture<PolledMessages> second = loseTheSecondRead(transport, reader);
            transport.nextRead().answer(FakeTransport.empty(1, 5));
            Read gap = transport.nextRead();
            assertThat(gap.targets()).isEqualTo(Map.of(1L, GAP_FROM_TWO));
            gap.answer(FakeTransport.refused(1, IggyErrorCode.HISTORY_UNAVAILABLE.getCode()));

            assertThatThrownBy(() -> second.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).lost())
                            .contains(new ReadException.OffsetRange(BigInteger.TWO, BigInteger.valueOf(4))));
            assertThatThrownBy(() -> next(reader))
                    .hasCauseInstanceOf(ReadException.class)
                    .satisfies(error -> assertThat(((ReadException) error.getCause()).errorCode())
                            .hasValue(IggyErrorCode.HISTORY_UNAVAILABLE.getCode()));
            CompletableFuture<PolledMessages> afterSeek = reader.next();
            assertThat(transport.readsNothingFor(QUIET))
                    .as("the stopped partition reads nothing")
                    .isTrue();

            reader.seek(1L, PollingStrategy.next()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.nextRead().answer(section(1, 5));
            assertThat(offsets(afterSeek.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A timestamp start whose lookup found no stored offset gets a seek, whose store has an unknown outcome. A close
     * before any read then deletes the offset that the store could leave, so a restart applies the start again.
     */
    @Test
    void shouldRestoreTheMissingOffsetOfAStartWhenASeekStoreHasAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, PollingStrategy.timestamp(BigInteger.TEN), ON_READ, 1L);
        reader.next();
        Read poll = transport.nextRead();
        assertThat(poll.poll()).isTrue();
        poll.answer(FakeTransport.empty(1, 0));
        failSeekWithUnknownStore(transport, reader);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.offsets).doesNotContainKey(1L);
        assertThat(transport.events).containsExactly("delete 1");
    }

    /**
     * The same holds for an offset start, before or after its lookup, and for a start that a poll resolved to an
     * offset. The store can leave an offset before or after that start, and the close deletes it. A batch that arrived
     * still decides the rewind, so the close stores the offset before a queued batch.
     */
    @ParameterizedTest
    @MethodSource("startsWithoutAStoredOffsetAndSeeks")
    void shouldRewindAStartWithoutAStoredOffsetAfterASeekStoreWithAnUnknownOutcomeUnderOnRead(
            PollingStrategy start, FetchedMessages firstReply, long seekTo, String closeWrite) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, start, ON_READ, 1L);
        if (firstReply != null) {
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read first = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            first.answer(firstReply);
        }
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
        assertThatThrownBy(() -> reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(seekTo)))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);
        assertThat(transport.offsets).containsEntry(1L, seekTo - 1);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly(closeWrite);
    }

    static List<Arguments> startsWithoutAStoredOffsetAndSeeks() {
        PollingStrategy five = PollingStrategy.offset(BigInteger.valueOf(5));
        FetchedMessages nothing = FakeTransport.empty(1, 5);
        return List.of(
                Arguments.of(five, null, 3L, "delete 1"),
                Arguments.of(five, null, 10L, "delete 1"),
                Arguments.of(five, nothing, 3L, "delete 1"),
                Arguments.of(five, nothing, 10L, "delete 1"),
                Arguments.of(PollingStrategy.timestamp(BigInteger.TEN), section(1, POLLED_AT), 3L, "delete 1"),
                Arguments.of(five, section(1, 5, 6), 3L, "store 1 4"));
    }

    /**
     * A read whose reply was lost stored past what the application took. After a seek store with an unknown outcome,
     * the close moves the offset back before that read, not to the offset that the read left.
     */
    @Test
    void shouldMoveBeforeALostReadOnCloseAfterASeekStoreWithAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 4L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read read = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        transport.offsets.put(1L, 6L);
        read.answer(FakeTransport.lost(1, 41));
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
        assertThatThrownBy(() -> reader.seek(1L, PollingStrategy.offset(BigInteger.TWO))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);
        assertThat(transport.offsets).containsEntry(1L, 1L);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly("store 1 4");
    }

    /**
     * A position from a poll names where the next read starts, not what the server stored. After a seek store with an
     * unknown outcome, the close restores the offset from before that store, or deletes it when there was none. The
     * same holds for a seek that waited for the poll.
     */
    @ParameterizedTest
    @MethodSource("storedOffsetsAndPollTimings")
    void shouldRestoreTheStoredOffsetOnCloseAfterAPolledPositionAndASeekStoreWithAnUnknownOutcomeUnderOnRead(
            Long stored, boolean seeksDuringThePoll, String closeWrite) throws Exception {
        FakeTransport transport = new FakeTransport(false);
        if (stored != null) {
            transport.offsets.put(1L, stored);
        }
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.seek(1L, PollingStrategy.last()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Read poll = transport.nextRead();
        assertThat(poll.poll()).isTrue();
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
        PollingStrategy twenty = PollingStrategy.offset(BigInteger.valueOf(20));
        CompletableFuture<Void> waiting = seeksDuringThePoll ? reader.seek(1L, twenty) : null;
        poll.answer(section(1, 90, 91));
        CompletableFuture<Void> sought = seeksDuringThePoll ? waiting : reader.seek(1L, twenty);
        assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);
        assertThat(transport.offsets).containsEntry(1L, 19L);

        reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly(closeWrite);
    }

    static List<Arguments> storedOffsetsAndPollTimings() {
        return List.of(
                Arguments.of(50L, false, "store 1 50"),
                Arguments.of(50L, true, "store 1 50"),
                Arguments.of(null, false, "delete 1"),
                Arguments.of(null, true, "delete 1"));
    }

    /**
     * A seek delete with an unknown outcome can have removed the stored offset, so a read from the next offset would
     * start at 0. The partition reads on from its old position instead, also after an empty read.
     */
    @Test
    void shouldReadFromTheOldPositionAfterASeekDeleteWithAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 4L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        try {
            transport.deleteFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
            assertThatThrownBy(() ->
                            reader.seek(1L, PollingStrategy.first()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyOperationException.class);
            assertThat(transport.offsets).doesNotContainKey(1L);

            CompletableFuture<PolledMessages> polled = reader.next();
            PollingStrategy old = PollingStrategy.offset(BigInteger.valueOf(5));
            Read empty = transport.nextRead();
            assertThat(empty.targets()).containsEntry(1L, old);
            empty.answer(FakeTransport.empty(1, 5));
            Read read = transport.nextRead();
            assertThat(read.targets()).containsEntry(1L, old);
            read.answer(section(1, 5));
            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L);
        } finally {
            reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A seek store that ends with an unknown outcome after its partition left and came back belongs to the earlier
     * assignment, whose offset another member can have moved since. The new assignment stays as it is, so its close
     * moves nothing.
     */
    @Test
    void shouldLeaveANewAssignmentAloneWhenASeekStoreOfAnEarlierOneHasAnUnknownOutcomeUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.offsets.put(1L, 4L);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        CompletableFuture<Void> reply = new CompletableFuture<>();
        try {
            assignment(transport, 1L);
            await(() -> reader.partitions().contains(1L));
            transport.storeReply = reply;
            CompletableFuture<Void> sought = reader.seek(1L, PollingStrategy.offset(BigInteger.TEN));
            await(() -> !transport.sent.isEmpty());
            assignment(transport);
            assignment(transport, 1L);
            await(() -> reader.partitions().contains(1L));
            assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyValidationException.class);
            transport.offsets.put(1L, 40L);
            transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
            reply.complete(null);
            await(() -> !transport.abandoned.isEmpty());
        } finally {
            reply.complete(null);
            closeGroupReader(reader, transport);
        }

        assertThat(transport.abandoned).containsExactly(42L);
        assertThat(transport.events).isEmpty();
    }

    /**
     * A batch of an earlier assignment can stay queued, or reach the application after its partition came back.
     * Another member can have moved the offset since, so it decides nothing at the close, and only the new assignment
     * does.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldMoveNothingOnCloseForABatchOfAnEarlierAssignmentUnderOnRead(boolean handedOver) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> cancelled = reader.next();
            Read read = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            read.answer(section(1, 10, 11));
            assignment(transport);
            assignment(transport, 1L);
            await(() -> reader.partitions().contains(1L));
            if (handedOver) {
                assertThat(offsets(reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                        .containsExactly(10L, 11L);
                PartitionContext nextOwner =
                        new PartitionContext(BigInteger.valueOf(7), BigInteger.TWO, BigInteger.TWO);
                transport.probes.put(1L, section(1, nextOwner, 30));
                CompletableFuture<PolledMessages> next = reader.next();
                Read own = transport.nextRead();
                assertThat(next.cancel(false)).isTrue();
                own.answer(section(1, nextOwner, 30, 31));
            }
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).isEqualTo(handedOver ? List.of("store 1 29") : List.of());
    }

    /**
     * A lost membership, as after a lost session, starts no new assignment: what the earlier session delivered still
     * decides the close. The server installs the owner again under a higher generation, so the store names owner
     * generation 0 until a read names the new owner, and the op that the client saw committed after that install.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldMoveBackWithoutTheOwnerOfALostMembershipOnCloseUnderOnRead(boolean newOwnerRead) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        PartitionContext newOwner = new PartitionContext(BigInteger.valueOf(7), BigInteger.TWO, BigInteger.TWO);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            assertThat(offsets(next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(10L, 11L);
            if (!newOwnerRead) {
                queue(reader, transport, section(1, 12, 13));
            }
            loseMembership(transport, 1L);
            if (newOwnerRead) {
                queue(reader, transport, section(1, newOwner, 12, 13));
            }
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).containsExactly("store 1 11");
        assertThat(transport.written)
                .extracting(PartitionContext::ownerGeneration)
                .containsExactly(newOwnerRead ? BigInteger.TWO : BigInteger.ZERO);
    }

    /** A commit, and the final store of an automatic commit, after a lost membership name owner generation 0. */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldStoreACheckpointWithoutTheOwnerOfALostMembership(boolean automatic) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        // Two messages count no event, so only the close stores.
        AutoCommit everyThousand =
                new AutoCommit(Optional.empty(), Optional.of(CommitEvery.messages(1000)), CommitAfter.DELIVERY);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(automatic ? MANUAL.withCommit(CommitPolicy.auto(everyThousand)) : MANUAL)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            Checkpoint delivered = next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .checkpoint()
                    .orElseThrow();
            loseMembership(transport, 1L);
            if (!automatic) {
                reader.commit(delivered).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).containsExactly("store 1 11");
        assertThat(transport.written)
                .extracting(PartitionContext::ownerGeneration)
                .containsExactly(BigInteger.ZERO);
    }

    /** A seek write after a new session names owner generation 0 and the op that the client saw committed. */
    @Test
    void shouldStoreASeekWithoutTheOwnerOfALostMembershipUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            loseMembership(transport, 1L);

            reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(5)))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).containsExactly("store 1 4");
    }

    /**
     * A new session installs the owner again, and the reader learns it only from the refusal of its next read, which
     * looks the same as after a move away and back. That refusal is no leave: the partition keeps its lost read, and
     * the next read and the close rewind name owner generation 0.
     */
    @Test
    void shouldRewindBeforeALostReadAfterARefusalOfItsOwnerUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L);
            queue(reader, transport, FakeTransport.lost(1, 41));
            transport.offsets.put(1L, 11L);
            transport.changeSession();
            reader.next();
            Read stale = transport.nextRead();
            assertThat(owner(stale)).isEqualTo(BigInteger.ONE);

            stale.answer(FakeTransport.refused(1, IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()));
            assignment(transport, 1L);

            assertThat(owner(transport.nextRead())).isZero();
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).containsExactly("store 1 9");
        assertThat(transport.written).extracting(PartitionContext::metadataOp).containsExactly(BigInteger.TWO);
    }

    /**
     * Only a write that replaces a lost owner with owner generation 0 needs the op that the client saw committed. A
     * reader that lost no membership keeps the op of its read.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldStoreWithTheOpOfItsReadWithoutALostMembership(boolean group) throws Exception {
        FakeTransport transport = new FakeTransport(group);
        PartitionReader reader = group ? member(transport, MANUAL) : standalone(transport, MANUAL, 1L);
        try {
            if (group) {
                assignment(transport, 1L);
            }
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            Checkpoint delivered = next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .checkpoint()
                    .orElseThrow();
            transport.commitMetadata();

            reader.commit(delivered).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.written).containsExactly(FakeTransport.CONTEXT);
    }

    /**
     * A rebalance can install the owner of the member again at an op that no reply of the client named, so the server
     * refuses a write that names the owner of the last read. That is no leave. The reader reads the partition once,
     * without a commit, with owner generation 0 and without a wait, and after a transient refusal again after the retry
     * interval. The write then goes once more with the owner and op of the reply. The client sends a write without a
     * context with the incarnation of its route, so a reply of any incarnation serves it, and the write keeps
     * incarnation 0.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldSendASeekWriteAgainForTheOwnerThatTheServerInstalledUnderOnRead(boolean readFirst) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = member(transport, ON_READ);
        PartitionContext sent = readFirst ? FakeTransport.CONTEXT : PartitionContext.EMPTY;
        PartitionContext installed = readFirst ? INSTALLED : otherIncarnation(INSTALLED);
        try {
            assignment(transport, 1L);
            await(() -> reader.partitions().contains(1L));
            if (readFirst) {
                CompletableFuture<PolledMessages> next = reader.next();
                transport.nextRead().answer(section(1, 10, 11));
                next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());

            CompletableFuture<Void> seek = reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(5)));
            Read refused = transport.nextRead();
            long refusedAt = System.nanoTime();
            refused.answer(FakeTransport.refused(1, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()));
            Read served = transport.nextRead();
            assertThat(System.nanoTime() - refusedAt).isGreaterThanOrEqualTo(RETRY_INTERVAL.toNanos());
            served.answer(section(1, installed, 12));
            seek.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(List.of(refused, served)).allSatisfy(read -> {
                assertThat(read.poll()).isTrue();
                assertThat(read.count()).isOne();
                assertThat(read.targets()).isEqualTo(Map.of(1L, PollingStrategy.next()));
                assertThat(read.options().maxWait()).isZero();
            });
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).containsExactly("store 1 4");
        assertThat(transport.written)
                .containsExactly(
                        sent,
                        new PartitionContext(sent.incarnation(), installed.ownerGeneration(), installed.metadataOp()));
    }

    /**
     * The close rewind goes once more for the owner that the server installed only if the read for that owner serves
     * the incarnation of the rewind. Otherwise the refusal fails the close as before. The server refuses a read of one
     * partition over its poll quota as a whole request, and the close reads the owner again after that refusal. That
     * read needs only the context of its reply, so a message in it that does not decode changes nothing.
     */
    @ParameterizedTest
    @MethodSource("ownerReads")
    void shouldMoveBackOnCloseForTheOwnerThatTheServerInstalledUnderOnRead(
            FetchedMessages ownerRead, boolean moves, boolean overQuota) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        PartitionReader reader = member(transport, ON_READ);
        assignment(transport, 1L);
        CompletableFuture<PolledMessages> next = reader.next();
        transport.nextRead().answer(section(1, 10, 11));
        next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        queue(reader, transport, section(1, 12, 13));
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());

        CompletableFuture<Void> closed = reader.closeAsync();
        if (overQuota) {
            Read refused = transport.nextRead();
            refused.reply().completeExceptionally(serverError(IggyErrorCode.DEFERRED_POLL_CAPACITY_EXCEEDED));
        }
        transport.nextRead().answer(ownerRead);
        transport.assignments.forEach(sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(1L)))));

        if (moves) {
            closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            assertThatThrownBy(() -> closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyServerException.class);
        }
        assertThat(transport.events).isEqualTo(moves ? List.of("store 1 11") : List.of());
    }

    static List<Arguments> ownerReads() {
        return List.of(
                Arguments.of(section(1, INSTALLED, 12), true, false),
                Arguments.of(section(1, otherIncarnation(INSTALLED), 12), false, false),
                Arguments.of(
                        FakeTransport.refused(1, IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()),
                        false,
                        false),
                Arguments.of(section(1, INSTALLED, 12), true, true),
                Arguments.of(FakeTransport.undecodable(1, INSTALLED, 12), true, false));
    }

    /**
     * Once sent, a seek write has the reader as its caller. So the close ends the read for the owner of a seek write
     * that the server refused as not the owner, and the refusal stands. The close sends no other read, also when it
     * starts while that read waits for its retry.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldEndTheOwnerReadOfASeekWriteWhenTheCloseStartsUnderOnRead(boolean waitsForRetry) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        // The close starts long before the retry of a refused read.
        PartitionReader reader = PartitionReader.builder(transport)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL.multipliedBy(10))
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assignment(transport, 1L);
        await(() -> reader.partitions().contains(1L));
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
        CompletableFuture<Void> seek = reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(5)));
        Read ownerRead = transport.nextRead();
        FetchedMessages refusal = FakeTransport.refused(1, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());

        if (waitsForRetry) {
            ownerRead.answer(refusal);
        }
        CompletableFuture<Void> closed = startClose(reader);
        if (!waitsForRetry) {
            ownerRead.answer(refusal);
        }
        transport.assignments.forEach(sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(1L)))));

        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThatThrownBy(() -> seek.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyServerException.class)
                .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                        .isEqualTo(IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()));
        assertThat(transport.reads).isEmpty();
        assertThat(transport.events).isEmpty();
    }

    /**
     * The reader is the caller of an automatic store too, so the close ends the read for its owner in the same way, and
     * the refusal of that store is logged. The final store of the close then reads the owner itself and goes once more
     * for it.
     */
    @Test
    void shouldEndTheOwnerReadOfAnAutomaticStoreWhenTheCloseStarts() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(PartitionReader.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        FakeTransport transport = new FakeTransport(true);
        try {
            AutoCommit everyBatch =
                    new AutoCommit(Optional.empty(), Optional.of(CommitEvery.batch()), CommitAfter.DELIVERY);
            PartitionReader reader = member(transport, MANUAL.withCommit(CommitPolicy.auto(everyBatch)));
            assignment(transport, 1L);
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
            transport.storeReply.complete(null);
            Read ownerRead = nextPoll(transport);

            CompletableFuture<Void> closed = startClose(reader);
            ownerRead.answer(FakeTransport.refused(1, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()));
            nextPoll(transport).answer(section(1, INSTALLED, 12));
            transport.assignments.forEach(
                    sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(1L)))));
            closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(appender.list)
                .filteredOn(event -> event.getLevel() == Level.ERROR)
                .extracting(ILoggingEvent::getFormattedMessage)
                .singleElement()
                .satisfies(message -> assertThat(message)
                        .contains("Failed to store the counted offsets of a reader")
                        .contains(IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.name()));
        assertThat(transport.events).containsExactly("store 1 11");
        // The automatic store, the final store of the close without the lost owner, and that store for the new owner.
        assertThat(transport.written)
                .extracting(PartitionContext::ownerGeneration)
                .containsExactly(BigInteger.ONE, BigInteger.ZERO, INSTALLED.ownerGeneration());
    }

    /**
     * The close ends the read for the owner of a commit of the application too. The commit fails with its refusal
     * before the close ends, and no further store of it goes out: not once the wait for a retry of that read ends, and
     * not for an answer of that read that comes after the close started.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldEndTheOwnerReadOfACommitWhenTheCloseStarts(boolean waitsForRetry) throws Exception {
        FakeTransport transport = new FakeTransport(true);
        // The close starts long before the retry of a refused read.
        PartitionReader reader = PartitionReader.builder(transport)
                .options(MANUAL)
                .retryInterval(RETRY_INTERVAL.multipliedBy(10))
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assignment(transport, 1L);
        CompletableFuture<PolledMessages> next = reader.next();
        transport.nextRead().answer(section(1, 10, 11));
        Checkpoint delivered =
                next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).checkpoint().orElseThrow();
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
        // The leave waits, so the close cannot end before the commit does.
        transport.leaveReply = new CompletableFuture<>();
        CompletableFuture<Void> commit = reader.commit(delivered);
        Read ownerRead = nextPoll(transport);

        if (waitsForRetry) {
            ownerRead.answer(FakeTransport.refused(1, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()));
        }
        CompletableFuture<Void> closed = startClose(reader);
        if (!waitsForRetry) {
            ownerRead.answer(section(1, INSTALLED, 12));
        }

        assertThatThrownBy(() -> commit.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyServerException.class)
                .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                        .isEqualTo(IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()));
        assertThat(closed).as("the commit ends before the close").isNotDone();
        transport.assignments.forEach(sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(1L)))));
        transport.leaveReply.complete(null);
        closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.reads).noneMatch(Read::poll);
        assertThat(transport.sent).containsExactly(new Store(1, 11));
    }

    /** A write of a partition that left the assignment keeps the refusal of the server, and reads nothing for it. */
    @Test
    void shouldFailACommitThatTheServerRefusesAfterItsPartitionLeft() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            Checkpoint delivered = next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .checkpoint()
                    .orElseThrow();
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<Void> commit = reader.commit(delivered);
            assignment(transport);
            await(() -> reader.partitions().isEmpty());
            transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
            transport.storeReply.complete(null);

            assertThatThrownBy(() -> commit.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyServerException.class);
            assertThat(transport.reads).noneMatch(Read::poll);
        } finally {
            transport.storeReply.complete(null);
            closeGroupReader(reader, transport);
        }

        assertThat(transport.written).containsExactly(FakeTransport.CONTEXT);
    }

    /**
     * After a lost membership, as after a lost session, the owner of the partition is gone. A partition that then
     * leaves before a read names its new owner has no owner that can store for it, so it stores nothing.
     */
    @Test
    void shouldStoreNothingForAPartitionThatLeavesAfterALostMembership() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        AutoCommit everyThousand =
                new AutoCommit(Optional.empty(), Optional.of(CommitEvery.messages(1000)), CommitAfter.DELIVERY);
        PartitionReader reader = member(transport, MANUAL.withCommit(CommitPolicy.auto(everyThousand)));
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> next = reader.next();
            transport.nextRead().answer(section(1, 10, 11));
            next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            loseMembership(transport, 1L);

            assignment(transport);
            await(() -> reader.partitions().isEmpty());
        } finally {
            closeGroupReader(reader, transport);
        }

        assertThat(transport.events).isEmpty();
    }

    /**
     * A committing read that the close cancels can have stored its end. If a seek waits for it and the store of the
     * seek has an unknown outcome, the read decides as a lost read does: the close stores before its start. A delete
     * would let a restart with the Last start skip 8 and 9.
     */
    @Test
    void shouldStoreBeforeAReadThatTheCloseCancelsWhileASeekWaitsUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        PartitionReader reader = standalone(transport, PollingStrategy.last(), ON_READ, 1L);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read poll = transport.nextRead();
        assertThat(poll.poll()).isTrue();
        poll.answer(section(1, 8, 9));
        assertThat(transport.nextRead().commit()).isTrue();
        assertThat(cancelled.cancel(false)).isTrue();
        CompletableFuture<Void> seek = reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(6)));
        transport.offsets.put(1L, 9L);
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));

        assertThatThrownBy(() -> reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);

        assertThat(seek).isCompletedExceptionally();
        assertThat(transport.events).containsExactly("store 1 7");
    }

    /**
     * A rewind store of the close with an unknown outcome fails the close. The close releases it in the client, so a
     * later reader of the partition on the same client can store.
     */
    @Test
    void shouldReleaseARewindStoreWithAnUnknownOutcomeWhenTheCloseFailsUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        transport.offsets.put(1L, 4L);
        PartitionReader reader = standalone(transport, ON_READ, 1L);
        reader.next();
        transport.nextRead();
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));

        assertThatThrownBy(() -> reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);
        assertThat(transport.abandoned).containsExactly(42L);
        transport
                .store(1L, BigInteger.valueOf(4), FakeTransport.CONTEXT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * A lookup of a start that ends after its partition left the assignment and came back belongs to the old state. Its
     * refusal leaves the new state alone, which reads with a lookup of its own.
     */
    @Test
    void shouldDropTheStartLookupOfAnEarlierAssignmentUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        CompletableFuture<Void> stale = new CompletableFuture<>();
        transport.lookupReply = stale;
        PollingStrategy start = PollingStrategy.offset(BigInteger.valueOf(5));
        PartitionReader reader = PartitionReader.builder(transport)
                .start(start)
                .options(ON_READ)
                .retryInterval(RETRY_INTERVAL)
                .build()
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        try {
            assignment(transport, 1L);
            CompletableFuture<PolledMessages> polled = reader.next();
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            while (transport.lookups.isEmpty() && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(5);
            }
            transport.lookupReply = CompletableFuture.completedFuture(null);
            assignment(transport);
            assignment(transport, 1L);
            Read read = transport.nextRead();
            assertThat(read.targets()).containsEntry(1L, start);

            stale.completeExceptionally(serverError(IggyErrorCode.INVALID_SIZE_BYTES));
            read.answer(section(1, 5, 6));

            assertThat(offsets(polled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)))
                    .containsExactly(5L, 6L);
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    private static void failSeekWithUnknownStore(FakeTransport transport, PartitionReader reader) {
        transport.storeFailures.add(new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, 42));
        assertThatThrownBy(() -> reader.seek(1L, PollingStrategy.offset(BigInteger.TEN))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyOperationException.class);
        assertThat(transport.abandoned).containsExactly(42L);
    }

    /** The first fetch of a start. A timestamp or Last start polls first, and the fake answers that poll at {@code at}. */
    private static Read fetchFrom(FakeTransport transport, long at) throws InterruptedException {
        Read read = transport.nextRead();
        if (!read.poll()) {
            return read;
        }
        read.answer(section(1, at));
        return transport.nextRead();
    }

    /** Starts the close of {@code reader}, and returns once it started: the loop runs it before any later call. */
    private static CompletableFuture<Void> startClose(PartitionReader reader) {
        CompletableFuture<Void> closed = reader.closeAsync();
        assertThatThrownBy(() -> reader.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
        return closed;
    }

    /** The next poll of the reader. The reads ahead of a reader without {@code OnRead} stay without an answer. */
    static Read nextPoll(FakeTransport transport) throws InterruptedException {
        Read read = transport.nextRead();
        while (!read.poll()) {
            read = transport.nextRead();
        }
        return read;
    }

    /** Leaves a batch queued: its read answers a {@code next()} that the caller cancelled. */
    static void queue(PartitionReader reader, FakeTransport transport, FetchedMessages section)
            throws InterruptedException {
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read read = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        read.answer(section);
    }

    /**
     * Answers an assignment read with no membership, as after a lost session whose successor installed the owners
     * again, and the one after the join.
     */
    static void loseMembership(FakeTransport transport, Long... partitions) throws InterruptedException {
        CompletableFuture<Optional<ConsumerGroupAssignment>> sync =
                transport.assignments.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(sync).as("the reader reads its assignment").isNotNull();
        transport.changeSession();
        sync.complete(Optional.empty());
        assignment(transport, partitions);
    }

    /** The owner generation that {@code read} names for partition 1. */
    private static BigInteger owner(Read read) {
        return read.targets().get(1L).context().orElseThrow().ownerGeneration();
    }

    private static PartitionContext otherIncarnation(PartitionContext context) {
        return new PartitionContext(
                context.incarnation().add(BigInteger.ONE), context.ownerGeneration(), context.metadataOp());
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }
}
