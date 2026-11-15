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

import org.apache.iggy.client.async.reader.FakeTransport.Store;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderCancelTest.TRANSIENT_REFUSAL;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.INSTALLED;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.await;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.QUIET;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.RETRY_INTERVAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.assignment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The consumer counts each message as its commit policy says. */
class IggyConsumerTest {
    private static final PollOptions WAIT = PollOptions.waitFor(Duration.ofSeconds(1));

    @Test
    void shouldCountAMessageWhenTheNextIsAskedForAndTheLastOneAtShutdown() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, auto(CommitAfter.PROCESSING));
        transport.nextRead().answer(section(1, 0, 1));

        assertThat(offset(consumer.next())).isZero();
        assertThat(offset(consumer.next())).isEqualTo(1);
        awaitStores(transport, 1);
        assertThat(transport.stores).containsExactly(new Store(1, 0));

        consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 0), new Store(1, 1));
    }

    @Test
    void shouldCountEachMessageAtDelivery() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, auto(CommitAfter.DELIVERY));
        transport.nextRead().answer(section(1, 3, 4));

        offset(consumer.next());

        awaitStores(transport, 1);
        assertThat(transport.stores).containsExactly(new Store(1, 3));
        consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** A handler that fails leaves its message uncounted and ends the loop with its error. */
    @Test
    void shouldCountAfterASuccessfulHandlerAndEndTheLoopOnAFailedOne() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, auto(CommitAfter.PROCESSING));
        transport.nextRead().answer(section(1, 0, 1, 2));
        List<Long> handled = new CopyOnWriteArrayList<>();
        IllegalStateException failure = new IllegalStateException("handler failed");

        CompletableFuture<Void> loop = consumer.consume(message -> {
            long offset = message.checkpoint().offset().longValue();
            handled.add(offset);
            return offset == 2 ? CompletableFuture.failedFuture(failure) : CompletableFuture.completedFuture(null);
        });

        assertThatThrownBy(() -> loop.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCause(failure);
        assertThat(handled).containsExactly(0L, 1L, 2L);
        consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertThat(transport.stores).containsExactly(new Store(1, 0), new Store(1, 1));
    }

    /** Under OnRead the server stored the whole batch, so the shutdown moves the offset back to the last one taken. */
    @Test
    void shouldMoveTheOffsetBackToTheLastMessageHandedOverAtShutdownUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT.withCommit(CommitPolicy.onRead()));
        CompletableFuture<ReceivedMessage> first = consumer.next();
        transport.nextRead().answer(section(1, 0, 1, 2, 3));
        offset(first);
        offset(consumer.next());

        consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.stores).containsExactly(new Store(1, 1));
        assertThatThrownBy(() -> consumer.next().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
    }

    @Test
    void shouldKeepTheMessageWhenNextIsCancelledBeforeTheReadCompletes() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT.withCommit(CommitPolicy.onRead()));
        try {
            CompletableFuture<ReceivedMessage> cancelled = consumer.next();
            FakeTransport.Read read = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            CompletableFuture<ReceivedMessage> resumed = consumer.next();
            read.answer(section(1, 0, 1));

            assertThat(offset(resumed)).isZero();
            assertThat(offset(consumer.next())).isEqualTo(1);
        } finally {
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldShutDownWhileTheHandlerLoopWaitsForMessages() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT.withCommit(CommitPolicy.onRead()));
        CompletableFuture<Void> running = consumer.consume(message -> {
            throw new AssertionError("No message was delivered");
        });
        FakeTransport.Read read = transport.nextRead();
        try {
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(running).isDone();
        } finally {
            running.cancel(false);
            read.reply().complete(List.of());
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A seek drops only the batches of its own partition, so a batch of another one that the reader handed over stays. */
    @Test
    void shouldKeepABatchOfAnotherPartitionThatArrivesWhileASeekRuns() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT, 1L, 2L);
        try {
            CompletableFuture<ReceivedMessage> pending = consumer.next();
            FakeTransport.Read read = transport.nextRead();
            consumer.seek(1L, PollingStrategy.offset(BigInteger.TEN)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            read.answer(section(2, 5));

            ReceivedMessage message = pending.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(message.partitionId()).isEqualTo(2L);
            assertThat(message.checkpoint().offset()).isEqualTo(BigInteger.valueOf(5));
        } finally {
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A seek holds the messages of its partition back. A failed one hands them over, also a batch that the reader hands
     * over during the seek, so it loses no message.
     */
    @Test
    void shouldHandOverTheHeldMessagesAfterAFailedSeekUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT.withCommit(CommitPolicy.onRead()));
        try {
            CompletableFuture<ReceivedMessage> first = consumer.next();
            transport.nextRead().answer(section(1, 0, 1));
            assertThat(offset(first)).isZero();
            transport.storeFailures.add(new IggyClientException("The connection dropped"));
            assertThatThrownBy(() -> consumer.seek(1L, PollingStrategy.offset(BigInteger.TEN))
                            .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
            assertThat(offset(consumer.next())).isEqualTo(1);

            CompletableFuture<ReceivedMessage> cancelled = consumer.next();
            FakeTransport.Read read = transport.nextRead();
            assertThat(cancelled.cancel(false)).isTrue();
            read.answer(section(1, 2, 3));
            transport.storeReply = new CompletableFuture<>();
            transport.storeFailures.add(new IggyClientException("The connection dropped"));
            CompletableFuture<Void> sought = consumer.seek(1L, PollingStrategy.offset(BigInteger.TEN));
            CompletableFuture<ReceivedMessage> held = consumer.next();
            assertThat(transport.readsNothingFor(QUIET)).isTrue();
            assertThat(held).isNotDone();
            transport.storeReply.complete(null);

            assertThatThrownBy(() -> sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(IggyClientException.class);
            assertThat(offset(held)).isEqualTo(2);
            assertThat(offset(consumer.next())).isEqualTo(3);
        } finally {
            transport.storeReply.complete(null);
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A seek that the consumer refuses at once changes nothing. So the held message stays with the earlier seek of its
     * partition, and that seek drops it when it moves.
     */
    @Test
    void shouldLeaveTheHeldMessagesToTheEarlierSeekAfterARefusedOneUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(false);
        IggyConsumer consumer = consumer(transport, WAIT.withCommit(CommitPolicy.onRead()));
        try {
            CompletableFuture<ReceivedMessage> first = consumer.next();
            transport.nextRead().answer(section(1, 0, 1));
            assertThat(offset(first)).isZero();
            transport.storeReply = new CompletableFuture<>();
            CompletableFuture<Void> sought = consumer.seek(1L, PollingStrategy.offset(BigInteger.TEN));
            PollingStrategy ofAnotherPartition =
                    new PollingStrategy(PollingKind.Offset, BigInteger.ONE, Optional.empty(), Optional.of(2L));

            assertThatThrownBy(() -> consumer.seek(1L, ofAnotherPartition)).isInstanceOf(IggyValidationException.class);
            transport.storeReply.complete(null);
            sought.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            CompletableFuture<ReceivedMessage> afterSeek = consumer.next();
            assertThat(afterSeek)
                    .as("the seek that moved dropped the held message")
                    .isNotDone();
            transport.nextRead().answer(section(1, 10));
            assertThat(offset(afterSeek)).isEqualTo(10);
        } finally {
            transport.storeReply.complete(null);
            consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A message of a batch from an earlier assignment of its partition decides nothing at the shutdown: another member
     * can have moved the offset since. So the shutdown moves the offset back before the first batch of the current
     * assignment.
     */
    @Test
    void shouldMoveBackOnShutdownOnlyForTheCurrentAssignmentUnderOnRead() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        IggyConsumer consumer = IggyConsumer.build(PartitionReader.builder(transport)
                        .options(WAIT.withCommit(CommitPolicy.onRead()))
                        .retryInterval(Duration.ofMillis(100)))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        PartitionContext nextOwner = new PartitionContext(BigInteger.valueOf(7), BigInteger.TWO, BigInteger.TWO);
        assignment(transport, 1L);
        queue(consumer, transport, section(1, 10, 11));
        assignment(transport);
        assignment(transport, 1L);
        await(() -> consumer.partitions().contains(1L));
        assertThat(offset(consumer.next())).isEqualTo(10);
        assertThat(offset(consumer.next())).isEqualTo(11);
        transport.probes.put(1L, section(1, nextOwner, 30));
        queue(consumer, transport, section(1, nextOwner, 30, 31));

        CompletableFuture<Void> shutdown = consumer.shutdown();
        transport.assignments.forEach(sync -> sync.complete(Optional.of(new ConsumerGroupAssignment(1, List.of(1L)))));
        shutdown.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThat(transport.events).containsExactly("store 1 29");
    }

    /** A cancel of the shutdown cancels its close, which ends the owner read of the move back. A later one waits. */
    @Test
    void shouldEndTheShutdownWhenItsCallerCancelsUnderALastingTransientRefusal() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        IggyConsumer consumer = IggyConsumer.build(PartitionReader.builder(transport)
                        .options(WAIT.withCommit(CommitPolicy.onRead()))
                        .retryInterval(RETRY_INTERVAL))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assignment(transport, 1L);
        CompletableFuture<ReceivedMessage> first = consumer.next();
        transport.nextRead().answer(section(1, 0, 1));
        offset(first);
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());

        CompletableFuture<Void> shutdown = consumer.shutdown();
        transport.nextRead().answer(TRANSIENT_REFUSAL);
        FakeTransport.Read retried = transport.nextRead();
        assertThat(shutdown.cancel(false)).isTrue();
        retried.answer(TRANSIENT_REFUSAL);

        assertThat(transport.readsNothingFor(RETRY_INTERVAL.multipliedBy(2))).isTrue();
        assertThatThrownBy(() -> consumer.shutdown().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyServerException.class);
        assertThat(transport.membership).containsExactly("join", "leave");
        assertThat(transport.events).isEmpty();
    }

    /** Leaves a batch queued in the reader: its read answers a {@code next()} that the caller cancelled. */
    private static void queue(IggyConsumer consumer, FakeTransport transport, FetchedMessages section)
            throws InterruptedException {
        CompletableFuture<ReceivedMessage> cancelled = consumer.next();
        FakeTransport.Read read = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        read.answer(section);
    }

    private static PollOptions auto(CommitAfter after) {
        return WAIT.withCommit(
                CommitPolicy.auto(new AutoCommit(Optional.empty(), Optional.of(CommitEvery.message()), after)));
    }

    private static IggyConsumer consumer(FakeTransport transport, PollOptions options) throws Exception {
        return consumer(transport, options, 1L);
    }

    private static IggyConsumer consumer(FakeTransport transport, PollOptions options, Long... partitions)
            throws Exception {
        return IggyConsumer.build(PartitionReader.builder(transport)
                        .partitions(List.of(partitions))
                        .options(options)
                        .retryInterval(Duration.ofMillis(100)))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static long offset(CompletableFuture<ReceivedMessage> message) throws Exception {
        return message.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .checkpoint()
                .offset()
                .longValue();
    }

    private static void awaitStores(FakeTransport transport, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (transport.stores.size() < count && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
    }
}
