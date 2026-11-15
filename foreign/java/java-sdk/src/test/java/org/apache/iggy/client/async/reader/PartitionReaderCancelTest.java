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

import org.apache.iggy.client.async.reader.FakeTransport.Read;
import org.apache.iggy.client.async.reader.FakeTransport.Store;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PolledMessages;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.INSTALLED;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.nextPoll;
import static org.apache.iggy.client.async.reader.PartitionReaderStoredOffsetTest.queue;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.MANUAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.ON_READ;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.RETRY_INTERVAL;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.assignment;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.closeGroupReader;
import static org.apache.iggy.client.async.reader.PartitionReaderTest.member;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The cancel of a write that reads its owner after a refusal as not the owner ends that read, and the refusal stands. */
class PartitionReaderCancelTest {
    static final FetchedMessages TRANSIENT_REFUSAL =
            FakeTransport.refused(1, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());

    /**
     * The close belongs to its first caller, so the cancel of a later close ends nothing. The cancel of the first ends
     * the owner read of the move back. The leave and the stop of the reader still run, and a later close waits for them.
     */
    @Test
    void shouldEndTheCloseWhenItsFirstCallerCancelsUnderALastingTransientRefusal() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = memberWithANewOwner(transport);
        CompletableFuture<Void> first = reader.closeAsync();
        CompletableFuture<Void> later = reader.closeAsync();
        assertThat(reader.closeAsync().cancel(false)).isTrue();

        transport.nextRead().answer(TRANSIENT_REFUSAL);
        Read retried = transport.nextRead();
        assertThat(first.cancel(false)).isTrue();
        assertThat(later).isNotDone();
        retried.answer(TRANSIENT_REFUSAL);

        assertRefusedAsNotOwned(later);
        assertThat(transport.readsNothingFor(RETRY_INTERVAL.multipliedBy(2))).isTrue();
        assertThat(loop(reader).awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
        assertThat(transport.membership).containsExactly("join", "leave");
        assertThat(transport.events).isEmpty();
    }

    /**
     * Without a cancel, the close budget ends the owner read of the move back, so a lasting transient refusal cannot
     * hold the close. The refusal stands, and the leave and the stop of the reader still run.
     */
    @Test
    void shouldEndTheOwnerReadOfTheCloseAtItsBudgetUnderALastingTransientRefusal() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = memberWithANewOwner(transport);
        long started = System.nanoTime();
        CompletableFuture<Void> closed = reader.closeAsync();
        long giveUp = started + PartitionReader.CLOSE_BUDGET.plus(TIMEOUT).toNanos();
        while (!closed.isDone() && System.nanoTime() - giveUp < 0) {
            Read ownerRead = transport.reads.poll(RETRY_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
            if (ownerRead != null) {
                ownerRead.answer(TRANSIENT_REFUSAL);
            }
        }

        assertThat(closed).as("the close ends within its budget").isDone();
        assertThat(System.nanoTime() - started).isGreaterThanOrEqualTo(PartitionReader.CLOSE_BUDGET.toNanos());
        assertRefusedAsNotOwned(closed);
        assertThat(transport.membership).containsExactly("join", "leave");
        assertThat(transport.events).isEmpty();
    }

    /**
     * An interrupt of the thread that waits in {@link PartitionReader#close()} cancels the close, as in the blocking
     * reader, so a lasting transient refusal of the owner read holds that thread no longer. The refusal stands, and the
     * leave and the stop of the reader still run.
     */
    @Test
    void shouldEndTheCloseThatItsThreadInterruptsUnderALastingTransientRefusal() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = memberWithANewOwner(transport);
        CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
        CompletableFuture<RuntimeException> thrown = new CompletableFuture<>();
        Thread caller = new Thread(() -> {
            try {
                reader.close();
                thrown.complete(null);
            } catch (RuntimeException error) {
                interrupted.complete(Thread.currentThread().isInterrupted());
                thrown.complete(error);
            }
        });
        caller.setDaemon(true);
        caller.start();

        transport.nextRead().answer(TRANSIENT_REFUSAL);
        Read retried = transport.nextRead();
        caller.interrupt();
        assertThat(thrown.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isInstanceOf(IggyClientException.class);
        assertThat(interrupted.getNow(false)).isTrue();
        retried.answer(TRANSIENT_REFUSAL);

        assertRefusedAsNotOwned(reader.closeAsync());
        assertThat(transport.readsNothingFor(RETRY_INTERVAL.multipliedBy(2))).isTrue();
        assertThat(loop(reader).awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
        assertThat(transport.membership).containsExactly("join", "leave");
        assertThat(transport.events).isEmpty();
    }

    /**
     * After the cancel of the first close, the answer of an owner read in flight leads to no write, and the refusal
     * stands. The writes of the close that were not sent yet still go once, and so does the leave.
     */
    @Test
    void shouldSendNoWriteForTheAnswerOfAnOwnerReadThatEndsAfterTheCancelOfTheClose() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        transport.starts.put(1L, 10L);
        transport.starts.put(2L, 20L);
        PartitionReader reader = member(transport, ON_READ);
        assignment(transport, 1L, 2L);
        CompletableFuture<PolledMessages> cancelled = reader.next();
        Read read = transport.nextRead();
        assertThat(cancelled.cancel(false)).isTrue();
        read.answer(section(1, 10, 11), section(2, 20, 21));
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());

        CompletableFuture<Void> first = reader.closeAsync();
        Read ownerRead = transport.nextRead();
        assertThat(ownerRead.poll()).isTrue();
        assertThat(ownerRead.targets()).containsOnlyKeys(1L);
        assertThat(first.cancel(false)).isTrue();
        ownerRead.answer(section(1, INSTALLED, 12));

        assertRefusedAsNotOwned(reader.closeAsync());
        assertThat(transport.sent).containsExactly(new Store(1, 9), new Store(2, 19));
        assertThat(transport.readsNothingFor(RETRY_INTERVAL.multipliedBy(2))).isTrue();
        assertThat(transport.membership).containsExactly("join", "leave");
        assertThat(transport.events).isEmpty();
    }

    @Test
    void shouldEndTheOwnerReadOfACommitThatItsCallerCancels() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            CompletableFuture<Void> commit = reader.commit(deliveredUnderANewOwner(reader, transport));

            nextPoll(transport).answer(TRANSIENT_REFUSAL);
            Read retried = nextPoll(transport);
            assertThat(commit.cancel(false)).isTrue();
            retried.answer(TRANSIENT_REFUSAL);

            TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL.multipliedBy(2).toMillis());
            assertThat(transport.reads).noneMatch(Read::poll);
            assertThat(transport.events).isEmpty();
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * After the cancel of a commit, the answer of its owner read in flight leads to no write, and the refusal stands.
     */
    @Test
    void shouldSendNoWriteForTheAnswerOfAnOwnerReadThatEndsAfterTheCancelOfACommit() throws Exception {
        FakeTransport transport = new FakeTransport(true);
        PartitionReader reader = member(transport, MANUAL);
        try {
            CompletableFuture<Void> commit = reader.commit(deliveredUnderANewOwner(reader, transport));
            Read ownerRead = nextPoll(transport);
            assertThat(commit.cancel(false)).isTrue();
            ownerRead.answer(section(1, INSTALLED, 12));

            TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL.multipliedBy(2).toMillis());
            assertThat(transport.reads).noneMatch(Read::poll);
            assertThat(transport.sent).containsExactly(new Store(1, 11));
            assertThat(transport.events).isEmpty();
        } finally {
            closeGroupReader(reader, transport);
        }
    }

    /**
     * A member under {@code OnRead} with a queued batch, so its close moves the offset back. The server installed
     * another owner since, so it refuses that write as not the owner.
     */
    static PartitionReader memberWithANewOwner(FakeTransport transport) throws Exception {
        transport.starts.put(1L, 10L);
        PartitionReader reader = member(transport, ON_READ);
        assignment(transport, 1L);
        CompletableFuture<PolledMessages> next = reader.next();
        transport.nextRead().answer(section(1, 10, 11));
        next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        queue(reader, transport, section(1, 12, 13));
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
        return reader;
    }

    /**
     * Delivers a batch of partition 1 to {@code reader} and returns its checkpoint. The server installed another owner
     * since, so it refuses a store of that checkpoint as not the owner.
     */
    private static Checkpoint deliveredUnderANewOwner(PartitionReader reader, FakeTransport transport)
            throws Exception {
        assignment(transport, 1L);
        CompletableFuture<PolledMessages> next = reader.next();
        transport.nextRead().answer(section(1, 10, 11));
        Checkpoint delivered =
                next.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).checkpoint().orElseThrow();
        transport.installOwner(INSTALLED.ownerGeneration(), INSTALLED.metadataOp());
        return delivered;
    }

    private static void assertRefusedAsNotOwned(CompletableFuture<Void> close) {
        assertThatThrownBy(() -> close.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyServerException.class)
                .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                        .isEqualTo(IggyErrorCode.CONSUMER_GROUP_PARTITION_NOT_OWNED.getCode()));
    }

    private static ExecutorService loop(PartitionReader reader) throws ReflectiveOperationException {
        Field field = PartitionReader.class.getDeclaredField("loop");
        field.setAccessible(true);
        return (ExecutorService) field.get(reader);
    }
}
