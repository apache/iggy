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
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.PollOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.reader.FakeTransport.TIMEOUT;
import static org.apache.iggy.client.async.reader.FakeTransport.section;
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

    private static PollOptions auto(CommitAfter after) {
        return WAIT.withCommit(
                CommitPolicy.auto(new AutoCommit(Optional.empty(), Optional.of(CommitEvery.message()), after)));
    }

    private static IggyConsumer consumer(FakeTransport transport, PollOptions options) throws Exception {
        return IggyConsumer.build(PartitionReader.builder(transport)
                        .partitions(List.of(1L))
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
