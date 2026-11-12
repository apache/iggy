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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What an interrupt does to a blocking poll, which a cancel can make lose the messages that it committed. */
class MessagesTcpClientFetchTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration INTERRUPT_GRACE = Duration.ofMillis(300);

    @Test
    void shouldWaitThroughAnInterruptForTheReplyOfACommittingPoll() throws Exception {
        HeldPoll delegate = new HeldPoll();
        Caller caller = Caller.poll(new MessagesTcpClient(delegate), true);
        delegate.called.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        caller.thread.interrupt();
        // An interrupt that ended the wait would complete the call at once.
        assertThatThrownBy(() -> caller.result.get(INTERRUPT_GRACE.toMillis(), TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        PolledMessages polled = new PolledMessages(1L, BigInteger.ZERO, 0L, List.of(), PartitionContext.EMPTY);
        delegate.polled.complete(polled);

        assertThat(caller.result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isSameAs(polled);
        assertThat(caller.interruptedAfter.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
    }

    @Test
    void shouldCancelAPollThatDoesNotCommitWhenTheCallerIsInterrupted() throws Exception {
        HeldPoll delegate = new HeldPoll();
        Caller caller = Caller.poll(new MessagesTcpClient(delegate), false);
        delegate.called.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        caller.thread.interrupt();

        assertThatThrownBy(() -> caller.result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
        assertThat(delegate.polled).isCancelled();
    }

    /** A thread that makes one blocking poll and reports its outcome and its interrupt flag after it. */
    private record Caller(
            Thread thread, CompletableFuture<Object> result, CompletableFuture<Boolean> interruptedAfter) {
        static Caller poll(MessagesTcpClient client, boolean autoCommit) {
            return start(() -> client.pollMessagesDeferred(
                    StreamId.of(1L),
                    TopicId.of(1L),
                    Optional.of(1L),
                    Consumer.of(1L),
                    PollingStrategy.next(),
                    1L,
                    autoCommit,
                    DeferredPollOptions.defaults()));
        }

        private static Caller start(Supplier<Object> call) {
            CompletableFuture<Object> result = new CompletableFuture<>();
            CompletableFuture<Boolean> interruptedAfter = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                try {
                    result.complete(call.get());
                } catch (RuntimeException error) {
                    result.completeExceptionally(error);
                }
                interruptedAfter.complete(Thread.currentThread().isInterrupted());
            });
            thread.setDaemon(true);
            thread.start();
            return new Caller(thread, result, interruptedAfter);
        }
    }

    /** An async client whose deferred poll replies only when the test completes {@link #polled}. */
    private static final class HeldPoll implements org.apache.iggy.client.async.MessagesClient {
        private final CompletableFuture<Void> called = new CompletableFuture<>();
        private final CompletableFuture<PolledMessages> polled = new CompletableFuture<>();

        @Override
        @SuppressWarnings("checkstyle:ParameterNumber")
        public CompletableFuture<PolledMessages> pollMessagesDeferred(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit,
                DeferredPollOptions options) {
            called.complete(null);
            return polled;
        }

        @Override
        public CompletableFuture<PolledMessages> pollMessages(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit) {
            throw new UnsupportedOperationException("The test makes deferred polls only");
        }

        @Override
        public CompletableFuture<SendMessagesResponse> sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            throw new UnsupportedOperationException("The test makes deferred polls only");
        }
    }
}
