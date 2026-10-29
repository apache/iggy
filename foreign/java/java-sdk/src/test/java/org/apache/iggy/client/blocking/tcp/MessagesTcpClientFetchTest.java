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
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What an interrupt does to a blocking fetch, which a cancel can make lose the messages that it committed. */
class MessagesTcpClientFetchTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Test
    void shouldWaitThroughAnInterruptForTheReplyOfACommittingFetch() throws Exception {
        HeldFetch delegate = new HeldFetch();
        Caller caller = Caller.fetch(new MessagesTcpClient(delegate), true);
        delegate.called.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        caller.thread.interrupt();
        List<FetchedMessages> sections = List.of();
        delegate.reply.complete(sections);

        assertThat(caller.result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isSameAs(sections);
        assertThat(caller.interruptedAfter.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
    }

    @Test
    void shouldCancelAFetchThatDoesNotCommitWhenTheCallerIsInterrupted() throws Exception {
        HeldFetch delegate = new HeldFetch();
        Caller caller = Caller.fetch(new MessagesTcpClient(delegate), false);
        delegate.called.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        caller.thread.interrupt();

        assertThatThrownBy(() -> caller.result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .hasCauseInstanceOf(IggyClientException.class);
        assertThat(delegate.reply).isCancelled();
        assertThat(caller.interruptedAfter.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .isTrue();
    }

    /** A thread that makes one blocking fetch and reports its outcome and its interrupt flag after it. */
    private record Caller(
            Thread thread,
            CompletableFuture<List<FetchedMessages>> result,
            CompletableFuture<Boolean> interruptedAfter) {
        static Caller fetch(MessagesTcpClient client, boolean autoCommit) {
            CompletableFuture<List<FetchedMessages>> result = new CompletableFuture<>();
            CompletableFuture<Boolean> interruptedAfter = new CompletableFuture<>();
            Thread thread = new Thread(() -> {
                try {
                    result.complete(client.fetchMessages(
                            StreamId.of(1L),
                            TopicId.of(1L),
                            Map.of(1L, PollingStrategy.next()),
                            Consumer.of(1L),
                            1L,
                            autoCommit,
                            DeferredPollOptions.defaults()));
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

    /** An async client whose fetch replies only when the test completes {@link #reply}. */
    private static final class HeldFetch implements org.apache.iggy.client.async.MessagesClient {
        private final CompletableFuture<Void> called = new CompletableFuture<>();
        private final CompletableFuture<List<FetchedMessages>> reply = new CompletableFuture<>();

        @Override
        public CompletableFuture<List<FetchedMessages>> fetchMessages(
                StreamId streamId,
                TopicId topicId,
                Map<Long, PollingStrategy> targets,
                Consumer consumer,
                Long count,
                boolean autoCommit,
                DeferredPollOptions options) {
            called.complete(null);
            return reply;
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
            throw new UnsupportedOperationException("The test makes fetches only");
        }

        @Override
        public CompletableFuture<SendMessagesResponse> sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            throw new UnsupportedOperationException("The test makes fetches only");
        }
    }
}
