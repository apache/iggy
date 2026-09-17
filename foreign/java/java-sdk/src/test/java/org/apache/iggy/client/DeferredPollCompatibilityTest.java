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

package org.apache.iggy.client;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An implementation written before deferred polling existed still compiles and
 * runs. Its inherited default reports the missing support instead of silently
 * degrading to an immediate poll, which would drop the readiness, byte and
 * request-time guarantees the caller asked for.
 */
class DeferredPollCompatibilityTest {

    private static final PolledMessages EMPTY = new PolledMessages(0L, BigInteger.ZERO, 0L, List.of());

    @Test
    void shouldReportMissingSupportFromABlockingImplementation() {
        var client = new LegacyBlockingClient();

        assertThatThrownBy(() -> client.pollMessagesDeferred(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.of(0L),
                        Consumer.of(0L),
                        PollingStrategy.next(),
                        10L,
                        false,
                        DeferredPollOptions.defaults()))
                .isInstanceOf(IggyOperationNotSupportedException.class);
        assertThat(client.immediatePolls).isZero();
    }

    @Test
    void shouldReportMissingSupportFromAnAsyncImplementation() {
        var client = new LegacyAsyncClient();

        assertThatThrownBy(() -> client.pollMessagesDeferred(
                                1L,
                                1L,
                                Optional.of(0L),
                                0L,
                                PollingStrategy.next(),
                                10L,
                                false,
                                DeferredPollOptions.defaults())
                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IggyOperationNotSupportedException.class);
        assertThat(client.immediatePolls).isZero();
    }

    @Test
    void shouldDelegateNumericOverloadsToTheTypedMethod() {
        var client = new RecordingBlockingClient();

        client.pollMessagesDeferred(
                7L, 9L, Optional.of(3L), 5L, PollingStrategy.next(), 10L, true, DeferredPollOptions.defaults());

        assertThat(client.streamId).hasToString("7");
        assertThat(client.topicId).hasToString("9");
        assertThat(client.consumer.id()).hasToString("5");
        assertThat(client.consumer.kind()).isEqualTo(Consumer.Kind.Consumer);
    }

    private static class LegacyBlockingClient implements org.apache.iggy.client.blocking.MessagesClient {
        private int immediatePolls;

        @Override
        public PolledMessages pollMessages(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit) {
            immediatePolls++;
            return EMPTY;
        }

        @Override
        public SendMessagesResponse sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            return SendMessagesResponse.empty();
        }
    }

    private static final class LegacyAsyncClient implements org.apache.iggy.client.async.MessagesClient {
        private int immediatePolls;

        @Override
        public CompletableFuture<PolledMessages> pollMessages(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit) {
            immediatePolls++;
            return CompletableFuture.completedFuture(EMPTY);
        }

        @Override
        public CompletableFuture<SendMessagesResponse> sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            return CompletableFuture.completedFuture(SendMessagesResponse.empty());
        }
    }

    private static final class RecordingBlockingClient extends LegacyBlockingClient {
        private StreamId streamId;
        private TopicId topicId;
        private Consumer consumer;

        @Override
        @SuppressWarnings("checkstyle:ParameterNumber")
        public PolledMessages pollMessagesDeferred(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit,
                DeferredPollOptions options) {
            this.streamId = streamId;
            this.topicId = topicId;
            this.consumer = consumer;
            return EMPTY;
        }
    }
}
