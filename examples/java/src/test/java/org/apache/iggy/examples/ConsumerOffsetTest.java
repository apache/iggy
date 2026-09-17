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

package org.apache.iggy.examples;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.examples.async.AsyncConsumer;
import org.apache.iggy.examples.gettingstarted.consumer.GettingStartedConsumer;
import org.apache.iggy.examples.messageenvelope.consumer.MessageEnvelopeConsumer;
import org.apache.iggy.examples.messageheaders.consumer.MessageHeadersConsumer;
import org.apache.iggy.examples.shared.Messages.OrderConfirmed;
import org.apache.iggy.examples.streambuilder.StreamBasic;
import org.apache.iggy.examples.tcptls.consumer.TcpTlsConsumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout(30)
class ConsumerOffsetTest {
    private static final int MAX_BATCH_SIZE = 2;
    private static final int MESSAGES_COUNT = 10;
    private static final int CONSUMER_TIMEOUT_SECONDS = 20;
    private static final long IDLE_POLL_MILLIS = 100;

    @ParameterizedTest(name = "{0}, first retained offset {1}")
    @MethodSource("blockingConsumers")
    void shouldConsumeRetainedMessagesOnce(Class<?> consumerClass, int firstOffset) throws Exception {
        int messageCount = consumerClass == StreamBasic.class ? 3 : MESSAGES_COUNT;
        var retained = new RetainedMessages(firstOffset, messageCount, consumerClass == MessageEnvelopeConsumer.class);
        try (var client = new IggyTcpClient("localhost", 8090) {
            @Override
            public MessagesClient messages() {
                return retained;
            }
        }) {
            Method consume = consumerClass.getDeclaredMethod("consumeMessages", IggyTcpClient.class);
            consume.setAccessible(true);
            consume.invoke(null, client);
        }
        retained.assertConsumedOnce();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "25, false", "0, true", "25, true"})
    void shouldAdvanceAsynchronouslyOnlyAfterProcessing(int firstOffset, boolean failFirstProcessing) throws Exception {
        var retained = new RetainedMessages(firstOffset, MESSAGES_COUNT, false);
        List<BigInteger> requestedOffsets = new ArrayList<>();
        List<DeferredPollOptions> requestedOptions = new ArrayList<>();
        var messages = new DeferredMessages((strategy, count, options) -> {
            requestedOffsets.add(strategy.value());
            requestedOptions.add(options);
            if (failFirstProcessing && requestedOffsets.size() == 1) {
                var message = retained.retained.get(0);
                var invalid = new Message(message.header(), null, message.userHeaders());
                return CompletableFuture.completedFuture(
                        new PolledMessages(0L, message.header().offset(), 1L, List.of(invalid)));
            }
            return CompletableFuture.completedFuture(retained.poll(strategy, count));
        });
        runConsumer(messages);
        retained.assertConsumedOnce();
        assertThat(requestedOptions)
                .as("the example asks the server to hold the poll instead of sleeping")
                .isNotEmpty()
                .allMatch(DeferredPollOptions.defaults()::equals);
        if (failFirstProcessing) {
            assertThat(requestedOffsets)
                    .as("processing failure must retry the same polling offset")
                    .startsWith(BigInteger.ZERO, BigInteger.ZERO);
        }
    }

    @Test
    void shouldNotAdvanceOnEmptyDeferredPolls() throws Exception {
        var retained = new RetainedMessages(0, MESSAGES_COUNT, false);
        List<BigInteger> requestedOffsets = new ArrayList<>();
        var emptyPolls = new AtomicInteger();
        var messages = new DeferredMessages((strategy, count, options) -> {
            requestedOffsets.add(strategy.value());
            // Two quiet replies before every batch: an empty deferred reply
            // advances nothing, so the next poll repeats the same offset.
            if (emptyPolls.incrementAndGet() % 3 != 0) {
                return CompletableFuture.completedFuture(new PolledMessages(0L, BigInteger.ZERO, 0L, List.of()));
            }
            return CompletableFuture.completedFuture(retained.poll(strategy, count));
        });
        runConsumer(messages);
        retained.assertConsumedOnce();
        assertThat(requestedOffsets).startsWith(BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO);
    }

    @Test
    void shouldStopAfterTheIdleLimitWithoutMessages() throws Exception {
        var polls = new AtomicInteger();
        // A held poll costs real time on a server, so the double spends some
        // too; otherwise the idle limit would be measured against a hot loop.
        var messages = new DeferredMessages((strategy, count, options) -> {
            polls.incrementAndGet();
            return CompletableFuture.supplyAsync(
                    () -> new PolledMessages(0L, BigInteger.ZERO, 0L, List.of()),
                    CompletableFuture.delayedExecutor(IDLE_POLL_MILLIS, TimeUnit.MILLISECONDS));
        });
        runConsumer(messages);
        assertThat(polls.get()).isGreaterThan(1);
    }

    private static void runConsumer(DeferredMessages messages) throws Exception {
        var client = new AsyncIggyTcpClient("localhost", 8090) {
            @Override
            public org.apache.iggy.client.async.MessagesClient messages() {
                return messages;
            }
        };
        ExecutorService processingPool = Executors.newSingleThreadExecutor();
        try {
            Method consume = AsyncConsumer.class.getDeclaredMethod(
                    "pollMessagesAsync", AsyncIggyTcpClient.class, ExecutorService.class);
            consume.setAccessible(true);
            var completed = (CompletableFuture<?>) consume.invoke(null, client, processingPool);
            completed.get(CONSUMER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            processingPool.shutdownNow();
            assertThat(processingPool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            client.close().get(5, TimeUnit.SECONDS);
        }
    }

    private static Stream<Arguments> blockingConsumers() {
        return Stream.of(
                        GettingStartedConsumer.class,
                        TcpTlsConsumer.class,
                        MessageHeadersConsumer.class,
                        MessageEnvelopeConsumer.class,
                        StreamBasic.class)
                .flatMap(consumer -> Stream.of(0, 25).map(firstOffset -> Arguments.of(consumer, firstOffset)));
    }

    /**
     * An async client that only answers deferred polls. An immediate poll fails
     * the test, so a consumer that stops asking the server to wait is caught.
     */
    private static final class DeferredMessages implements org.apache.iggy.client.async.MessagesClient {
        private final Reply reply;

        private DeferredMessages(Reply reply) {
            this.reply = reply;
        }

        @Override
        public CompletableFuture<PolledMessages> pollMessagesDeferred(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit,
                DeferredPollOptions options) {
            return reply.poll(strategy, count, options);
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
            throw new AssertionError("Consumer must poll with an explicit readiness wait");
        }

        @Override
        public CompletableFuture<SendMessagesResponse> sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            throw new AssertionError("Consumer must not send messages");
        }

        @FunctionalInterface
        private interface Reply {
            CompletableFuture<PolledMessages> poll(PollingStrategy strategy, Long count, DeferredPollOptions options);
        }
    }

    private static final class RetainedMessages implements MessagesClient {
        private final List<Message> retained;
        private final List<BigInteger> delivered = new ArrayList<>();

        private RetainedMessages(int firstOffset, int count, boolean envelope) {
            var order = new OrderConfirmed(1, 100.0, Instant.EPOCH);
            var template = Message.of(
                    envelope ? order.toJsonEnvelope() : order.toJson(),
                    Map.of(HeaderKey.fromString("message_type"), HeaderValue.fromString(order.getMessageType())));
            retained = IntStream.range(firstOffset, firstOffset + count)
                    .mapToObj(offset -> new Message(
                            new MessageHeader(
                                    template.header().checksum(),
                                    template.header().id(),
                                    BigInteger.valueOf(offset),
                                    template.header().timestamp(),
                                    template.header().originTimestamp(),
                                    template.header().userHeadersLength(),
                                    template.header().payloadLength(),
                                    template.header().reserved()),
                            template.payload(),
                            template.userHeaders()))
                    .toList();
        }

        @Override
        public PolledMessages pollMessages(
                StreamId streamId,
                TopicId topicId,
                Optional<Long> partitionId,
                Consumer consumer,
                PollingStrategy strategy,
                Long count,
                boolean autoCommit) {
            return poll(strategy, count);
        }

        private PolledMessages poll(PollingStrategy strategy, Long count) {
            var batch = retained.stream()
                    .filter(message -> message.header().offset().compareTo(strategy.value()) >= 0)
                    .limit(Math.min(count, MAX_BATCH_SIZE))
                    .toList();
            delivered.addAll(
                    batch.stream().map(message -> message.header().offset()).toList());
            return new PolledMessages(
                    0L, retained.get(retained.size() - 1).header().offset(), (long) batch.size(), batch);
        }

        @Override
        public SendMessagesResponse sendMessages(
                StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
            throw new AssertionError("Consumer must not send messages");
        }

        private void assertConsumedOnce() {
            assertThat(delivered)
                    .as("retained offsets delivered once, in order")
                    .containsExactlyElementsOf(retained.stream()
                            .map(message -> message.header().offset())
                            .toList());
        }
    }
}
