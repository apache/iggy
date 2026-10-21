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

package org.apache.iggy.client.async;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.group.ChannelGroup;
import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iggy.TestConstants.PERSISTED_TOPIC_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Async-specific integration tests that exercise concurrency and CompletableFuture patterns.
 *
 * <p>Basic CRUD operations (streams, topics, messages, users, consumer groups, partitions,
 * consumer offsets, personal access tokens, system) are covered by the blocking test suite
 * which exercises the same async code path via the blocking wrapper.
 */
public class AsyncClientIntegrationTest extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(AsyncClientIntegrationTest.class);

    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";
    private static final int TIMEOUT_SECONDS = 5;
    private static final int PAUSED_READER_REQUESTS = 8;
    private static final int REQUEST_COMMAND_CODE_OFFSET = 196;
    private static final int LARGE_REPLY_BYTES = 8 * 1024 * 1024;
    private static final int RECEIVE_BUFFER_BYTES = 64 * 1024;
    private static final int PAUSED_READER_TIMEOUT_SECONDS = 30;

    private AsyncIggyTcpClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.users().login(USERNAME, PASSWORD).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldPollMessagesAndVerifyContent() throws Exception {
        // given
        String streamName = "poll-content-test-" + UUID.randomUUID();
        StreamId streamId = StreamId.of(streamName);
        TopicId topicId = TopicId.of("test-topic");
        long partitionId = 1L;

        try {
            client.streams().createStream(streamName).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.topics()
                    .createTopic(
                            streamId,
                            2L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            "test-topic",
                            PERSISTED_TOPIC_OPTIONS)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                messages.add(Message.of(String.format("Test message %d", i)));
            }
            client.messages()
                    .sendMessages(streamId, topicId, Partitioning.partitionId(partitionId), messages)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when
            var polledMessages = client.messages()
                    .pollMessages(
                            streamId,
                            topicId,
                            Optional.of(partitionId),
                            Consumer.of(12345L),
                            PollingStrategy.offset(BigInteger.ZERO),
                            10L,
                            false)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // then
            assertThat(polledMessages).isNotNull();
            assertThat(polledMessages.partitionId()).isEqualTo(partitionId);
            assertThat(polledMessages.messages()).hasSize(10);

            for (int i = 0; i < polledMessages.messages().size(); i++) {
                String content = new String(polledMessages.messages().get(i).payload());
                assertThat(content).isEqualTo(String.format("Test message %d", i));
            }
        } finally {
            client.streams().deleteStream(streamId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldHandleConcurrentSendsAndPolls() throws Exception {
        // given
        String streamName = "concurrent-test-" + UUID.randomUUID();
        StreamId streamId = StreamId.of(streamName);
        TopicId topicId = TopicId.of("test-topic");
        long partitionId = 1L;

        try {
            client.streams().createStream(streamName).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.topics()
                    .createTopic(
                            streamId,
                            2L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            "test-topic",
                            PERSISTED_TOPIC_OPTIONS)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when — send messages concurrently from multiple threads
            List<CompletableFuture<?>> operations = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                final int threadNum = i;
                var future = CompletableFuture.supplyAsync(() -> {
                            List<Message> messages = new ArrayList<>();
                            for (int j = 0; j < 20; j++) {
                                messages.add(Message.of(String.format("Thread %d - Message %d", threadNum, j)));
                            }
                            return messages;
                        })
                        .thenCompose(messages -> client.messages()
                                .sendMessages(streamId, topicId, Partitioning.partitionId(partitionId), messages));
                operations.add(future);
            }

            // poll concurrently while sends are in progress
            for (int i = 0; i < 3; i++) {
                final long consumerId = 10000L + i;
                var future = client.messages()
                        .pollMessages(
                                streamId,
                                topicId,
                                Optional.of(partitionId),
                                Consumer.of(consumerId),
                                PollingStrategy.last(),
                                10L,
                                false);
                operations.add(future);
            }

            // then — all operations should complete without errors
            CompletableFuture.allOf(operations.toArray(new CompletableFuture[0]))
                    .get(15, TimeUnit.SECONDS);

            log.info("Completed {} concurrent operations", operations.size());
        } finally {
            client.streams().deleteStream(streamId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldSendAndPollLargeVolume() throws Exception {
        // given
        String streamName = "volume-test-" + UUID.randomUUID();
        StreamId streamId = StreamId.of(streamName);
        TopicId topicId = TopicId.of("test-topic");
        long partitionId = 1L;
        int messageCount = 100;

        try {
            client.streams().createStream(streamName).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.topics()
                    .createTopic(
                            streamId,
                            2L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            "test-topic",
                            PERSISTED_TOPIC_OPTIONS)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when — send messages in concurrent batches
            List<CompletableFuture<SendMessagesResponse>> sendFutures = new ArrayList<>();
            for (int batch = 0; batch < 10; batch++) {
                List<Message> batchMessages = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    int msgNum = batch * 10 + i;
                    batchMessages.add(Message.of(String.format("Batch message %d", msgNum)));
                }
                sendFutures.add(client.messages()
                        .sendMessages(streamId, topicId, Partitioning.partitionId(partitionId), batchMessages));
            }
            CompletableFuture.allOf(sendFutures.toArray(new CompletableFuture[0]))
                    .get(10, TimeUnit.SECONDS);

            // then — poll all messages back
            var polledMessages = client.messages()
                    .pollMessages(
                            streamId,
                            topicId,
                            Optional.of(partitionId),
                            Consumer.of(1L),
                            PollingStrategy.offset(BigInteger.ZERO),
                            (long) messageCount + 10,
                            false)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(polledMessages).isNotNull();
            assertThat(polledMessages.messages().size()).isGreaterThanOrEqualTo(messageCount);
            log.info(
                    "Sent {} messages in batches, polled {} back",
                    messageCount,
                    polledMessages.messages().size());
        } finally {
            client.streams().deleteStream(streamId).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** Requires the external server fixture to use a two-slot reply queue. */
    @Test
    @EnabledIfEnvironmentVariable(named = "IGGY_MESSAGE_BUS_CLIENT_QUEUE_CAPACITY", matches = "2")
    void shouldCompletePipelinedPollsAfterTheReaderResumes() throws Exception {
        client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .requestTimeout(Duration.ofSeconds(PAUSED_READER_TIMEOUT_SECONDS))
                .heartbeatInterval(Duration.ofMinutes(1))
                .build();
        client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.users().login(USERNAME, PASSWORD).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        String streamName = "paused-reader-" + UUID.randomUUID();
        StreamId stream = StreamId.of(streamName);
        TopicId topic = TopicId.of("large-replies");
        List<Channel> paused = List.of();
        try {
            client.streams().createStream(streamName).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.topics()
                    .createTopic(
                            stream,
                            1L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            "large-replies",
                            PERSISTED_TOPIC_OPTIONS)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (int index = 0; index < PAUSED_READER_REQUESTS; index++) {
                String payload = String.valueOf((char) ('a' + index)).repeat(LARGE_REPLY_BYTES);
                client.messages()
                        .sendMessages(stream, topic, Partitioning.partitionId(0L), List.of(Message.of(payload)))
                        .get(PAUSED_READER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            pollAt(stream, topic, 0).get(PAUSED_READER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            paused = clientChannels(client);
            assertThat(paused).isNotEmpty();
            CompletableFuture<Void> written = pauseReads(paused);
            List<CompletableFuture<PolledMessages>> polls = new ArrayList<>();
            for (int index = 0; index < PAUSED_READER_REQUESTS; index++) {
                polls.add(pollAt(stream, topic, index));
            }
            written.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(polls).allSatisfy(poll -> assertThat(poll).isNotDone());
            var observer = AsyncIggyTcpClient.builder()
                    .host(serverHost())
                    .port(serverTcpPort())
                    .build();
            try {
                observer.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                observer.users().login(USERNAME, PASSWORD).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                observer.system().getStats().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                observer.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            for (Channel channel : paused) {
                channel.eventLoop()
                        .submit(() -> channel.config().setAutoRead(true))
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            CompletableFuture.allOf(polls.toArray(new CompletableFuture<?>[0]))
                    .get(PAUSED_READER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (int index = 0; index < polls.size(); index++) {
                byte[] payload = polls.get(index).join().messages().get(0).payload();
                assertThat(payload).hasSize(LARGE_REPLY_BYTES);
                assertThat(payload[0]).isEqualTo((byte) ('a' + index));
            }
        } finally {
            for (Channel channel : paused) {
                channel.eventLoop()
                        .submit(() -> channel.config().setAutoRead(true))
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            client.streams().deleteStream(stream).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<Void> pauseReads(List<Channel> channels) throws Exception {
        CompletableFuture<Void> written = new CompletableFuture<>();
        AtomicInteger requests = new AtomicInteger();
        for (Channel channel : channels) {
            channel.eventLoop()
                    .submit(() -> {
                        channel.config().setOption(ChannelOption.SO_RCVBUF, RECEIVE_BUFFER_BYTES);
                        channel.config().setAutoRead(false);
                        channel.pipeline().addFirst("count-pipelined-polls", new ChannelOutboundHandlerAdapter() {
                            @Override
                            public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) {
                                if (message instanceof ByteBuf frame
                                        && frame.getUnsignedIntLE(frame.readerIndex() + REQUEST_COMMAND_CODE_OFFSET)
                                                == CommandCode.Messages.POLL.getValue()) {
                                    promise.addListener(done -> {
                                        if (done.isSuccess() && requests.incrementAndGet() == PAUSED_READER_REQUESTS) {
                                            written.complete(null);
                                        }
                                    });
                                }
                                context.write(message, promise);
                            }
                        });
                    })
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        return written;
    }

    private CompletableFuture<PolledMessages> pollAt(StreamId stream, TopicId topic, int offset) {
        return client.messages()
                .pollMessages(
                        stream,
                        topic,
                        Optional.of(0L),
                        Consumer.of(0L),
                        PollingStrategy.offset(BigInteger.valueOf(offset)),
                        1L,
                        false);
    }

    // Test-side access covers learned replica links without adding a production pause hook.
    public static List<Channel> clientChannels(AsyncIggyTcpClient client) throws ReflectiveOperationException {
        List<Object> connections = new ArrayList<>();
        connections.add(((AtomicReference<?>) field(client, "connection")).get());
        Map<?, ?> slots = (Map<?, ?>) field(field(client, "pollRouter"), "connections");
        for (Object slot : slots.values()) {
            Object connection = field(slot, "connection");
            if (connection != null) {
                connections.add(connection);
            }
        }
        var channels = new LinkedHashSet<Channel>();
        for (Object connection : connections) {
            channels.addAll((ChannelGroup) field(connection, "channels"));
        }
        return List.copyOf(channels);
    }

    public static Object field(Object target, String name) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
