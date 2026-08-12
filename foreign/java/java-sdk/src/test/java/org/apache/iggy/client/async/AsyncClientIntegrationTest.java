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

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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
                            Optional.empty(),
                            "test-topic")
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
                            Optional.empty(),
                            "test-topic")
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
                            Optional.empty(),
                            "test-topic")
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // when — send messages in concurrent batches
            List<CompletableFuture<Void>> sendFutures = new ArrayList<>();
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
}
