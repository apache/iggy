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
import org.apache.iggy.client.async.reader.IggyConsumer;
import org.apache.iggy.client.async.reader.PartitionReader;
import org.apache.iggy.client.async.reader.ReceivedMessage;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.AutoCommit;
import org.apache.iggy.message.CommitAfter;
import org.apache.iggy.message.CommitEvery;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The partition reader and the poll of one partition against a server. */
class AsyncPartitionReaderTest extends BaseIntegrationTest {
    private static final int TIMEOUT_SECONDS = 15;
    private static final long PARTITIONS = 3;
    private static final int MESSAGES_PER_PARTITION = 5;
    // More than the reads that the late message waits for when the reads take turns.
    private static final int BACKLOG_MESSAGES = 50;
    private static final PollOptions SHORT_WAIT = PollOptions.waitFor(Duration.ofSeconds(1));

    private AsyncIggyTcpClient client;
    private StreamId stream;
    private TopicId topic;

    @BeforeEach
    void setUp() throws Exception {
        client = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials("iggy", "iggy")
                .build();
        client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.login().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        String name = "reader-" + UUID.randomUUID();
        client.streams().createStream(name).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        stream = StreamId.of(name);
        client.topics().createTopic(stream, "orders", PARTITIONS).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        topic = TopicId.of("orders");
        for (long partition = 0; partition < PARTITIONS; partition++) {
            List<Message> messages = new ArrayList<>();
            for (int index = 0; index < MESSAGES_PER_PARTITION; index++) {
                messages.add(Message.of(partition + ":" + index));
            }
            client.messages()
                    .sendMessages(stream, topic, Partitioning.partitionId(partition), messages)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            client.streams().deleteStream(stream).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } finally {
            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReadEveryMessageOfEveryPartitionOnceInOffsetOrder() throws Exception {
        PartitionReader reader = client.reader(stream, topic, Consumer.of(1L))
                .partitions(List.of(0L, 1L, 2L))
                .start(PollingStrategy.first())
                .options(SHORT_WAIT)
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            Map<Long, List<String>> read = readAll(reader, PARTITIONS * MESSAGES_PER_PARTITION);

            for (long partition = 0; partition < PARTITIONS; partition++) {
                List<String> expected = new ArrayList<>();
                for (int index = 0; index < MESSAGES_PER_PARTITION; index++) {
                    expected.add(partition + ":" + index);
                }
                assertThat(read.get(partition)).containsExactlyElementsOf(expected);
            }
        } finally {
            reader.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** Under OnRead the server stores each read, and the close moves the offset back to the last delivered message. */
    @Test
    void shouldStoreWhatItDeliversUnderOnReadAndResumeFromIt() throws Exception {
        Consumer consumer = Consumer.of(2L);
        PartitionReader first = client.reader(stream, topic, consumer)
                .partitions(List.of(0L))
                .options(SHORT_WAIT.withCount(2).withCommit(CommitPolicy.onRead()))
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        PolledMessages delivered = first.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        first.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(texts(delivered)).containsExactly("0:0", "0:1");
        assertThat(storedOffset(consumer, 0)).contains(BigInteger.ONE);

        PartitionReader second = client.reader(stream, topic, consumer)
                .partitions(List.of(0L))
                .options(SHORT_WAIT.withCommit(CommitPolicy.onRead()))
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            assertThat(texts(second.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)))
                    .containsExactly("0:2", "0:3", "0:4");
        } finally {
            second.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        assertThat(storedOffset(consumer, 0)).contains(BigInteger.valueOf(4));
    }

    @Test
    void shouldStoreEachBatchAtDeliveryUnderAnAutomaticCommit() throws Exception {
        Consumer consumer = Consumer.of(3L);
        AutoCommit eachBatch = new AutoCommit(Optional.empty(), Optional.of(CommitEvery.batch()), CommitAfter.DELIVERY);
        PartitionReader reader = client.reader(stream, topic, consumer)
                .partitions(List.of(1L))
                .start(PollingStrategy.first())
                .options(SHORT_WAIT.withCount(3).withCommit(CommitPolicy.auto(eachBatch)))
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            reader.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!storedOffset(consumer, 1).equals(Optional.of(BigInteger.TWO)) && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(20);
            }
            assertThat(storedOffset(consumer, 1)).contains(BigInteger.TWO);
        } finally {
            reader.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReadTheAssignmentOfAGroupMember() throws Exception {
        ConsumerGroupDetails group = client.consumerGroups()
                .createConsumerGroup(stream, topic, "readers")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        PartitionReader reader = client.reader(stream, topic, Consumer.group(ConsumerId.of(group.id())))
                .options(SHORT_WAIT.withCommit(CommitPolicy.onRead()))
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            Map<Long, List<String>> read = readAll(reader, PARTITIONS * MESSAGES_PER_PARTITION);

            assertThat(read.keySet()).containsExactlyInAnyOrder(0L, 1L, 2L);
            assertThat(reader.partitions()).containsExactlyInAnyOrder(0L, 1L, 2L);
        } finally {
            reader.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** A group consumer stores what its handler processed, and the next member resumes after it. */
    @Test
    void shouldResumeAConsumerGroupAfterWhatItsHandlerProcessed() throws Exception {
        ConsumerGroupDetails group = client.consumerGroups()
                .createConsumerGroup(stream, topic, "workers")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Consumer member = Consumer.group(ConsumerId.of(group.id()));
        AutoCommit afterProcessing =
                new AutoCommit(Optional.empty(), Optional.of(CommitEvery.message()), CommitAfter.PROCESSING);
        PollOptions options = SHORT_WAIT.withCommit(CommitPolicy.auto(afterProcessing));
        IggyConsumer first = IggyConsumer.build(
                        client.reader(stream, topic, member).options(options))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        List<String> handled = new ArrayList<>();
        CompletableFuture<Void> enough = new CompletableFuture<>();
        first.consume(message -> {
            handled.add(new String(message.message().payload(), StandardCharsets.UTF_8));
            if (handled.size() == 6) {
                enough.complete(null);
                return CompletableFuture.failedFuture(new IllegalStateException("stop after six"));
            }
            return CompletableFuture.completedFuture(null);
        });
        enough.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        first.shutdown().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        IggyConsumer second = IggyConsumer.build(
                        client.reader(stream, topic, member).options(options))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        List<String> rest = new ArrayList<>();
        try {
            while (rest.size() < PARTITIONS * MESSAGES_PER_PARTITION - 5) {
                ReceivedMessage message = second.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                rest.add(new String(message.message().payload(), StandardCharsets.UTF_8));
            }
        } finally {
            second.shutdown().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        List<String> all = new ArrayList<>(handled.subList(0, 5));
        all.addAll(rest);
        assertThat(all)
                .as("the sixth message failed its handler, so the next member reads it again")
                .hasSize((int) (PARTITIONS * MESSAGES_PER_PARTITION))
                .doesNotHaveDuplicates()
                .contains(handled.get(5));
    }

    /** A partition with a backlog holds messages at every read, and another one that gets a message is read too. */
    @Test
    void shouldReadAPartitionThatGetsAMessageWhileAnotherKeepsABacklog() throws Exception {
        List<Message> backlog = new ArrayList<>();
        for (int index = 0; index < BACKLOG_MESSAGES; index++) {
            backlog.add(Message.of("backlog"));
        }
        client.messages()
                .sendMessages(stream, topic, Partitioning.partitionId(0L), backlog)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        PartitionReader reader = client.reader(stream, topic, Consumer.of(6L))
                .partitions(List.of(0L, 2L))
                .start(PollingStrategy.offset(BigInteger.valueOf(MESSAGES_PER_PARTITION)))
                .options(SHORT_WAIT.withCount(1))
                .prefetchMessages(1)
                .build()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            assertThat(reader.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).partitionId())
                    .isZero();
            client.messages()
                    .sendMessages(stream, topic, Partitioning.partitionId(2L), List.of(Message.of("late")))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            List<String> late = List.of();
            for (int read = 0; read < BACKLOG_MESSAGES && late.isEmpty(); read++) {
                PolledMessages batch = reader.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (batch.partitionId() == 2L) {
                    late = texts(batch);
                }
            }

            assertThat(late).containsExactly("late");
        } finally {
            reader.closeAsync().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** The server returns a first message larger than the byte limit alone. */
    @Test
    void shouldPollAFirstMessageLargerThanTheByteLimit() throws Exception {
        byte[] large = new byte[4096];
        client.messages()
                .sendMessages(
                        stream,
                        topic,
                        Partitioning.partitionId(2L),
                        List.of(Message.of(new String(large, StandardCharsets.UTF_8))))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        PolledMessages polled = client.messages()
                .pollMessages(
                        stream,
                        topic,
                        2L,
                        Consumer.of(4L),
                        PollingStrategy.offset(BigInteger.valueOf(MESSAGES_PER_PARTITION)),
                        PollOptions.defaults().withMaxBytes(DeferredPollOptions.MIN_MAX_BYTES))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(polled.messages()).hasSize(1);
        assertThat(polled.messages().get(0).payload()).hasSize(large.length);
    }

    @Test
    void shouldStoreTheOffsetOfAPollWithOnRead() throws Exception {
        Consumer consumer = Consumer.of(5L);

        PolledMessages polled = client.messages()
                .pollMessages(
                        stream,
                        topic,
                        0L,
                        consumer,
                        PollingStrategy.next(),
                        PollOptions.defaults().withCount(2).withCommit(CommitPolicy.onRead()))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(texts(polled)).containsExactly("0:0", "0:1");
        assertThat(storedOffset(consumer, 0)).contains(BigInteger.ONE);
        assertThat(polled.continuation()).isPresent();
    }

    private Map<Long, List<String>> readAll(PartitionReader reader, long messages) throws Exception {
        Map<Long, List<String>> read = new HashMap<>();
        long count = 0;
        while (count < messages) {
            PolledMessages batch = reader.next().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            read.computeIfAbsent(batch.partitionId(), partition -> new ArrayList<>())
                    .addAll(texts(batch));
            count += batch.messages().size();
        }
        return read;
    }

    private Optional<BigInteger> storedOffset(Consumer consumer, long partition) throws Exception {
        return client.consumerOffsets()
                .getConsumerOffset(stream, topic, Optional.of(partition), consumer)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .map(info -> info.storedOffset());
    }

    private static List<String> texts(PolledMessages batch) {
        return batch.messages().stream()
                .map(message -> new String(message.payload(), StandardCharsets.UTF_8))
                .toList();
    }
}
