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

package org.apache.iggy.connector.flink.source;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.connector.serialization.StringDeserializationSchema;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class IggySourceRecoveryTest {
    private static final List<String> INPUT =
            IntStream.range(0, 10).mapToObj(index -> "record-" + index).toList();

    @Test
    void uninterruptedReaderEmitsEveryRecord() throws Exception {
        Broker broker = new Broker();
        List<String> output = new ArrayList<>();
        try (IggySourceReader<String> reader = reader(broker, initialSplit())) {
            drain(reader, output);
        }
        assertThat(output).containsExactlyElementsOf(INPUT);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "IGGY_FLINK_RECOVERY_REPRO", matches = "true")
    void checkpointWithBufferedRecordsMustNotSkipThemOnRestore() throws Exception {
        Broker broker = new Broker();
        List<String> output = new ArrayList<>();
        IggySourceSplit checkpoint;
        try (IggySourceReader<String> reader = reader(broker, initialSplit())) {
            reader.pollNext(collectingOutput(output));
            assertThat(output).containsExactly("record-0");
            checkpoint = serializedCheckpoint(reader);
        }
        try (IggySourceReader<String> restored = reader(broker, checkpoint)) {
            drain(restored, output);
        }
        assertThat(output).containsExactlyElementsOf(INPUT);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "IGGY_FLINK_RECOVERY_REPRO", matches = "true")
    void restoreMustReplayRecordsFetchedAfterTheLastCheckpoint() throws Exception {
        Broker broker = new Broker();
        List<String> uncheckpointedOutput = new ArrayList<>();
        IggySourceSplit checkpoint;
        try (IggySourceReader<String> reader = reader(broker, initialSplit())) {
            checkpoint = serializedCheckpoint(reader);
            reader.pollNext(collectingOutput(uncheckpointedOutput));
            assertThat(uncheckpointedOutput).containsExactly("record-0");
        }
        List<String> restoredOutput = new ArrayList<>();
        try (IggySourceReader<String> restored = reader(broker, checkpoint)) {
            drain(restored, restoredOutput);
        }
        assertThat(restoredOutput).containsExactlyElementsOf(INPUT);
    }

    private static IggySourceSplit initialSplit() {
        return IggySourceSplit.create("1", "1", 1, 0);
    }

    private static IggySourceReader<String> reader(Broker broker, IggySourceSplit split) {
        IggySourceReader<String> reader = new IggySourceReader<>(
                null, new StubClient(broker), new StringDeserializationSchema(), Consumer.group(1L), 10L);
        reader.addSplits(List.of(split));
        reader.start();
        return reader;
    }

    private static IggySourceSplit serializedCheckpoint(IggySourceReader<String> reader) throws Exception {
        IggySourceSplitSerializer serializer = new IggySourceSplitSerializer();
        IggySourceSplit split = reader.snapshotState(1L).get(0);
        return serializer.deserialize(serializer.getVersion(), serializer.serialize(split));
    }

    private static void drain(IggySourceReader<String> reader, List<String> output) throws Exception {
        for (int attempt = 0; attempt < INPUT.size() + 2; attempt++) {
            reader.pollNext(collectingOutput(output));
        }
    }

    @SuppressWarnings("unchecked")
    private static ReaderOutput<String> collectingOutput(List<String> output) {
        return (ReaderOutput<String>) Proxy.newProxyInstance(
                ReaderOutput.class.getClassLoader(),
                new Class<?>[] {ReaderOutput.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("collect")) {
                        output.add((String) arguments[0]);
                        return null;
                    }
                    throw new AssertionError("Unexpected output call: " + method);
                });
    }

    private static final class Broker {
        private int nextOffset;

        private PolledMessages poll(PollingStrategy strategy, long count, boolean autoCommit) {
            int start = strategy.kind() == PollingKind.Offset ? strategy.value().intValueExact() : nextOffset;
            List<Message> messages = new ArrayList<>();
            int end = Math.min(INPUT.size(), start + Math.toIntExact(count));
            for (int offset = start; offset < end; offset++) {
                Message original = Message.of(INPUT.get(offset));
                MessageHeader header = original.header();
                messages.add(new Message(
                        new MessageHeader(
                                header.checksum(),
                                header.id(),
                                BigInteger.valueOf(offset),
                                header.timestamp(),
                                header.originTimestamp(),
                                header.userHeadersLength(),
                                header.payloadLength(),
                                header.reserved()),
                        original.payload(),
                        original.userHeaders()));
            }
            if (autoCommit && !messages.isEmpty()) {
                nextOffset = end;
            }
            return new PolledMessages(1L, BigInteger.valueOf(INPUT.size() - 1), (long) messages.size(), messages);
        }
    }

    private static final class StubClient extends AsyncIggyTcpClient {
        private final Broker broker;
        private boolean joined;

        private StubClient(Broker broker) {
            super("localhost", 8090);
            this.broker = broker;
        }

        @Override
        public MessagesClient messages() {
            return (MessagesClient) Proxy.newProxyInstance(
                    MessagesClient.class.getClassLoader(),
                    new Class<?>[] {MessagesClient.class},
                    (proxy, method, arguments) -> {
                        if (!method.getName().equals("pollMessages")) {
                            throw new AssertionError("Unexpected client call: " + method);
                        }
                        assertThat(joined).isTrue();
                        assertThat(((StreamId) arguments[0]).getId()).isEqualTo(1L);
                        assertThat(((TopicId) arguments[1]).getId()).isEqualTo(1L);
                        assertThat(arguments[2]).isEqualTo(Optional.of(1L));
                        Consumer consumer = (Consumer) arguments[3];
                        assertThat(consumer.kind()).isEqualTo(Consumer.Kind.ConsumerGroup);
                        assertThat(consumer.id().getId()).isEqualTo(1L);
                        return CompletableFuture.completedFuture(broker.poll(
                                (PollingStrategy) arguments[4], (Long) arguments[5], (Boolean) arguments[6]));
                    });
        }

        @Override
        public ConsumerGroupsClient consumerGroups() {
            return (ConsumerGroupsClient) Proxy.newProxyInstance(
                    ConsumerGroupsClient.class.getClassLoader(),
                    new Class<?>[] {ConsumerGroupsClient.class},
                    (proxy, method, arguments) -> {
                        if (!method.getName().equals("joinConsumerGroup")) {
                            throw new AssertionError("Unexpected group call: " + method);
                        }
                        assertThat(((StreamId) arguments[0]).getId()).isEqualTo(1L);
                        assertThat(((TopicId) arguments[1]).getId()).isEqualTo(1L);
                        assertThat(((ConsumerId) arguments[2]).getId()).isEqualTo(1L);
                        joined = true;
                        return CompletableFuture.completedFuture(null);
                    });
        }

        @Override
        public CompletableFuture<Void> close() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
