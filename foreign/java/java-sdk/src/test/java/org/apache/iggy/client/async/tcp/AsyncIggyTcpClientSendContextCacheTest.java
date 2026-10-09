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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.partitionContext;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The send-context cache follows the Rust SDK's ConsumerGroupClientState:
 * only a history refusal drops contexts, those of the refused topic, and this
 * client's own stream, topic and partition changes drop them all.
 */
class AsyncIggyTcpClientSendContextCacheTest {
    private static final int GET_SEND_CONTEXT_CODE = 105;
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int OPERATION_DELETE_STREAM = 130;
    private static final int OPERATION_DELETE_TOPIC = 134;
    private static final int OPERATION_CREATE_PARTITIONS = 136;
    private static final int OPERATION_DELETE_PARTITIONS = 137;
    private static final int HISTORY_UNAVAILABLE = 87;
    private static final int TOO_BIG_MESSAGE_PAYLOAD = 4022;
    private static final int ROUTE_CACHE_LIMIT = 4096;
    // Numeric identifiers encode as kind, length and a u32 value, so the topic
    // id sits at a fixed offset in both request bodies.
    private static final int SEND_CONTEXT_TOPIC_OFFSET = 8;
    private static final int SEND_MESSAGES_TOPIC_OFFSET = 12;
    private static final StreamId STREAM = StreamId.of(1L);

    @Test
    void shouldKeepTheSendContextAfterAFailureOtherThanHistoryUnavailable() throws Exception {
        AtomicBoolean refuseNextSend = new AtomicBoolean();
        assertSendContexts(refuseNextSend, (client, fake) -> {
            send(client, 1, 0).get(5, TimeUnit.SECONDS);
            refuseNextSend.set(true);
            assertServerError(send(client, 1, 0), TOO_BIG_MESSAGE_PAYLOAD);
            send(client, 1, 0).get(5, TimeUnit.SECONDS);

            assertThat(fake.discoveries).hasValue(1);
            assertThat(fake.sentIncarnations).containsExactly(1L, 1L, 1L);
        });
    }

    @Test
    void shouldDropEveryContextOfTheRefusedTopicAfterHistoryUnavailable() throws Exception {
        assertSendContexts(new AtomicBoolean(), (client, fake) -> {
            send(client, 1, 0).get(5, TimeUnit.SECONDS);
            send(client, 1, 1).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);
            assertThat(fake.discoveries).hasValue(3);

            fake.incarnations.put(1L, 2L);
            send(client, 1, 0).get(5, TimeUnit.SECONDS);
            send(client, 1, 1).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);

            assertThat(fake.historyRefusals).hasValue(1);
            assertThat(fake.discoveries).hasValue(5);
            assertThat(fake.sentIncarnations).containsExactly(1L, 1L, 1L, 1L, 2L, 2L, 1L);
        });
    }

    @Test
    void shouldDropEverySendContextAfterThisClientChangesStreamsTopicsOrPartitions() throws Exception {
        assertSendContexts(new AtomicBoolean(), (client, fake) -> {
            send(client, 1, 0).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);
            client.streams().deleteStream(StreamId.of(9L)).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);
            client.topics().deleteTopic(STREAM, TopicId.of(1L)).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);
            client.partitions().createPartitions(STREAM, TopicId.of(1L), 1L).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);
            client.partitions().deletePartitions(STREAM, TopicId.of(1L), 1L).get(5, TimeUnit.SECONDS);
            send(client, 2, 0).get(5, TimeUnit.SECONDS);

            assertThat(fake.discoveries).hasValue(6);
        });
    }

    @Test
    void shouldNotClearTheSendContextCacheWhenItOutgrowsTheRouteCacheLimit() {
        // Thousands of round trips through a fake server take longer than the
        // client's heartbeat interval, so this runs in memory.
        InMemoryConnection connection = new InMemoryConnection();
        try {
            MessagesTcpClient messages = new MessagesTcpClient(() -> connection);
            for (long partition = 0; partition <= ROUTE_CACHE_LIMIT; partition++) {
                messages.sendMessages(
                                STREAM, TopicId.of(1L), Partitioning.partitionId(partition), List.of(Message.of("m")))
                        .join();
            }
            messages.sendMessages(STREAM, TopicId.of(1L), Partitioning.partitionId(0L), List.of(Message.of("m")))
                    .join();

            assertThat(connection.discoveries).hasValue(ROUTE_CACHE_LIMIT + 1);
        } finally {
            connection.close().join();
        }
    }

    private static void assertSendContexts(AtomicBoolean refuseNextSend, Scenario scenario) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            FakeTopics fake = new FakeTopics();
            CompletableFuture<Void> server = serve(serverSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.is(GET_SEND_CONTEXT_CODE, OPERATION_NON_REPLICATED)) {
                    fake.discoveries.incrementAndGet();
                    long incarnation = fake.incarnation(request, SEND_CONTEXT_TOPIC_OFFSET);
                    return Response.success(OPERATION_NON_REPLICATED, partitionContext(Unpooled.buffer(), incarnation));
                }
                if (request.operation() == OPERATION_SEND_MESSAGES) {
                    fake.sentIncarnations.add(request.incarnation());
                    if (refuseNextSend.getAndSet(false)) {
                        return Response.error(OPERATION_SEND_MESSAGES, TOO_BIG_MESSAGE_PAYLOAD);
                    }
                    if (request.incarnation() != fake.incarnation(request, SEND_MESSAGES_TOPIC_OFFSET)) {
                        fake.historyRefusals.incrementAndGet();
                        return Response.error(OPERATION_SEND_MESSAGES, HISTORY_UNAVAILABLE);
                    }
                    return Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
                }
                if (List.of(
                                OPERATION_DELETE_STREAM,
                                OPERATION_DELETE_TOPIC,
                                OPERATION_CREATE_PARTITIONS,
                                OPERATION_DELETE_PARTITIONS)
                        .contains(request.operation())) {
                    return Response.success(
                            request.operation(), Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(serverSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                scenario.run(client, fake);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<SendMessagesResponse> send(AsyncIggyTcpClient client, long topic, long partition) {
        return client.messages()
                .sendMessages(STREAM, TopicId.of(topic), Partitioning.partitionId(partition), List.of(Message.of("m")));
    }

    /** Partition incarnations the fake server holds, one per topic. */
    private static final class FakeTopics {
        private final Map<Long, Long> incarnations = new ConcurrentHashMap<>();
        private final AtomicInteger discoveries = new AtomicInteger();
        private final AtomicInteger historyRefusals = new AtomicInteger();
        private final List<Long> sentIncarnations = new CopyOnWriteArrayList<>();

        private long incarnation(Request request, int topicOffset) {
            long topic = ByteBuffer.wrap(request.body())
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt(topicOffset);
            return incarnations.getOrDefault(topic, 1L);
        }
    }

    /** Answers every send-context discovery with a context and every other request with an empty reply. */
    private static final class InMemoryConnection extends AsyncTcpConnection {
        private final AtomicInteger discoveries = new AtomicInteger();

        InMemoryConnection() {
            super(
                    InetAddress.getLoopbackAddress().getHostAddress(),
                    1,
                    false,
                    Optional.empty(),
                    new TcpConnectionPoolConfig(),
                    Optional.empty());
        }

        @Override
        CompletableFuture<ByteBuf> send(
                int commandCode, ByteBuf payload, long requestDeadlineNanos, TransientFailoverState failoverState) {
            payload.release();
            if (commandCode != GET_SEND_CONTEXT_CODE) {
                return CompletableFuture.completedFuture(Unpooled.EMPTY_BUFFER);
            }
            discoveries.incrementAndGet();
            return CompletableFuture.completedFuture(partitionContext(Unpooled.buffer(), 1));
        }
    }

    @FunctionalInterface
    private interface Scenario {
        void run(AsyncIggyTcpClient client, FakeTopics fake) throws Exception;
    }
}
