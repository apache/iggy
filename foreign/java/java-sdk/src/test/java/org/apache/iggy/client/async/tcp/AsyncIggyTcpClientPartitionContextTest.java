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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_POLL_ROUTING_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.clusterMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.partitionContext;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.poll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;

class AsyncIggyTcpClientPartitionContextTest {
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int OPERATION_STORE_CONSUMER_OFFSET = 161;
    private static final int GET_SEND_CONTEXT_CODE = 105;
    private static final int GET_OFFSET_ROUTING_CODE = 123;
    private static final int HISTORY_UNAVAILABLE = 87;

    @Test
    void shouldKeepCoordinatorWhenAPlainPollIsCanceledDuringContextDiscovery() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger polls = new AtomicInteger();
            CompletableFuture<Void> routing = new CompletableFuture<>();
            CompletableFuture<Void> releaseRoute = new CompletableFuture<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    routing.complete(null);
                    releaseRoute.join();
                    return Response.success(
                            OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, coordinatorSocket.getLocalPort()));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    polls.incrementAndGet();
                    return Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                CompletableFuture<PolledMessages> canceled = poll(client, Optional.of(0L), false);
                routing.get(5, TimeUnit.SECONDS);
                assertThat(canceled.cancel(false)).isTrue();
                releaseRoute.complete(null);
                poll(client, Optional.of(0L), false).get(5, TimeUnit.SECONDS);
                assertThat(polls).hasValue(1);
            } finally {
                releaseRoute.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldSendOnTheLeaderThatAnsweredAMovedContextDiscovery() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket oldLeaderSocket = new ServerSocket(0, 1, loopback);
                ServerSocket newLeaderSocket = new ServerSocket(0, 1, loopback)) {
            int oldPort = oldLeaderSocket.getLocalPort();
            int newPort = newLeaderSocket.getLocalPort();
            AtomicInteger denials = new AtomicInteger();
            AtomicInteger sends = new AtomicInteger();
            CompletableFuture<Void> oldLeader = serve(oldLeaderSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            clusterMetadata(oldPort, newPort, denials.get() > 0 ? newPort : oldPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_SEND_CONTEXT_CODE, OPERATION_NON_REPLICATED)) {
                    denials.incrementAndGet();
                    return Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
                }
                throw new IllegalStateException("Unexpected request to old leader: " + request);
            });
            CompletableFuture<Void> newLeader = serve(newLeaderSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, clusterMetadata(oldPort, newPort, newPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                if (request.is(GET_SEND_CONTEXT_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, partitionContext(Unpooled.buffer(), 1));
                }
                if (request.operation() == OPERATION_SEND_MESSAGES) {
                    sends.incrementAndGet();
                    return Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
                }
                throw new IllegalStateException("Unexpected request to new leader: " + request);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(oldPort)
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(10))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                send(client).get(10, TimeUnit.SECONDS);
                assertThat(client.getConnectionInfo().port()).isEqualTo(newPort);
                assertThat(denials).hasValueGreaterThan(1);
                assertThat(sends).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            oldLeader.get(5, TimeUnit.SECONDS);
            newLeader.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReuseTheSendContextAndRefreshItOnceAfterAnIncarnationChange() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicLong incarnation = new AtomicLong(1);
            AtomicLong discovered = new AtomicLong(1);
            AtomicInteger discoveries = new AtomicInteger();
            List<Long> sentIncarnations = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_SEND_CONTEXT_CODE, OPERATION_NON_REPLICATED)) {
                    discoveries.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED, partitionContext(Unpooled.buffer(), discovered.get()));
                }
                if (request.operation() == OPERATION_SEND_MESSAGES) {
                    sentIncarnations.add(request.incarnation());
                    return request.incarnation() == incarnation.get()
                            ? Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER)
                            : Response.error(OPERATION_SEND_MESSAGES, HISTORY_UNAVAILABLE);
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                send(client).get(5, TimeUnit.SECONDS);
                send(client).get(5, TimeUnit.SECONDS);
                assertThat(discoveries).hasValue(1);

                incarnation.set(2);
                discovered.set(2);
                send(client).get(5, TimeUnit.SECONDS);
                assertThat(discoveries).hasValue(2);

                incarnation.set(3);
                assertServerError(send(client), HISTORY_UNAVAILABLE);
                assertThat(discoveries).hasValue(3);
                assertThat(sentIncarnations).containsExactly(1L, 1L, 1L, 2L, 2L, 2L);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldStoreAnOffsetWithTheCachedContextOrTheContextItWasPolledWith() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicLong incarnation = new AtomicLong(1);
            AtomicInteger discoveries = new AtomicInteger();
            List<Long> storedIncarnations = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_OFFSET_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    discoveries.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, coordinatorSocket.getLocalPort(), incarnation.get()));
                }
                if (request.operation() == OPERATION_STORE_CONSUMER_OFFSET) {
                    storedIncarnations.add(request.incarnation());
                    return request.incarnation() == incarnation.get()
                            ? Response.success(
                                    OPERATION_STORE_CONSUMER_OFFSET,
                                    Unpooled.buffer().writeIntLE(0))
                            : Response.error(OPERATION_STORE_CONSUMER_OFFSET, HISTORY_UNAVAILABLE);
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                storeOffset(client, 1).get(5, TimeUnit.SECONDS);
                storeOffset(client, 2).get(5, TimeUnit.SECONDS);
                assertThat(discoveries).hasValue(1);

                incarnation.set(2);
                var polledBeforeTheChange = new PartitionContext(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO);
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(
                                        StreamId.of(1L),
                                        TopicId.of(1L),
                                        0L,
                                        Consumer.of(7L),
                                        BigInteger.valueOf(3),
                                        polledBeforeTheChange),
                        HISTORY_UNAVAILABLE);
                assertServerError(storeOffset(client, 4), HISTORY_UNAVAILABLE);
                assertThat(discoveries).hasValue(1);
                storeOffset(client, 5).get(5, TimeUnit.SECONDS);
                assertThat(discoveries).hasValue(2);
                assertThat(storedIncarnations).containsExactly(1L, 1L, 1L, 1L, 2L);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<SendMessagesResponse> send(AsyncIggyTcpClient client) {
        return client.messages()
                .sendMessages(StreamId.of(1L), TopicId.of(1L), Partitioning.partitionId(0L), List.of(Message.of("m")));
    }

    private static CompletableFuture<Void> storeOffset(AsyncIggyTcpClient client, long offset) {
        return client.consumerOffsets()
                .storeConsumerOffset(
                        StreamId.of(1L), TopicId.of(1L), Optional.of(0L), Consumer.of(7L), BigInteger.valueOf(offset));
    }
}
