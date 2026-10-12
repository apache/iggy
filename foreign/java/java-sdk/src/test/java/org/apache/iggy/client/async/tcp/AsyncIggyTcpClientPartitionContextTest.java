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

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientRawRequestTest.offsetTarget;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientRawRequestTest.rawDeleteOffset;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientRawRequestTest.rawStoreOffset;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_POLL_ROUTING_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GROUP_SYNC_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_ON_PRIMARY_CODE;
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
    private static final int OPERATION_DELETE_CONSUMER_OFFSET = 162;
    private static final int STORE_OFFSET_CODE = 121;
    private static final int DELETE_OFFSET_CODE = 122;
    private static final int GET_SEND_CONTEXT_CODE = 105;
    private static final int GET_OFFSET_ROUTING_CODE = 123;
    private static final int HISTORY_UNAVAILABLE = 87;
    private static final int LIFECYCLE_BUSY = IggyErrorCode.LIFECYCLE_BUSY.getCode();
    private static final int PARTITION_NOT_OWNED = 5009;
    private static final int ASSIGNED_PARTITION = 7;
    // An offset write status of the fake primary: the write is stored.
    private static final int STORED = 0;
    private static final long ROUTE_INCARNATION = 1;
    private static final PartitionContext CALLER_CONTEXT =
            new PartitionContext(BigInteger.valueOf(41), BigInteger.valueOf(42), BigInteger.valueOf(43));

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
                assertServerError(storeOffset(client, 3), HISTORY_UNAVAILABLE);
                assertThat(discoveries).hasValue(1);
                storeOffset(client, 4).get(5, TimeUnit.SECONDS);
                assertThat(discoveries).hasValue(2);
                var polledBeforeTheChange = new PartitionContext(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO);
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(
                                        StreamId.of(1L),
                                        TopicId.of(1L),
                                        0L,
                                        Consumer.of(7L),
                                        BigInteger.valueOf(5),
                                        polledBeforeTheChange),
                        HISTORY_UNAVAILABLE);
                assertThat(discoveries).hasValue(2);
                assertThat(storedIncarnations).containsExactly(1L, 1L, 1L, 2L, 1L);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldStampTheCallerPollContextOnEveryRetryInsteadOfADiscoveredOne() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger discoveries = new AtomicInteger();
            AtomicInteger polls = new AtomicInteger();
            List<PartitionContext> stamped = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    discoveries.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, coordinatorSocket.getLocalPort(), ROUTE_INCARNATION));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    stamped.add(stampedContext(request));
                    // A refusal replays the same frame, a lifecycle refusal encodes a new one.
                    return switch (polls.incrementAndGet()) {
                        case 1 -> Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
                        case 2 -> Response.error(OPERATION_NON_REPLICATED, LIFECYCLE_BUSY);
                        default -> Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                    };
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                pollPartition(client, PollingStrategy.next().withContext(CALLER_CONTEXT))
                        .get(5, TimeUnit.SECONDS);
                pollPartition(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);
                assertThat(stamped)
                        .containsExactly(
                                CALLER_CONTEXT, CALLER_CONTEXT, CALLER_CONTEXT, routeContext(ROUTE_INCARNATION));
                assertThat(discoveries).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldDropTheCachedPollRouteWhenAPollWithACallerContextFails() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger discoveries = new AtomicInteger();
            List<PartitionContext> stamped = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    discoveries.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, coordinatorSocket.getLocalPort(), ROUTE_INCARNATION));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    PartitionContext context = stampedContext(request);
                    stamped.add(context);
                    return context.equals(CALLER_CONTEXT)
                            ? Response.error(OPERATION_NON_REPLICATED, HISTORY_UNAVAILABLE)
                            : Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                pollPartition(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);
                assertServerError(
                        pollPartition(client, PollingStrategy.next().withContext(CALLER_CONTEXT)), HISTORY_UNAVAILABLE);
                pollPartition(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);

                assertThat(stamped)
                        .containsExactly(
                                routeContext(ROUTE_INCARNATION), CALLER_CONTEXT, routeContext(ROUTE_INCARNATION));
                assertThat(discoveries).hasValue(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldKeepTheCallerPollContextWhenAPrimaryPollIsRerouted() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger routes = new AtomicInteger();
            AtomicInteger polls = new AtomicInteger();
            List<PartitionContext> stamped = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            clusterMetadata(
                                    coordinatorSocket.getLocalPort(),
                                    primarySocket.getLocalPort(),
                                    coordinatorSocket.getLocalPort()));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    // Every route reports a newer incarnation, so the reroute offers another context.
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, primarySocket.getLocalPort(), routes.incrementAndGet()));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            CompletableFuture<Void> primary = serve(primarySocket, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                    stamped.add(stampedContext(request));
                    return polls.incrementAndGet() == 1
                            ? Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED)
                            : Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected primary request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                pollPartition(client, PollingStrategy.next().withContext(CALLER_CONTEXT))
                        .get(5, TimeUnit.SECONDS);
                assertThat(routes).hasValue(2);
                pollPartition(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);
                assertThat(routes).hasValue(2);
                assertThat(stamped).containsExactly(CALLER_CONTEXT, CALLER_CONTEXT, routeContext(2));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReturnPartitionNotOwnedToAGroupPollWithACallerContextAndRetryOneWithout() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger syncs = new AtomicInteger();
            AtomicInteger polls = new AtomicInteger();
            List<PartitionContext> stamped = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GROUP_SYNC_CODE, OPERATION_NON_REPLICATED)) {
                    syncs.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            Unpooled.buffer().writeLongLE(1).writeIntLE(1).writeIntLE(0));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, coordinatorSocket.getLocalPort(), ROUTE_INCARNATION));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    stamped.add(stampedContext(request));
                    return polls.incrementAndGet() <= 2
                            ? Response.error(OPERATION_NON_REPLICATED, PARTITION_NOT_OWNED)
                            : Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                assertServerError(
                        pollGroup(client, PollingStrategy.next().withContext(CALLER_CONTEXT)), PARTITION_NOT_OWNED);
                assertThat(polls).hasValue(1);
                pollGroup(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);
                assertThat(polls).hasValue(3);
                assertThat(syncs).hasValue(3);
                assertThat(stamped)
                        .containsExactly(
                                CALLER_CONTEXT, routeContext(ROUTE_INCARNATION), routeContext(ROUTE_INCARNATION));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldSyncAnEmptyGroupAssignmentAgainOnTheNextPoll() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger syncs = new AtomicInteger();
            AtomicInteger polls = new AtomicInteger();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, singleNodeMetadata(coordinatorSocket.getLocalPort()));
                }
                if (request.is(GROUP_SYNC_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            syncs.incrementAndGet() == 1
                                    ? Unpooled.buffer().writeLongLE(1).writeIntLE(0)
                                    : Unpooled.buffer()
                                            .writeLongLE(1)
                                            .writeIntLE(1)
                                            .writeIntLE(ASSIGNED_PARTITION));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, coordinatorSocket.getLocalPort(), ROUTE_INCARNATION));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    polls.incrementAndGet();
                    return Response.success(OPERATION_NON_REPLICATED, emptyPoll(ASSIGNED_PARTITION));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                pollGroup(client, PollingStrategy.next()).get(5, TimeUnit.SECONDS);
                assertThat(polls).hasValue(0);
                assertThat(pollGroup(client, PollingStrategy.next())
                                .get(5, TimeUnit.SECONDS)
                                .partitionId())
                        .isEqualTo(ASSIGNED_PARTITION);
                assertThat(syncs).hasValue(2);
                assertThat(polls).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldStoreOffsetsOnThePrimaryAndRouteAgainAfterARefusal() throws Exception {
        for (int refusal : new int[] {HISTORY_UNAVAILABLE, PARTITION_NOT_OWNED, LIFECYCLE_BUSY}) {
            // The client replaces its data connection after a refusal other than 58.
            try (OffsetCluster cluster = new OffsetCluster(2, STORED, refusal, STORED)) {
                AsyncIggyTcpClient client = client(cluster.coordinatorSocket);
                try {
                    client.connect().get(5, TimeUnit.SECONDS);
                    client.login().get(5, TimeUnit.SECONDS);
                    storeOffset(client, 1).get(5, TimeUnit.SECONDS);
                    assertServerError(storeOffset(client, 2), refusal);
                    storeOffset(client, 3).get(5, TimeUnit.SECONDS);
                } finally {
                    client.close().get(5, TimeUnit.SECONDS);
                }
                cluster.awaitServed();
                assertThat(cluster.coordinatorWrites).isEmpty();
                assertThat(cluster.routings).hasSize(2);
                assertThat(cluster.primaryWrites)
                        .extracting(AsyncIggyTcpClientPartitionContextTest::stampedContext)
                        .containsExactly(routeContext(1), routeContext(1), routeContext(2));
                assertThat(cluster.binds).extracting(Request::connection).containsExactly(0, 1);
                byte[] store = cluster.primaryWrites.get(0).body();
                // A route names the consumer, stream, topic and partition that the write leads with.
                assertThat(cluster.routings)
                        .allSatisfy(routing -> assertThat(routing.body())
                                .isEqualTo(Arrays.copyOf(store, store.length - Long.BYTES - 1)));
            }
        }
    }

    @Test
    void shouldKeepTheCallerOffsetContextWhenAPrimaryStoreIsRerouted() throws Exception {
        try (OffsetCluster cluster = new OffsetCluster(1, TRANSIENT_NOT_ACCEPTED, STORED, STORED)) {
            AsyncIggyTcpClient client = client(cluster.coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                storeOffset(client, 1, CALLER_CONTEXT).get(5, TimeUnit.SECONDS);
                assertThat(cluster.routings).hasSize(2);
                storeOffset(client, 2).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.routings).hasSize(2);
            // Every route reports a newer incarnation, so the reroute offers another context.
            assertThat(cluster.primaryWrites)
                    .extracting(AsyncIggyTcpClientPartitionContextTest::stampedContext)
                    .containsExactly(CALLER_CONTEXT, CALLER_CONTEXT, routeContext(2));
        }
    }

    @Test
    void shouldNumberPrimaryOffsetWritesFromTheCoordinatorSession() throws Exception {
        try (OffsetCluster cluster = new OffsetCluster(2, HISTORY_UNAVAILABLE, STORED)) {
            AsyncIggyTcpClient client = client(cluster.coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                send(client).get(5, TimeUnit.SECONDS);
                assertServerError(storeOffset(client, 1), HISTORY_UNAVAILABLE);
                storeOffset(client, 2).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            // The partition deduplicates by client and request id, whichever connection carries them.
            long sent = cluster.coordinatorWrites.get(0).requestId();
            assertThat(cluster.primaryWrites).extracting(Request::connection).containsExactly(0, 1);
            assertThat(cluster.primaryWrites).extracting(Request::requestId).containsExactly(sent + 1, sent + 2);
        }
    }

    @Test
    void shouldReturnALifecycleRefusalOfAnOffsetRouteWithoutRetryingIt() throws Exception {
        try (OffsetCluster cluster = new OffsetCluster(1, STORED)) {
            cluster.refuseFirstRoute(LIFECYCLE_BUSY);
            AsyncIggyTcpClient client = client(cluster.coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                assertServerError(storeOffset(client, 1), LIFECYCLE_BUSY);
                assertThat(cluster.routings).hasSize(1);
                storeOffset(client, 2).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.routings).hasSize(2);
            assertThat(cluster.primaryWrites).hasSize(1);
        }
    }

    @Test
    void shouldSendRawOffsetWritesOfAClusterToThePrimary() throws Exception {
        try (OffsetCluster cluster = new OffsetCluster(1, STORED, STORED)) {
            AsyncIggyTcpClient client = client(cluster.coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(STORE_OFFSET_CODE, rawStoreOffset()).get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(DELETE_OFFSET_CODE, rawDeleteOffset()).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            assertThat(cluster.coordinatorWrites).isEmpty();
            cluster.awaitServed();
            assertThat(cluster.routings)
                    .singleElement()
                    .satisfies(routing -> assertThat(routing.body()).isEqualTo(ByteBufUtil.getBytes(offsetTarget())));
            assertThat(cluster.primaryWrites)
                    .extracting(Request::operation)
                    .containsExactly(OPERATION_STORE_CONSUMER_OFFSET, OPERATION_DELETE_CONSUMER_OFFSET);
            assertThat(cluster.primaryWrites)
                    .extracting(AsyncIggyTcpClientPartitionContextTest::stampedContext)
                    .containsOnly(routeContext(1));
        }
    }

    private static CompletableFuture<PolledMessages> pollPartition(
            AsyncIggyTcpClient client, PollingStrategy strategy) {
        return client.messages()
                .pollMessages(StreamId.of(1L), TopicId.of(1L), Optional.of(0L), Consumer.of(7L), strategy, 1L, false);
    }

    private static CompletableFuture<PolledMessages> pollGroup(AsyncIggyTcpClient client, PollingStrategy strategy) {
        return client.messages()
                .pollMessages(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.empty(),
                        Consumer.group(ConsumerId.of(7L)),
                        strategy,
                        1L,
                        false);
    }

    private static PartitionContext stampedContext(Request request) {
        return new PartitionContext(
                BigInteger.valueOf(request.incarnation()),
                BigInteger.valueOf(request.ownerGeneration()),
                BigInteger.valueOf(request.metadataOp()));
    }

    private static PartitionContext routeContext(long incarnation) {
        return new PartitionContext(BigInteger.valueOf(incarnation), BigInteger.ZERO, BigInteger.ZERO);
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

    private static CompletableFuture<Void> storeOffset(
            AsyncIggyTcpClient client, long offset, PartitionContext context) {
        return client.consumerOffsets()
                .storeConsumerOffset(
                        StreamId.of(1L), TopicId.of(1L), 0L, Consumer.of(7L), BigInteger.valueOf(offset), context);
    }

    private static Response stored(int operation) {
        return Response.success(operation, Unpooled.buffer().writeIntLE(0));
    }

    private static boolean isOffsetWrite(Request request) {
        return request.operation() == OPERATION_STORE_CONSUMER_OFFSET
                || request.operation() == OPERATION_DELETE_CONSUMER_OFFSET;
    }

    /**
     * A coordinator that routes offset writes to a primary, each route reporting a newer
     * incarnation, and a primary that answers its offset writes with the given statuses.
     */
    private static final class OffsetCluster implements AutoCloseable {
        private final ServerSocket coordinatorSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        private final ServerSocket primarySocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        private final List<Request> routings = new CopyOnWriteArrayList<>();
        private final List<Request> coordinatorWrites = new CopyOnWriteArrayList<>();
        private final List<Request> primaryWrites = new CopyOnWriteArrayList<>();
        private final List<Request> binds = new CopyOnWriteArrayList<>();
        private final int[] writeStatuses;
        private final CompletableFuture<Void> coordinator;
        private final CompletableFuture<Void> primary;
        private volatile int firstRouteRefusal = STORED;

        /** @param dataConnections how many connections the primary serves before it stops */
        OffsetCluster(int dataConnections, int... writeStatuses) throws IOException {
            this.writeStatuses = writeStatuses;
            coordinator = serve(coordinatorSocket, this::coordinate);
            primary = serve(primarySocket, dataConnections, this::answerOnPrimary);
        }

        void refuseFirstRoute(int status) {
            firstRouteRefusal = status;
        }

        void awaitServed() throws Exception {
            coordinator.get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws IOException {
            coordinatorSocket.close();
            primarySocket.close();
        }

        private Response coordinate(Request request) {
            if (request.operation() == OPERATION_REGISTER) {
                return Response.success(OPERATION_REGISTER, registerBody(1));
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(
                        OPERATION_NON_REPLICATED,
                        clusterMetadata(
                                coordinatorSocket.getLocalPort(),
                                primarySocket.getLocalPort(),
                                coordinatorSocket.getLocalPort()));
            }
            if (request.is(GET_SEND_CONTEXT_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, partitionContext(Unpooled.buffer(), 1));
            }
            if (request.is(GET_OFFSET_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                routings.add(request);
                return routings.size() == 1 && firstRouteRefusal != STORED
                        ? Response.error(OPERATION_NON_REPLICATED, firstRouteRefusal)
                        : Response.success(
                                OPERATION_NON_REPLICATED,
                                pollRoute(request, 1, 1, primarySocket.getLocalPort(), routings.size()));
            }
            if (request.operation() == OPERATION_SEND_MESSAGES) {
                coordinatorWrites.add(request);
                return Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
            }
            if (isOffsetWrite(request)) {
                coordinatorWrites.add(request);
                return stored(request.operation());
            }
            throw new IllegalStateException("Unexpected coordinator request: " + request);
        }

        private Response answerOnPrimary(Request request) {
            if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                binds.add(request);
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            }
            if (!isOffsetWrite(request) || primaryWrites.size() == writeStatuses.length) {
                throw new IllegalStateException("Unexpected primary request: " + request);
            }
            int status = writeStatuses[primaryWrites.size()];
            primaryWrites.add(request);
            return status == STORED ? stored(request.operation()) : Response.error(request.operation(), status);
        }
    }
}
