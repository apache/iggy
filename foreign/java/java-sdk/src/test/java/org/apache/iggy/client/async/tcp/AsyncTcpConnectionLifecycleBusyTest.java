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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.AsyncTcpConnection.TransientFailoverState;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_POLL_ROUTING_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.transientResult;
import static org.assertj.core.api.Assertions.assertThat;

class AsyncTcpConnectionLifecycleBusyTest {
    private static final int LIFECYCLE_BUSY = IggyErrorCode.LIFECYCLE_BUSY.getCode();
    private static final int TRANSIENT_NOT_COMMITTED = IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode();
    private static final int UNAUTHORIZED = IggyErrorCode.UNAUTHORIZED.getCode();
    private static final int PING_CODE = 1;
    private static final int LOGIN_CODE = 38;
    private static final int POLL_ON_PRIMARY_CODE = 104;
    private static final int JOIN_GROUP_CODE = 604;
    private static final int OPERATION_JOIN_GROUP = 148;
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);
    // Pauses doubling from 50 ms start attempts near 0, 50, 150, 350 and 750 ms
    // of the request timeout; a fixed 50 ms pause would start twenty.
    private static final int MAX_ATTEMPTS = 5;
    private static final Duration FIRST_PAUSE = Duration.ofMillis(50);
    private static final Duration SECOND_PAUSE = Duration.ofMillis(100);
    private static final Duration DEADLINE_MARGIN = Duration.ofMillis(500);
    // The fifth refusal starts an 800 ms pause; closing 300 ms after it lands inside.
    private static final int LONG_PAUSE_ATTEMPT = 5;
    private static final Duration INTO_LONG_PAUSE = Duration.ofMillis(300);
    private static final PartitionContext CONTEXT =
            new PartitionContext(BigInteger.valueOf(17), BigInteger.valueOf(3), BigInteger.valueOf(4));
    private static final byte[] JOIN_BODY = "join-body".getBytes(StandardCharsets.UTF_8);
    private static final byte[] JOINED = "joined".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CREDENTIAL = "iggy".getBytes(StandardCharsets.UTF_8);

    @Test
    void shouldRetryLifecycleBusyAsNewRequestsWithTheSameContext() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Request> joins = new CopyOnWriteArrayList<>();
            List<Long> arrivals = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    arrivals.add(System.nanoTime());
                    joins.add(request);
                    return joins.size() < 3 ? lifecycleBusy() : joined();
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, Duration.ofSeconds(TEST_TIMEOUT_SECONDS));
            try {
                login(connection);
                ByteBuf response = connection
                        .send(
                                JOIN_GROUP_CODE,
                                Unpooled.wrappedBuffer(JOIN_BODY),
                                0,
                                new TransientFailoverState(CONTEXT))
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                try {
                    assertThat(ByteBufUtil.getBytes(response)).isEqualTo(JOINED);
                } finally {
                    response.release();
                }
            } finally {
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertThat(joins).hasSize(3);
            assertThat(joins).extracting(Request::requestId).doesNotHaveDuplicates();
            assertThat(joins).allSatisfy(join -> {
                assertThat(join.clientLow()).isEqualTo(joins.get(0).clientLow());
                assertThat(join.clientHigh()).isEqualTo(joins.get(0).clientHigh());
                assertThat(join.incarnation()).isEqualTo(17);
                assertThat(join.ownerGeneration()).isEqualTo(3);
                assertThat(join.metadataOp()).isEqualTo(4);
                assertThat(join.body()).isEqualTo(JOIN_BODY);
            });
            assertThat(Duration.ofNanos(arrivals.get(1) - arrivals.get(0))).isGreaterThanOrEqualTo(FIRST_PAUSE);
            assertThat(Duration.ofNanos(arrivals.get(2) - arrivals.get(1))).isGreaterThanOrEqualTo(SECOND_PAUSE);
        }
    }

    @Test
    void shouldReturnLifecycleBusyOnceTheNextPauseWouldPassTheRequestDeadline() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger attempts = new AtomicInteger();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    attempts.incrementAndGet();
                    return lifecycleBusy();
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, REQUEST_TIMEOUT);
            try {
                login(connection);
                long started = System.nanoTime();
                assertServerError(connection.send(JOIN_GROUP_CODE, Unpooled.wrappedBuffer(JOIN_BODY)), LIFECYCLE_BUSY);
                assertThat(Duration.ofNanos(System.nanoTime() - started))
                        .isLessThan(REQUEST_TIMEOUT.plus(DEADLINE_MARGIN));
            } finally {
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(attempts.get()).isBetween(2, MAX_ATTEMPTS);
        }
    }

    @Test
    void shouldServeOtherRequestsWhileALifecycleRetryPauses() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<String> served = new CopyOnWriteArrayList<>();
            AtomicBoolean pinged = new AtomicBoolean();
            CompletableFuture<Void> refused = new CompletableFuture<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
                    pinged.set(true);
                    served.add("ping");
                    return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    // The join succeeds only after the ping, so a held lease or lock
                    // would refuse it until its deadline.
                    if (pinged.get()) {
                        served.add("join");
                        return Response.success(
                                OPERATION_JOIN_GROUP, Unpooled.buffer().writeIntLE(0));
                    }
                    served.add("refused join");
                    refused.complete(null);
                    return lifecycleBusy();
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                CompletableFuture<Void> join =
                        client.consumerGroups().joinConsumerGroup(StreamId.of(1L), TopicId.of(1L), ConsumerId.of(1L));
                refused.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                client.system().ping().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                join.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(served).startsWith("refused join").endsWith("ping", "join");
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReturnLifecycleBusyFromPollRoutingExchangesWithoutRetrying() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger routings = new AtomicInteger();
            AtomicInteger primaryPolls = new AtomicInteger();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    routings.incrementAndGet();
                    return Response.error(OPERATION_NON_REPLICATED, LIFECYCLE_BUSY);
                }
                if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                    primaryPolls.incrementAndGet();
                    return Response.error(OPERATION_NON_REPLICATED, LIFECYCLE_BUSY);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, REQUEST_TIMEOUT);
            try {
                login(connection);
                // Context discovery may replay transient denials, never a lifecycle refusal.
                assertServerError(
                        connection.send(
                                GET_POLL_ROUTING_CODE,
                                Unpooled.EMPTY_BUFFER,
                                0,
                                new TransientFailoverState(PartitionContext.EMPTY, true)),
                        LIFECYCLE_BUSY);
                assertServerError(
                        connection.send(
                                POLL_ON_PRIMARY_CODE, Unpooled.EMPTY_BUFFER, 0, new TransientFailoverState(CONTEXT)),
                        LIFECYCLE_BUSY);
            } finally {
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(routings).hasValue(1);
            assertThat(primaryPolls).hasValue(1);
        }
    }

    @Test
    void shouldReplayNotCommittedUnderItsIdAndKeepTheUncertainOutcomeAcrossALifecycleRetry() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Long> requestIds = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    requestIds.add(request.requestId());
                    return switch (requestIds.size()) {
                        case 1 -> Response.error(OPERATION_JOIN_GROUP, TRANSIENT_NOT_COMMITTED);
                        case 2 -> lifecycleBusy();
                        default -> Response.error(OPERATION_JOIN_GROUP, UNAUTHORIZED);
                    };
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, REQUEST_TIMEOUT);
            try {
                login(connection);
                // A later refusal cannot prove the first attempt was not committed.
                assertServerError(
                        connection.send(JOIN_GROUP_CODE, Unpooled.wrappedBuffer(JOIN_BODY)), TRANSIENT_NOT_COMMITTED);
            } finally {
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(requestIds).hasSize(3);
            assertThat(requestIds.get(1)).isEqualTo(requestIds.get(0));
            assertThat(requestIds.get(2)).isNotEqualTo(requestIds.get(0));
        }
    }

    @Test
    void shouldReturnTheRefusalWhenTheChannelClosesDuringALifecyclePause() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger attempts = new AtomicInteger();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    attempts.incrementAndGet();
                    return Response.successAndDisconnect(OPERATION_JOIN_GROUP, transientResult(LIFECYCLE_BUSY));
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, REQUEST_TIMEOUT);
            try {
                login(connection);
                assertServerError(connection.send(JOIN_GROUP_CODE, Unpooled.wrappedBuffer(JOIN_BODY)), LIFECYCLE_BUSY);
            } finally {
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(attempts).hasValue(1);
        }
    }

    @Test
    void shouldFailAPausedLifecycleRetryWhenTheConnectionCloses() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger attempts = new AtomicInteger();
            CompletableFuture<Void> longPauseStarted = new CompletableFuture<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_JOIN_GROUP) {
                    if (attempts.incrementAndGet() == LONG_PAUSE_ATTEMPT) {
                        longPauseStarted.complete(null);
                    }
                    return lifecycleBusy();
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncTcpConnection connection = connection(socket, Duration.ofSeconds(TEST_TIMEOUT_SECONDS));
            CompletableFuture<ByteBuf> join;
            try {
                login(connection);
                join = connection.send(JOIN_GROUP_CODE, Unpooled.wrappedBuffer(JOIN_BODY));
                longPauseStarted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                Thread.sleep(INTO_LONG_PAUSE.toMillis());
            } finally {
                // Shutting down the event loop drops the pending retry task.
                connection.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertServerError(join, LIFECYCLE_BUSY);
            server.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(attempts).hasValue(LONG_PAUSE_ATTEMPT);
        }
    }

    /** The server commits a lifecycle refusal in the result section, as the metadata plane does. */
    private static Response lifecycleBusy() {
        return Response.success(OPERATION_JOIN_GROUP, transientResult(LIFECYCLE_BUSY));
    }

    private static Response joined() {
        return Response.success(
                OPERATION_JOIN_GROUP, Unpooled.buffer().writeIntLE(0).writeBytes(JOINED));
    }

    private static AsyncTcpConnection connection(ServerSocket socket, Duration requestTimeout) {
        return new AsyncTcpConnection(
                socket.getInetAddress().getHostAddress(),
                socket.getLocalPort(),
                false,
                Optional.empty(),
                new AsyncTcpConnection.TcpConnectionPoolConfig(),
                Optional.empty(),
                AsyncTcpConnection.DEFAULT_IO_THREADS,
                Optional.of(Duration.ofSeconds(TEST_TIMEOUT_SECONDS)),
                Optional.of(requestTimeout),
                Duration.ofHours(1),
                1024 * 1024,
                null,
                errorCode -> {},
                ignored -> {});
    }

    private static void login(AsyncTcpConnection connection) throws Exception {
        connection.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ByteBuf credentials = Unpooled.buffer();
        credentials.writeByte(CREDENTIAL.length).writeBytes(CREDENTIAL);
        credentials.writeByte(CREDENTIAL.length).writeBytes(CREDENTIAL);
        connection
                .send(LOGIN_CODE, credentials)
                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .release();
    }
}
