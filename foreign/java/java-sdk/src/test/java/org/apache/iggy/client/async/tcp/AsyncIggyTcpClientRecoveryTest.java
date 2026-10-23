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
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.EVICTION_STALE_CLIENT;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.connection;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.transientResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientRecoveryTest {
    // A call that paces its retries binds a few times a second, and one that does not binds thousands of times.
    private static final int MAX_PACED_BINDS = 100;

    @Test
    void shouldPreserveTheOperationIdWhenClearingARouterWithAnUnansweredWrite() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> written = new CompletableFuture<>();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(1));
                }
                if (request.is(CommandCode.System.GET_CLUSTER_METADATA.getValue(), VsrOperation.NON_REPLICATED)) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.is(CommandCode.Messages.GET_PARTITION_ROUTE.getValue(), VsrOperation.NON_REPLICATED)) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 1, 1, socket.getLocalPort()));
                }
                written.complete(request);
                return Response.noReply();
            });
            AsyncTcpConnection parent = connection(socket);
            PollRouter router = new PollRouter(() -> parent, endpoint -> {
                throw new IllegalStateException("The coordinator owns the partition");
            });
            router.coordinatorEndpoint(
                    () -> new ConnectionInfo(socket.getInetAddress().getHostAddress(), socket.getLocalPort()));
            try {
                parent.connect().get(5, TimeUnit.SECONDS);
                new UsersTcpClient(() -> parent).login("iggy", "iggy").get(5, TimeUnit.SECONDS);
                var pending = new MessagesTcpClient(() -> parent, new ClientRoutingState(), router)
                        .sendMessages(
                                StreamId.of(1L),
                                TopicId.of(1L),
                                Partitioning.partitionId(0L),
                                List.of(Message.of("retained")));
                var original = written.get(5, TimeUnit.SECONDS);
                router.clearSession().get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> {
                            var uncertain = (IggyOperationException) error.getCause();
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId()).isEqualTo(original.requestId());
                        });
            } finally {
                router.clearSession().get(5, TimeUnit.SECONDS);
                parent.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRejectNonPositiveResolutionTimeouts() {
        var client = AsyncIggyTcpClient.builder().build();
        for (Duration timeout : List.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofSeconds(Long.MIN_VALUE))) {
            assertThatThrownBy(() -> client.resolveOperation(1, timeout))
                    .isInstanceOf(IggyInvalidArgumentException.class)
                    .hasMessage("Resolution timeout must be positive");
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldReplayLostMutationLinksWithinTheOriginalCall() throws Exception {
        for (boolean routed : List.of(false, true)) {
            for (LostLink lostLink : LostLink.values()) {
                try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
                    List<Request> mutations = new CopyOnWriteArrayList<>();
                    var server = serve(socket, 2, request -> {
                        if (request.operation() == VsrOperation.REGISTER) {
                            return Response.success(VsrOperation.REGISTER, registerBody(42));
                        }
                        if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                            return Response.success(
                                    VsrOperation.NON_REPLICATED,
                                    AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                        }
                        if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                            return Response.success(
                                    VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                        }
                        if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                            return Response.success(
                                    VsrOperation.NON_REPLICATED,
                                    AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                            request, 42, 1, socket.getLocalPort()));
                        }
                        if (request.operation() == VsrOperation.DELETE_STREAM
                                || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                            mutations.add(request);
                            if (mutations.size() == 1) {
                                return switch (lostLink) {
                                    case DISCONNECT -> Response.disconnect();
                                    case STALE_CLIENT_EVICTION -> Response.eviction(EVICTION_STALE_CLIENT);
                                    case UNPROVED_REPLY ->
                                        Response.unproved(
                                                request.operation(),
                                                Unpooled.buffer().writeIntLE(0));
                                };
                            }
                            return Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                        }
                        throw new IllegalStateException("Unexpected request " + request);
                    });
                    var client = AsyncIggyTcpClient.builder()
                            .host(socket.getInetAddress().getHostAddress())
                            .port(socket.getLocalPort())
                            .credentials("iggy", "iggy")
                            .requestTimeout(Duration.ofSeconds(3))
                            .heartbeatInterval(Duration.ofMinutes(1))
                            .build();
                    try {
                        client.connect().get(5, TimeUnit.SECONDS);
                        client.login().get(5, TimeUnit.SECONDS);
                        client.sendBinaryRequest(
                                        routed
                                                ? CommandCode.ConsumerOffset.STORE.getValue()
                                                : CommandCode.Stream.DELETE.getValue(),
                                        routed
                                                ? AsyncIggyTcpClientRawRequestTest.rawStoreOffset()
                                                : new byte[] {1, 4, 1, 0, 0, 0})
                                .get(5, TimeUnit.SECONDS);
                        assertThat(mutations).hasSize(2);
                        Request first = mutations.get(0);
                        Request replay = mutations.get(1);
                        assertThat(replay.connection()).isEqualTo(first.connection() + 1);
                        assertThat(replay.requestId()).isEqualTo(first.requestId());
                        assertThat(replay.clientLow()).isEqualTo(first.clientLow());
                        assertThat(replay.clientHigh()).isEqualTo(first.clientHigh());
                        assertThat(replay.body()).isEqualTo(first.body());
                        assertThat(replay.incarnation()).isEqualTo(first.incarnation());
                        assertThat(replay.ownerGeneration()).isEqualTo(first.ownerGeneration());
                        assertThat(replay.metadataOp()).isEqualTo(first.metadataOp());
                    } finally {
                        client.close().get(5, TimeUnit.SECONDS);
                    }
                    server.get(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    @ParameterizedTest(name = "routed={0}, reason={1}")
    @CsvSource({"false,NO_SESSION", "true,NO_SESSION", "false,STALE_CLIENT", "true,STALE_CLIENT"})
    void shouldRetainADetachedMutationAndProveItOnTheSameSession(boolean routed, SessionEviction eviction)
            throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> binds = new CopyOnWriteArrayList<>();
            AtomicBoolean resolving = new AtomicBoolean();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    binds.add(request);
                    return Response.success(
                            VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    if (writes.size() == 1) {
                        return Response.eviction(eviction.reason);
                    }
                    return resolving.get()
                            ? Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0))
                            : Response.error(request.operation(), TRANSIENT_NOT_ACCEPTED);
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var written = writeMutation(client, routed);
                assertThatThrownBy(() -> written.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> {
                            var uncertain = (IggyOperationException) error.getCause();
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId())
                                    .isEqualTo(writes.get(0).requestId());
                        });
                assertThat(writes).hasSizeGreaterThan(1);

                resolving.set(true);
                client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))
                        .get(5, TimeUnit.SECONDS);
                assertThat(writes)
                        .extracting(Request::requestId)
                        .containsOnly(writes.get(0).requestId());
                assertThat(writes.get(writes.size() - 1).connection()).isEqualTo(1);
                assertThat(registers).hasSize(1);
                assertThat(binds).isNotEmpty().allSatisfy(bind -> {
                    ByteBuffer identity = AsyncIggyTcpClientTransientFailoverTest.bindIdentity(bind.body());
                    assertThat(identity.getLong(0)).isEqualTo(registers.get(0).clientLow());
                    assertThat(identity.getLong(8)).isEqualTo(registers.get(0).clientHigh());
                    assertThat(identity.getLong(16)).isEqualTo(42);
                });
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}, sessionEnded={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldBindTheSessionAgainOnANewLinkUnlessTheEvictionOfItsBindEndedIt(boolean routed, boolean sessionEnded)
            throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 3, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> binds = new CopyOnWriteArrayList<>();
            var server = serve(socket, 3, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(41 + registers.size()));
                }
                // The server evicts the link that carries the first bind.
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    binds.add(request);
                    return binds.size() == 1
                            ? Response.eviction(
                                    sessionEnded ? SessionEviction.NO_SESSION.reason : EVICTION_STALE_CLIENT)
                            : Response.success(
                                    VsrOperation.NON_REPLICATED,
                                    AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                    request, 41 + registers.size(), 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    return writes.size() == 1
                            ? Response.disconnect()
                            : Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(3))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var written = writeMutation(client, routed);
                if (sessionEnded) {
                    assertThatThrownBy(() -> written.get(5, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyOperationException.class,
                                    uncertain -> assertThat(uncertain.getErrorCode())
                                            .isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN));
                    assertThat(outcome(client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))))
                            .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());
                    writeMutation(client, routed).get(5, TimeUnit.SECONDS);
                } else {
                    written.get(5, TimeUnit.SECONDS);
                    assertThat(writes)
                            .extracting(Request::requestId)
                            .containsOnly(writes.get(0).requestId());
                }

                assertThat(writes).hasSize(2);
                assertThat(writes.get(1).connection()).isEqualTo(2);
                assertThat(binds)
                        .extracting(Request::connection)
                        .containsExactlyElementsOf(sessionEnded ? List.of(1) : List.of(1, 2));
                assertThat(binds).allSatisfy(bind -> {
                    ByteBuffer identity = AsyncIggyTcpClientTransientFailoverTest.bindIdentity(bind.body());
                    assertThat(identity.getLong(0)).isEqualTo(registers.get(0).clientLow());
                    assertThat(identity.getLong(8)).isEqualTo(registers.get(0).clientHigh());
                    assertThat(identity.getLong(16)).isEqualTo(42);
                });
                assertThat(registers)
                        .hasSize(sessionEnded ? 2 : 1)
                        .extracting(Request::clientLow)
                        .doesNotHaveDuplicates()
                        .last()
                        .isEqualTo(writes.get(1).clientLow());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * The receipt policy: a write loses its reply, and then its session ends, by a refused bind, an evicted bind or
     * a refused sign-in. The write never goes out again, and it ends uncertain, not with the error that ended the
     * session.
     */
    @ParameterizedTest(name = "sessionEnd={0}")
    @EnumSource(SessionEnd.class)
    void shouldEndAWriteWhoseReplyWasLostUncertainWhenItsSessionEnds(SessionEnd sessionEnd) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 3, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            AtomicInteger registers = new AtomicInteger();
            var server = serve(socket, 3, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return registers.incrementAndGet() > 1 && sessionEnd == SessionEnd.REFUSED_SIGN_IN
                            ? Response.error(VsrOperation.REGISTER, IggyErrorCode.USER_INACTIVE.getCode())
                            : Response.success(VsrOperation.REGISTER, registerBody(41 + registers.get()));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return switch (sessionEnd) {
                        case REFUSED_BIND ->
                            Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.STALE_CLIENT.getCode());
                        case EVICTED_BIND -> Response.eviction(SessionEviction.NO_SESSION.reason);
                        case REFUSED_SIGN_IN ->
                            Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                    };
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM) {
                    writes.add(request);
                    // The reply is lost with the link.
                    return Response.disconnect();
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(3))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                var written = writeMutation(client, false);

                assertThatThrownBy(() -> written.get(5, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId())
                                    .isEqualTo(writes.get(0).requestId());
                        });
                assertThat(writes).as("the attempts of the write").hasSize(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    @ParameterizedTest(name = "routed={0}, eviction={1}")
    @CsvSource({
        "false,NO_SESSION",
        "false,SESSION_TOO_LOW",
        "false,SESSION_RELEASE_MISMATCH",
        "false,USER_INACTIVE",
        "false,SESSION_ERROR",
        "true,NO_SESSION",
        "true,SESSION_TOO_LOW",
        "true,SESSION_RELEASE_MISMATCH",
        "true,USER_INACTIVE",
        "true,SESSION_ERROR"
    })
    void shouldKeepAnEvictedMutationUncertainUntilTheEndedSessionIsReset(boolean routed, SessionEviction eviction)
            throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(41 + registers.size()));
                }
                // The eviction ended the session, so the server cannot bind it again.
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                    request, 41 + registers.size(), 1, socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Stream.GET_ALL.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    return writes.size() == 1
                            ? Response.eviction(eviction.reason)
                            : Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var written = writeMutation(client, routed);
                assertThatThrownBy(() -> written.get(5, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId())
                                    .isEqualTo(writes.get(0).requestId());
                        });

                // A read binds a new link, and the refused bind resets the session.
                client.sendBinaryRequest(CommandCode.Stream.GET_ALL.getValue(), new byte[0])
                        .get(5, TimeUnit.SECONDS);
                assertThat(registers).hasSize(2);
                assertThat(writes).hasSize(1);
                assertThat(outcome(client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))))
                        .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());
                writeMutation(client, routed).get(5, TimeUnit.SECONDS);
                assertThat(writes).hasSize(2);
                assertThat(writes.get(1).clientLow()).isEqualTo(registers.get(1).clientLow());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}")
    @ValueSource(booleans = {false, true})
    void shouldEndAMutationAtOnceWhenItsReplyRefusesItAsUnauthenticated(boolean routed) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    return writes.size() == 1
                            ? Response.error(request.operation(), IggyErrorCode.UNAUTHENTICATED.getCode())
                            : Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        writeMutation(client, routed), IggyErrorCode.UNAUTHENTICATED.getCode());
                assertThat(writes).hasSize(1);
                assertThat(outcome(client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))))
                        .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());
                writeMutation(client, routed).get(5, TimeUnit.SECONDS);
                assertThat(writes).hasSize(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}, committed={1}, sessionEnded={2}")
    @CsvSource({
        "false,false,false",
        "false,false,true",
        "false,true,true",
        "true,false,false",
        "true,false,true",
        "true,true,true"
    })
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldEndAMutationAtOnceWhenItsReplyRefusesItAsStaleAndLetTheBindDecideTheSession(
            boolean routed, boolean committed, boolean sessionEnded) throws Exception {
        int stale = IggyErrorCode.STALE_CLIENT.getCode();
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> binds = new CopyOnWriteArrayList<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(41 + registers.size()));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    binds.add(request);
                    return sessionEnded
                            ? Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode())
                            : Response.success(
                                    VsrOperation.NON_REPLICATED,
                                    AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                    request, 41 + registers.size(), 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    if (writes.size() > 1) {
                        return Response.committed(
                                request.operation(), 3, Unpooled.buffer().writeIntLE(0));
                    }
                    return committed
                            ? Response.committed(request.operation(), 2, transientResult(stale))
                            : Response.error(request.operation(), stale);
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                AsyncIggyTcpClientTransientFailoverTest.assertServerError(writeMutation(client, routed), stale);
                assertThat(writes).hasSize(1);
                assertThat(outcome(client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))))
                        .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());

                // The client closed the link that carried the reply, so the next write binds the session again.
                writeMutation(client, routed).get(5, TimeUnit.SECONDS);
                assertThat(writes).hasSize(2);
                assertThat(writes.get(1).connection()).isEqualTo(1);
                assertThat(binds).hasSize(1);
                assertThat(registers)
                        .hasSize(sessionEnded ? 2 : 1)
                        .extracting(Request::clientLow)
                        .doesNotHaveDuplicates()
                        .last()
                        .isEqualTo(writes.get(1).clientLow());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A reply with status 30 closes the link to the partition primary, and the next poll binds the session there
     * again. That bind decides: a live session goes on, and an ended one signs in again, with no other call in
     * between.
     */
    @ParameterizedTest(name = "sessionEnded={0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldLetThePollAttachDecideTheSessionAfterAStaleClientReply(boolean sessionEnded) throws Exception {
        int stale = IggyErrorCode.STALE_CLIENT.getCode();
        int unauthenticated = IggyErrorCode.UNAUTHENTICATED.getCode();
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> attaches = new CopyOnWriteArrayList<>();
            List<Request> polls = new CopyOnWriteArrayList<>();
            var coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(registers.size()));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.clusterMetadata(
                                    coordinatorSocket.getLocalPort(),
                                    primarySocket.getLocalPort(),
                                    coordinatorSocket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                    request,
                                    registers.size(),
                                    Math.max(1, request.metadataOp()),
                                    primarySocket.getLocalPort()));
                }
                throw new IllegalStateException("Unexpected coordinator request " + request);
            });
            // The reply with status 30 ends the first link, and an ended session cannot bind the second one.
            var primary = serve(primarySocket, 2, request -> {
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    attaches.add(request);
                    boolean ended = sessionEnded
                            && request.connection() == 1
                            && AsyncIggyTcpClientTransientFailoverTest.bindIdentity(request.body())
                                            .getLong(16)
                                    == 1;
                    return ended
                            ? Response.error(VsrOperation.NON_REPLICATED, unauthenticated)
                            : Response.success(
                                    VsrOperation.NON_REPLICATED,
                                    AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.Messages.POLL.getValue()) {
                    polls.add(request);
                    return polls.size() == 1
                            ? Response.error(VsrOperation.NON_REPLICATED, stale)
                            : Response.success(
                                    VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected primary request " + request);
            });
            var client = AsyncIggyTcpClientTransientFailoverTest.client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        AsyncIggyTcpClientTransientFailoverTest.poll(client, Optional.of(0L), false), stale);
                if (sessionEnded) {
                    AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                            AsyncIggyTcpClientTransientFailoverTest.poll(client, Optional.of(0L), false),
                            unauthenticated);
                }
                AsyncIggyTcpClientTransientFailoverTest.poll(client, Optional.of(0L), false)
                        .get(5, TimeUnit.SECONDS);

                assertThat(polls).hasSize(2);
                assertThat(polls.get(1).connection()).isEqualTo(1);
                assertThat(attaches).hasSize(sessionEnded ? 3 : 2);
                assertThat(registers)
                        .hasSize(sessionEnded ? 2 : 1)
                        .extracting(Request::clientLow)
                        .doesNotHaveDuplicates()
                        .last()
                        .isEqualTo(polls.get(1).clientLow());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}, refusal={1}")
    @CsvSource({"false,STALE_CLIENT", "false,UNAUTHENTICATED", "true,STALE_CLIENT", "true,UNAUTHENTICATED"})
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldRetainASentMutationWhoseReplayAReplyRefusesAndProveItOnTheSameSession(
            boolean routed, IggyErrorCode refusal) throws Exception {
        // Only a stale-client reply closes its link, so the resolve binds the session on a third one.
        boolean detached = refusal == IggyErrorCode.STALE_CLIENT;
        try (ServerSocket socket = new ServerSocket(0, 3, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> binds = new CopyOnWriteArrayList<>();
            AtomicBoolean resolving = new AtomicBoolean();
            var server = serve(socket, detached ? 3 : 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    binds.add(request);
                    return Response.success(
                            VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    if (writes.size() == 1) {
                        return Response.disconnect();
                    }
                    return resolving.get()
                            ? Response.committed(
                                    request.operation(),
                                    2,
                                    Unpooled.buffer().writeIntLE(0).writeBytes(UncertainWrite.PROOF))
                            : Response.error(request.operation(), refusal.getCode());
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var written = writeMutation(client, routed);
                assertThatThrownBy(() -> written.get(5, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId())
                                    .isEqualTo(writes.get(0).requestId());
                        });
                assertThat(writes).hasSize(2);

                resolving.set(true);
                assertThat(outcome(client.resolveOperation(writes.get(0).requestId(), Duration.ofSeconds(1))))
                        .isEqualTo("commit " + HexFormat.of().formatHex(UncertainWrite.PROOF));
                assertThat(writes)
                        .hasSize(3)
                        .extracting(Request::requestId)
                        .containsOnly(writes.get(0).requestId());
                assertThat(writes.get(2).connection()).isEqualTo(detached ? 2 : 1);
                assertThat(registers).hasSize(1);
                assertThat(binds).isNotEmpty().allSatisfy(bind -> {
                    ByteBuffer identity = AsyncIggyTcpClientTransientFailoverTest.bindIdentity(bind.body());
                    assertThat(identity.getLong(0)).isEqualTo(registers.get(0).clientLow());
                    assertThat(identity.getLong(8)).isEqualTo(registers.get(0).clientHigh());
                    assertThat(identity.getLong(16)).isEqualTo(42);
                });
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("checkstyle:NPathComplexity")
    void shouldKeepTheStaleRejectionThatAReplayProvesAndStartANewSession(boolean routed) throws Exception {
        int stale = IggyErrorCode.STALE_CLIENT.getCode();
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> binds = new CopyOnWriteArrayList<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(41 + registers.size()));
                }
                // The committed rejection ended the session, so the server cannot bind it again.
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    binds.add(request);
                    return Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                    request, 41 + registers.size(), 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM
                        || request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    return switch (writes.size()) {
                        case 1 -> Response.noReply();
                        case 2 -> Response.committed(request.operation(), 2, transientResult(stale));
                        default ->
                            Response.committed(
                                    request.operation(), 3, Unpooled.buffer().writeIntLE(0));
                    };
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofMillis(300))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        writeMutation(client, routed), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
                long operation = writes.get(0).requestId();

                for (int resolve = 0; resolve < 2; resolve++) {
                    assertThat(outcome(client.resolveOperation(operation, Duration.ofSeconds(1))))
                            .isEqualTo("IggyServerException " + stale);
                }
                assertThat(writes).hasSize(2);

                writeMutation(client, routed).get(5, TimeUnit.SECONDS);
                assertThat(writes).hasSize(3);
                assertThat(writes.get(2).connection()).isEqualTo(1);
                assertThat(binds).hasSize(1);
                assertThat(registers)
                        .hasSize(2)
                        .extracting(Request::clientLow)
                        .doesNotHaveDuplicates()
                        .last()
                        .isEqualTo(writes.get(2).clientLow());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "evicted={0}")
    @ValueSource(booleans = {false, true})
    void shouldRetryAPartitionLookupOnlyWhenItsLinkIsLost(boolean evicted) throws Exception {
        int stale = IggyErrorCode.STALE_CLIENT.getCode();
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> lookups = new CopyOnWriteArrayList<>();
            List<Request> writes = new CopyOnWriteArrayList<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    lookups.add(request);
                    if (lookups.size() == 1) {
                        return evicted
                                ? Response.eviction(EVICTION_STALE_CLIENT)
                                : Response.error(VsrOperation.NON_REPLICATED, stale);
                    }
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    writes.add(request);
                    return Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var written = writeMutation(client, true);
                if (evicted) {
                    written.get(5, TimeUnit.SECONDS);
                } else {
                    AsyncIggyTcpClientTransientFailoverTest.assertServerError(written, stale);
                    assertThat(lookups).hasSize(1);
                    assertThat(writes).isEmpty();
                    writeMutation(client, true).get(5, TimeUnit.SECONDS);
                }
                assertThat(lookups).hasSize(2);
                assertThat(lookups.get(1).connection()).isEqualTo(1);
                assertThat(writes).hasSize(1);
                assertThat(registers).hasSize(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldEndAnEvictedProgressPollWith57WithoutReplayOrRetention() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> polls = new CopyOnWriteArrayList<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.POLL.getValue()) {
                    polls.add(request);
                    return Response.eviction(SessionEviction.NO_SESSION.reason);
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            var stream = org.apache.iggy.identifier.StreamId.of(1L);
            var topic = org.apache.iggy.identifier.TopicId.of(1L);
            var consumer = org.apache.iggy.consumergroup.Consumer.of(7L);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                for (boolean autoCommit : List.of(false, true)) {
                    var strategy = autoCommit
                            ? org.apache.iggy.message.PollingStrategy.offset(BigInteger.ZERO)
                            : org.apache.iggy.message.PollingStrategy.next();
                    var polled = client.messages()
                            .pollMessages(stream, topic, Optional.of(0L), consumer, strategy, 1L, autoCommit);
                    AsyncIggyTcpClientTransientFailoverTest.assertServerError(polled, 57);
                    assertThat(polls).hasSize(autoCommit ? 2 : 1);
                    long requestId = polls.get(polls.size() - 1).requestId();
                    assertThat(outcome(client.resolveOperation(requestId, Duration.ofSeconds(1))))
                            .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());
                }
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldResolveLostReplyWithFrozenIdentityAndReleaseLaneAfterCommittedRejection() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Request> mutations = new CopyOnWriteArrayList<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM) {
                    mutations.add(request);
                    return switch (mutations.size()) {
                        case 1 -> Response.noReply();
                        case 2 -> Response.committed(VsrOperation.DELETE_STREAM, 2, transientResult(1010));
                        default ->
                            Response.committed(
                                    VsrOperation.DELETE_STREAM,
                                    3,
                                    Unpooled.buffer().writeIntLE(0));
                    };
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofMillis(300))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            byte[] target = {1, 4, 1, 0, 0, 0};
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var lost = client.sendBinaryRequest(CommandCode.Stream.DELETE.getValue(), target);
                assertThatThrownBy(() -> lost.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class);
                IggyOperationException uncertain;
                try {
                    lost.join();
                    throw new AssertionError("Missing uncertainty");
                } catch (java.util.concurrent.CompletionException error) {
                    uncertain = (IggyOperationException) error.getCause();
                }
                assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                assertThatThrownBy(() -> client.sendBinaryRequest(CommandCode.Stream.DELETE.getValue(), target)
                                .join())
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.LANE_BLOCKED));
                assertThat(mutations).hasSize(1);
                assertThatThrownBy(() -> client.resolveOperation(uncertain.getOperationId(), Duration.ofSeconds(1))
                                .join())
                        .hasCauseInstanceOf(IggyServerException.class)
                        .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                                .isEqualTo(1010));
                client.sendBinaryRequest(CommandCode.Stream.DELETE.getValue(), target)
                        .get(5, TimeUnit.SECONDS);
                assertThat(mutations).hasSize(3);
                assertThat(mutations.get(1).requestId())
                        .isEqualTo(mutations.get(0).requestId());
                assertThat(mutations.get(1).clientLow())
                        .isEqualTo(mutations.get(0).clientLow());
                assertThat(mutations.get(1).clientHigh())
                        .isEqualTo(mutations.get(0).clientHigh());
                assertThat(mutations.get(1).body()).isEqualTo(mutations.get(0).body());
                assertThat(mutations.get(2).requestId())
                        .isNotEqualTo(mutations.get(0).requestId());
                for (Duration timeout : List.of(Duration.ofSeconds(1), Duration.ofSeconds(Long.MAX_VALUE))) {
                    assertThatThrownBy(() -> client.resolveOperation(uncertain.getOperationId(), timeout)
                                    .join())
                            .hasCauseInstanceOf(IggyServerException.class)
                            .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                                    .isEqualTo(1010));
                }
                assertThat(mutations).hasSize(3);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRetainCancelledTypedMutationUntilItsExactOperationIsResolved() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> written = new CompletableFuture<>();
            List<Request> mutations = new CopyOnWriteArrayList<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM) {
                    mutations.add(request);
                    written.complete(request);
                    return mutations.size() == 1
                            ? Response.noReply()
                            : Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(5))
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                var stream = org.apache.iggy.identifier.StreamId.of(1L);
                var cancelled = client.streams().deleteStream(stream);
                long operation = written.get(5, TimeUnit.SECONDS).requestId();
                cancelled.cancel(false);
                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        client.streams().deleteStream(stream).orTimeout(1, TimeUnit.SECONDS),
                        IggyErrorCode.LANE_BLOCKED.getCode());
                assertThat(mutations).hasSize(1);
                client.resolveOperation(operation, Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS);
                assertThat(mutations).hasSize(2);
                assertThat(mutations.get(1).requestId()).isEqualTo(operation);
                client.streams().deleteStream(stream).get(5, TimeUnit.SECONDS);
                assertThat(mutations).hasSize(3);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldStopUncertainProgressWithoutReplayAndKeepReadsRetryable() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            List<Request> polls = new CopyOnWriteArrayList<>();
            List<Request> stores = new CopyOnWriteArrayList<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.POLL.getValue()) {
                    polls.add(request);
                    if (polls.size() <= 4) {
                        return polls.size() % 2 == 0
                                ? Response.noReply()
                                : Response.error(VsrOperation.NON_REPLICATED, 57);
                    }
                    return polls.size() == 5
                            ? Response.error(VsrOperation.NON_REPLICATED, 57)
                            : Response.success(
                                    VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.emptyPoll(0));
                }
                if (request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    stores.add(request);
                    return Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofMillis(250))
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            var stream = org.apache.iggy.identifier.StreamId.of(1L);
            var topic = org.apache.iggy.identifier.TopicId.of(1L);
            var consumer = org.apache.iggy.consumergroup.Consumer.of(7L);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                for (int index = 0; index < 4; index++) {
                    boolean autoCommit = index >= 2;
                    var strategy = autoCommit
                            ? org.apache.iggy.message.PollingStrategy.offset(BigInteger.ZERO)
                            : org.apache.iggy.message.PollingStrategy.next();
                    var polled = client.messages()
                            .pollMessages(stream, topic, Optional.of(0L), consumer, strategy, 1L, autoCommit);
                    AsyncIggyTcpClientTransientFailoverTest.assertServerError(polled, 57);
                    assertThat(polls).hasSize(index + 1);
                    long requestId = polls.get(index).requestId();
                    assertThatThrownBy(() -> client.resolveOperation(requestId, Duration.ofSeconds(1))
                                    .join())
                            .hasCauseInstanceOf(IggyOperationException.class)
                            .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                    .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
                    client.consumerOffsets()
                            .storeConsumerOffset(stream, topic, Optional.of(0L), consumer, BigInteger.valueOf(index))
                            .get(5, TimeUnit.SECONDS);
                }
                client.messages()
                        .pollMessages(
                                stream,
                                topic,
                                Optional.of(0L),
                                consumer,
                                org.apache.iggy.message.PollingStrategy.offset(BigInteger.ZERO),
                                1L,
                                false)
                        .get(5, TimeUnit.SECONDS);
                assertThat(polls).hasSize(6);
                assertThat(stores).hasSize(4);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}, rejected={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void shouldShareOneProvedReplayWithConcurrentAndLaterResolvers(boolean routed, boolean rejected) throws Exception {
        try (var write = new UncertainWrite(routed, rejected)) {
            write.makeUncertain();
            var first = write.resolve(Duration.ofSeconds(5));
            write.replayed.get(5, TimeUnit.SECONDS);
            var concurrent = write.resolve(Duration.ofSeconds(5));
            assertThat(concurrent).isNotDone();

            write.release.complete(null);

            assertThat(outcome(first)).isEqualTo(write.proof());
            assertThat(outcome(concurrent)).isEqualTo(write.proof());
            assertThat(outcome(write.resolve(Duration.ofSeconds(1)))).isEqualTo(write.proof());
            assertThat(write.writes).hasSize(2).extracting(Request::requestId).containsOnly(write.operation);
            write.assertFreshWriteKeepsNoOutcome();
        }
    }

    @ParameterizedTest(name = "routed={0}")
    @ValueSource(booleans = {false, true})
    void shouldEndAResolveAsUncertainWhenItsWaitForARunningReplayExpires(boolean routed) throws Exception {
        try (var write = new UncertainWrite(routed, false)) {
            write.makeUncertain();
            var running = write.resolve(Duration.ofSeconds(5));
            write.replayed.get(5, TimeUnit.SECONDS);

            assertThat(outcome(write.resolve(Duration.ofMillis(100))))
                    .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_UNCERTAIN.getCode());
            assertThat(write.writes).hasSize(2);

            write.release.complete(null);
            assertThat(outcome(running)).isEqualTo(write.proof());
            write.assertFreshWriteKeepsNoOutcome();
        }
    }

    @ParameterizedTest(name = "routed={0}")
    @ValueSource(booleans = {false, true})
    void shouldKeepTheOutcomeThatAReplayProvesAfterAnAbandon(boolean routed) throws Exception {
        try (var write = new UncertainWrite(routed, false)) {
            write.makeUncertain();
            var replay = write.resolve(Duration.ofSeconds(5));
            write.replayed.get(5, TimeUnit.SECONDS);
            write.client.abandonOperation(write.operation).get(5, TimeUnit.SECONDS);

            write.release.complete(null);

            assertThat(outcome(replay)).isEqualTo(write.proof());
            assertThat(outcome(write.resolve(Duration.ofSeconds(1)))).isEqualTo(write.proof());
            assertThat(write.writes).hasSize(2);
            write.assertFreshWriteKeepsNoOutcome();
        }
    }

    @ParameterizedTest(name = "routed={0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldKeepTheLaneOfAnAbandonedReplayUntilTheReplayProvesItsOutcome(boolean routed) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> replaying = new CompletableFuture<>();
            AtomicBoolean held = new AtomicBoolean(true);
            AtomicBoolean linkLost = new AtomicBoolean();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(42));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                    return Response.success(
                            VsrOperation.NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
                }
                if (request.operation() != VsrOperation.DELETE_STREAM
                        && request.operation() != VsrOperation.STORE_CONSUMER_OFFSET) {
                    throw new IllegalStateException("Unexpected request " + request);
                }
                writes.add(request);
                if (writes.size() == 1) {
                    return Response.noReply();
                }
                if (request.requestId() != writes.get(0).requestId()) {
                    return Response.committed(
                            request.operation(), 3, Unpooled.buffer().writeIntLE(0));
                }
                replaying.complete(null);
                if (held.get()) {
                    return Response.error(request.operation(), IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode());
                }
                if (!linkLost.getAndSet(true)) {
                    return Response.disconnect();
                }
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0).writeBytes(UncertainWrite.PROOF));
            });
            // One pending operation, and bytes for two frames: the frame of the replay and one more.
            var client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofMillis(300))
                    .heartbeatInterval(Duration.ofHours(1))
                    .pendingOperations(1, 3L * VsrHeaders.HEADER_SIZE)
                    .build();
            String proof = "commit " + HexFormat.of().formatHex(UncertainWrite.PROOF);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        writeMutation(client, routed), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
                Request original = writes.get(0);
                var replay = client.resolveOperation(original.requestId(), Duration.ofSeconds(5));
                replaying.get(5, TimeUnit.SECONDS);
                var waiting = client.resolveOperation(original.requestId(), Duration.ofSeconds(5));

                client.abandonOperation(original.requestId()).get(5, TimeUnit.SECONDS);

                var queued = writeMutation(client, routed);
                // The write ends in the lane queue at its deadline. A routed call reports that as 57.
                assertThatThrownBy(() -> queued.get(5, TimeUnit.SECONDS))
                        .rootCause()
                        .isInstanceOfSatisfying(
                                IggyServerException.class,
                                refusal -> assertThat(refusal.getRawErrorCode())
                                        .isIn(TRANSIENT_NOT_ACCEPTED, IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode()));
                writeMutation(client, !routed).get(5, TimeUnit.SECONDS);
                assertThat(waiting).isNotDone();
                held.set(false);

                assertThat(outcome(replay)).isEqualTo(proof);
                assertThat(outcome(waiting)).isEqualTo(proof);
                assertThat(outcome(client.resolveOperation(original.requestId(), Duration.ofSeconds(1))))
                        .isEqualTo(proof);
                List<Request> lane = writes.stream()
                        .filter(request -> request.operation() == original.operation())
                        .toList();
                assertThat(lane).allSatisfy(request -> {
                    assertThat(request.requestId()).isEqualTo(original.requestId());
                    assertThat(request.body()).isEqualTo(original.body());
                });
                assertThat(lane.get(lane.size() - 1).connection()).isEqualTo(original.connection() + 1);
                writeMutation(client, routed).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @ParameterizedTest(name = "routed={0}, eviction={1}")
    @CsvSource({"false,NO_SESSION", "false,STALE_CLIENT", "true,NO_SESSION", "true,SESSION_TOO_LOW"})
    void shouldEndTheSentMutationUncertainAndRestartTheUnsentCallUnderTheNextSession(
            boolean routed, SessionEviction eviction) throws Exception {
        try (LostSession lost = new LostSession(eviction, true)) {
            var calls = lost.writeTwoAndEvict(routed);

            assertThatThrownBy(() -> calls.sent().get(5, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                        assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                        assertThat(uncertain.getOperationId())
                                .isEqualTo(lost.writes.get(0).requestId());
                    });
            calls.unsent().get(5, TimeUnit.SECONDS);
            // The sent write never went out again. The unsent one went out once, under the next session.
            assertThat(lost.registers).hasSize(2);
            assertThat(lost.writes).hasSize(2);
            Request restarted = lost.writes.get(1);
            assertThat(restarted.clientLow()).isEqualTo(lost.registers.get(1).clientLow());
            assertThat(restarted.clientHigh()).isEqualTo(lost.registers.get(1).clientHigh());
        }
    }

    @Test
    void shouldEndAnUnsentCallWith58WhenNoSessionIsBoundByItsDeadline() throws Exception {
        try (LostSession lost = new LostSession(SessionEviction.NO_SESSION, false)) {
            var calls = lost.writeTwoAndEvict(false);

            AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                    calls.sent(), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
            AsyncIggyTcpClientTransientFailoverTest.assertServerError(calls.unsent(), TRANSIENT_NOT_ACCEPTED);
            assertThat(lost.registers).hasSize(2);
            assertThat(lost.writes).hasSize(1);
        }
    }

    @ParameterizedTest(name = "refusal={0}")
    @ValueSource(ints = {TRANSIENT_NOT_ACCEPTED, AsyncTcpConnection.NOT_PRIMARY})
    void shouldPaceRefusedBindsAndEndTheSentMutationUncertainAtItsDeadline(int refusal) throws Exception {
        try (LostSession lost = new LostSession(SessionEviction.NO_SESSION, true, refusal)) {
            var calls = lost.writeTwoAndEvict(false);

            AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                    calls.sent(), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
            // The sent write stays retained in the lane that the unsent one waits for.
            AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                    calls.unsent(), IggyErrorCode.LANE_BLOCKED.getCode());
            assertThat(lost.registers).hasSize(1);
            assertThat(lost.writes).hasSize(1);
            assertThat(lost.binds).hasValueLessThan(MAX_PACED_BINDS);
        }
    }

    @Test
    void shouldEndAnUnsentCallWith58AtItsDeadlineWhenItsLinkDropsBeforeItsWriteAndEveryBindIsRefused()
            throws Exception {
        try (DroppedLink dropped = new DroppedLink()) {
            var unsent = dropped.admitOnTheDroppedLink();

            AsyncIggyTcpClientTransientFailoverTest.assertServerError(unsent, TRANSIENT_NOT_ACCEPTED);
            assertThat(dropped.deletes).isEmpty();
            assertThat(dropped.binds).hasValueLessThan(MAX_PACED_BINDS);
        }
    }

    /**
     * A node refuses the first write with 89 and drops its link. The next link finds the session lost: the node
     * refuses its bind, and the sign-in gets a new session. No node admitted the write, so it goes out again under
     * that session with a new request id and does not end uncertain.
     */
    @Test
    void shouldSendARefusedWriteAgainUnderTheNextSessionWhenTheSessionIsLostBeforeItsRetry() throws Exception {
        // The link that refuses the write and the link that finds the session lost.
        int connections = 2;
        try (ServerSocket socket = new ServerSocket(0, connections, InetAddress.getLoopbackAddress())) {
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> writes = new CopyOnWriteArrayList<>();
            var server = serve(socket, connections, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    return Response.success(VsrOperation.REGISTER, registerBody(registers.size()));
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                }
                if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                    return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() != VsrOperation.DELETE_STREAM) {
                    throw new IllegalStateException("Unexpected request " + request);
                }
                writes.add(request);
                return writes.size() == 1
                        ? Response.errorAndDisconnect(VsrOperation.DELETE_STREAM, AsyncTcpConnection.NOT_PRIMARY)
                        : Response.committed(
                                VsrOperation.DELETE_STREAM, 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(2))
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                writeMutation(client, false).get(5, TimeUnit.SECONDS);

                assertThat(registers).hasSize(2);
                assertThat(writes).hasSize(2);
                Request retried = writes.get(1);
                assertThat(retried.requestId()).isNotEqualTo(writes.get(0).requestId());
                assertThat(retried.clientLow()).isEqualTo(registers.get(1).clientLow());
                assertThat(retried.clientHigh()).isEqualTo(registers.get(1).clientHigh());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /** The outcome of a resolve as text: the reply body of a commit, or the type and code of its error. */
    private static String outcome(CompletableFuture<byte[]> resolved) throws Exception {
        try {
            return "commit " + HexFormat.of().formatHex(resolved.get(5, TimeUnit.SECONDS));
        } catch (ExecutionException error) {
            return error.getCause() instanceof IggyServerException server
                    ? server.getClass().getSimpleName() + " " + server.getRawErrorCode()
                    : error.getCause().toString();
        }
    }

    /** A routed offset write in partition 0, or a stream delete in the metadata lane. */
    private static CompletableFuture<byte[]> writeMutation(AsyncIggyTcpClient client, boolean routed) {
        return routed
                ? client.sendBinaryRequest(
                        CommandCode.ConsumerOffset.STORE.getValue(), AsyncIggyTcpClientRawRequestTest.rawStoreOffset())
                : client.sendBinaryRequest(CommandCode.Stream.DELETE.getValue(), new byte[] {1, 4, 1, 0, 0, 0});
    }

    /**
     * One node and one uncertain write: the first write of a mutation gets no reply. Every replay of it waits
     * for {@link #release} and then gets a committed reply, a commit or a rejection. Other writes commit. One
     * pending operation and two headers of bytes fit, so a leaked slot or frame stalls the next write.
     */
    private static final class UncertainWrite implements AutoCloseable {
        private static final byte[] PROOF = {7, 8, 9};
        private static final int REJECTION = IggyErrorCode.STREAM_NAME_NOT_FOUND.getCode();

        private final boolean routed;
        private final boolean rejected;
        private final ServerSocket socket;
        private final CompletableFuture<Void> server;
        private final AsyncIggyTcpClient client;
        private final List<Request> writes = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Request> replayed = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private long operation;

        UncertainWrite(boolean routed, boolean rejected) throws IOException {
            this.routed = routed;
            this.rejected = rejected;
            socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            server = serve(socket, this::respond);
            client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofMillis(300))
                    .heartbeatInterval(Duration.ofHours(1))
                    .pendingOperations(1, 2L * VsrHeaders.HEADER_SIZE)
                    .build();
        }

        void makeUncertain() throws Exception {
            client.connect().get(5, TimeUnit.SECONDS);
            client.login().get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> writeMutation(client, routed).get(5, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                        assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                        assertThat(uncertain.getOperationId())
                                .isEqualTo(writes.get(0).requestId());
                    });
            operation = writes.get(0).requestId();
        }

        CompletableFuture<byte[]> resolve(Duration timeout) {
            return client.resolveOperation(operation, timeout);
        }

        String proof() {
            return rejected
                    ? "IggyResourceNotFoundException " + REJECTION
                    : "commit " + HexFormat.of().formatHex(PROOF);
        }

        /** A write that was never uncertain commits, and the client keeps no outcome for it. */
        void assertFreshWriteKeepsNoOutcome() throws Exception {
            writeMutation(client, routed).get(5, TimeUnit.SECONDS);
            long fresh = writes.get(writes.size() - 1).requestId();
            assertThat(fresh).isNotEqualTo(operation);
            assertThat(outcome(client.resolveOperation(fresh, Duration.ofSeconds(1))))
                    .isEqualTo("IggyOperationException " + IggyErrorCode.OPERATION_NOT_RETAINED.getCode());
        }

        private Response respond(Request request) {
            if (request.operation() == VsrOperation.REGISTER) {
                return Response.success(VsrOperation.REGISTER, registerBody(42));
            }
            if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
            }
            if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 42, 1, socket.getLocalPort()));
            }
            if (request.operation() != VsrOperation.DELETE_STREAM
                    && request.operation() != VsrOperation.STORE_CONSUMER_OFFSET) {
                throw new IllegalStateException("Unexpected request " + request);
            }
            writes.add(request);
            if (writes.size() == 1) {
                return Response.noReply();
            }
            if (request.requestId() != writes.get(0).requestId()) {
                return Response.committed(
                        request.operation(), 3, Unpooled.buffer().writeIntLE(0));
            }
            replayed.complete(request);
            release.join();
            return Response.committed(
                    request.operation(),
                    2,
                    rejected
                            ? transientResult(REJECTION)
                            : Unpooled.buffer().writeIntLE(0).writeBytes(PROOF));
        }

        @Override
        public void close() throws Exception {
            release.complete(null);
            try {
                client.close().get(5, TimeUnit.SECONDS);
            } finally {
                socket.close();
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * One node that loses the session of the first write: it holds that write until a second write waits in the
     * same lane, then evicts it and answers every bind of the lost session with {@code bindReply}. The sign-in after
     * a refused bind gets a new session, or no reply when the session does not recover. Later writes commit.
     */
    private static final class LostSession implements AutoCloseable {
        // The evicted link and the link that recovers the session.
        private static final int CONNECTIONS = 2;
        private static final long FIRST_SESSION = 42;
        private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);
        // The second write needs it to reach the lane of the first one.
        private static final Duration QUEUE_TIME = Duration.ofMillis(200);

        private final SessionEviction eviction;
        private final boolean recovers;
        private final int bindReply;
        private final ServerSocket socket;
        private final CompletableFuture<Void> server;
        private final AsyncIggyTcpClient client;
        private final List<Request> registers = new CopyOnWriteArrayList<>();
        private final List<Request> writes = new CopyOnWriteArrayList<>();
        private final AtomicInteger binds = new AtomicInteger();
        private final CompletableFuture<Void> firstSent = new CompletableFuture<>();
        private final CompletableFuture<Void> evict = new CompletableFuture<>();

        LostSession(SessionEviction eviction, boolean recovers) throws IOException {
            this(eviction, recovers, IggyErrorCode.UNAUTHENTICATED.getCode());
        }

        LostSession(SessionEviction eviction, boolean recovers, int bindReply) throws IOException {
            this.eviction = eviction;
            this.recovers = recovers;
            this.bindReply = bindReply;
            socket = new ServerSocket(0, CONNECTIONS, InetAddress.getLoopbackAddress());
            server = serve(socket, CONNECTIONS, this::respond);
            client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(REQUEST_TIMEOUT)
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
        }

        /** Sends a write, queues a second one in its lane and then evicts the session of the first. */
        Calls writeTwoAndEvict(boolean routed) throws Exception {
            client.connect().get(5, TimeUnit.SECONDS);
            client.login().get(5, TimeUnit.SECONDS);
            var sent = writeMutation(client, routed);
            firstSent.get(5, TimeUnit.SECONDS);
            var unsent = writeMutation(client, routed);
            Thread.sleep(QUEUE_TIME.toMillis());
            evict.complete(null);
            return new Calls(sent, unsent);
        }

        private long session() {
            return FIRST_SESSION + registers.size() - 1;
        }

        private Response respond(Request request) {
            if (request.operation() == VsrOperation.REGISTER) {
                registers.add(request);
                return registers.size() == 1 || recovers
                        ? Response.success(VsrOperation.REGISTER, registerBody(session()))
                        : Response.noReply();
            }
            if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                binds.incrementAndGet();
                return Response.error(VsrOperation.NON_REPLICATED, bindReply);
            }
            if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
            }
            if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        AsyncIggyTcpClientTransientFailoverTest.pollRoute(
                                request, session(), 1, socket.getLocalPort()));
            }
            if (request.operation() != VsrOperation.DELETE_STREAM
                    && request.operation() != VsrOperation.STORE_CONSUMER_OFFSET) {
                throw new IllegalStateException("Unexpected request " + request);
            }
            writes.add(request);
            if (writes.size() > 1) {
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            }
            firstSent.complete(null);
            evict.join();
            return Response.eviction(eviction.reason);
        }

        @Override
        public void close() throws Exception {
            evict.complete(null);
            try {
                client.close().get(5, TimeUnit.SECONDS);
            } finally {
                socket.close();
            }
            server.get(5, TimeUnit.SECONDS);
        }

        private record Calls(CompletableFuture<byte[]> sent, CompletableFuture<byte[]> unsent) {}
    }

    /**
     * One node and a client with one pending operation. An unanswered offset write ends uncertain and keeps that
     * operation, so a delete waits for it in its own free lane. A ping that the node answers with an eviction drops
     * the link under the waiting delete, and every bind on the next link is refused with {@code NOT_PRIMARY}.
     */
    private static final class DroppedLink implements AutoCloseable {
        // The dropped link and the link that never binds.
        private static final int CONNECTIONS = 2;
        private static final long SESSION = 42;
        private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(1);
        // The delete needs it to reach its wait for the pending operation.
        private static final Duration QUEUE_TIME = Duration.ofMillis(200);
        private static final long PENDING_BYTES = 4L * VsrHeaders.HEADER_SIZE;

        private final ServerSocket socket;
        private final CompletableFuture<Void> server;
        private final AsyncIggyTcpClient client;
        private final List<Request> offsetWrites = new CopyOnWriteArrayList<>();
        private final List<Request> deletes = new CopyOnWriteArrayList<>();
        private final AtomicInteger binds = new AtomicInteger();

        DroppedLink() throws IOException {
            socket = new ServerSocket(0, CONNECTIONS, InetAddress.getLoopbackAddress());
            server = serve(socket, CONNECTIONS, this::respond);
            client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(REQUEST_TIMEOUT)
                    .heartbeatInterval(Duration.ofHours(1))
                    .pendingOperations(1, PENDING_BYTES)
                    .build();
        }

        /** Returns the delete, which the client admits on the dropped link once the offset write is abandoned. */
        CompletableFuture<byte[]> admitOnTheDroppedLink() throws Exception {
            client.connect().get(5, TimeUnit.SECONDS);
            client.login().get(5, TimeUnit.SECONDS);
            AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                    writeMutation(client, true), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
            var delete = writeMutation(client, false);
            Thread.sleep(QUEUE_TIME.toMillis());
            assertThatThrownBy(() -> client.system().ping().get(5, TimeUnit.SECONDS));
            client.abandonOperation(offsetWrites.get(0).requestId()).get(5, TimeUnit.SECONDS);
            return delete;
        }

        private Response respond(Request request) {
            if (request.operation() == VsrOperation.REGISTER) {
                return Response.success(VsrOperation.REGISTER, registerBody(SESSION));
            }
            if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                binds.incrementAndGet();
                return Response.error(VsrOperation.NON_REPLICATED, AsyncTcpConnection.NOT_PRIMARY);
            }
            if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
            }
            if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, SESSION, 1, socket.getLocalPort()));
            }
            if (request.commandCode() == CommandCode.System.PING.getValue()) {
                return Response.eviction(SessionEviction.NO_SESSION.reason);
            }
            if (request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                offsetWrites.add(request);
                return Response.noReply();
            }
            if (request.operation() == VsrOperation.DELETE_STREAM) {
                deletes.add(request);
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            }
            throw new IllegalStateException("Unexpected request " + request);
        }

        @Override
        public void close() throws Exception {
            try {
                client.close().get(5, TimeUnit.SECONDS);
            } finally {
                socket.close();
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    private enum LostLink {
        DISCONNECT,
        STALE_CLIENT_EVICTION,
        UNPROVED_REPLY
    }

    /** How the server ends the session of a write whose reply was lost. */
    private enum SessionEnd {
        REFUSED_BIND,
        EVICTED_BIND,
        REFUSED_SIGN_IN
    }

    /** The eviction reasons that the client reads as unauthenticated, with their wire codes. */
    private enum SessionEviction {
        NO_SESSION(1),
        STALE_CLIENT(EVICTION_STALE_CLIENT),
        SESSION_TOO_LOW(7),
        SESSION_RELEASE_MISMATCH(8),
        USER_INACTIVE(11),
        SESSION_ERROR(12);

        private final int reason;

        SessionEviction(int reason) {
            this.reason = reason;
        }
    }
}
