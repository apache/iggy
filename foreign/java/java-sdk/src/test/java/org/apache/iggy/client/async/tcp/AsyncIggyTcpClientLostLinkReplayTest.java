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
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.CREATE_STREAM_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_CREATE_STREAM;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.threeNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A request that replays after a lost link is not routed, so it belongs on the metadata primary. The replay tries
 * the primary that the roster names before the next node in roster order.
 */
class AsyncIggyTcpClientLostLinkReplayTest {
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final int BACKLOG = 4;
    private static final int MAX_REPLAY_ROUNDS = 100;
    private static final int PING_CODE = CommandCode.System.PING.getValue();

    @Test
    void shouldReplayALostMutationOnTheMetadataPrimaryBeforeTheNextRosterNode() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket lostSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket nextSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket primarySocket = new ServerSocket(0, BACKLOG, loopback)) {
            AtomicBoolean moved = new AtomicBoolean();
            List<Request> lostMutations = new CopyOnWriteArrayList<>();
            List<Request> nextMutations = new CopyOnWriteArrayList<>();
            List<Request> primaryMutations = new CopyOnWriteArrayList<>();
            Supplier<ByteBuf> roster = () -> threeNodeMetadata(
                    lostSocket.getLocalPort(),
                    nextSocket.getLocalPort(),
                    primarySocket.getLocalPort(),
                    moved.get() ? primarySocket.getLocalPort() : lostSocket.getLocalPort());
            CompletableFuture<Void> lost = serve(lostSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, roster.get());
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    lostMutations.add(request);
                    return Response.disconnect();
                }
                throw new IllegalStateException("Unexpected request to the lost node: " + request);
            });
            CompletableFuture<Void> next = serve(nextSocket, request -> {
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    nextMutations.add(request);
                    return Response.error(OPERATION_CREATE_STREAM, AsyncTcpConnection.NOT_PRIMARY);
                }
                return answerDataSlot(request, roster);
            });
            CompletableFuture<Void> primary = serve(primarySocket, request -> {
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    primaryMutations.add(request);
                    return Response.success(
                            OPERATION_CREATE_STREAM, Unpooled.buffer().writeIntLE(0));
                }
                return answerDataSlot(request, roster);
            });
            AsyncIggyTcpClient client = client(lostSocket);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                moved.set(true);
                assertThat(client.findLeaderElsewhere(client.getConnectionInfo())
                                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .map(ConnectionInfo::port)
                        .contains(primarySocket.getLocalPort());

                client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(lostMutations).hasSize(1);
                assertThat(nextMutations).as("replays on the next roster node").isEmpty();
                assertThat(primaryMutations)
                        .singleElement()
                        .extracting(Request::requestId)
                        .isEqualTo(lostMutations.get(0).requestId());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            lost.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            next.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            primary.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldPaceRepeatedLostLinksUntilTheMutationDeadline() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, BACKLOG, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            var serving = serve(socket, MAX_REPLAY_ROUNDS * 2, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    writes.add(request);
                    return Response.disconnect();
                }
                throw new IllegalStateException("Unexpected request " + request);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]),
                        IggyErrorCode.OPERATION_UNCERTAIN.getCode());
                assertThat(writes).hasSizeGreaterThan(1);
                assertThat(writes.size())
                        .as("paced replays inside one deadline")
                        .isLessThan(MAX_REPLAY_ROUNDS);
                assertThat(writes)
                        .extracting(Request::requestId)
                        .containsOnly(writes.get(0).requestId());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(serving).isNotCompletedExceptionally();
        }
    }

    /**
     * The link of the client drops, and the next one drops during the bind that a mutation waits for. The mutation
     * sent nothing, so it waits for another link within its deadline and goes out there once, as in Rust.
     */
    @Test
    void shouldSendAnUnsentMutationOnTheNextLinkWhenItsLinkDropsDuringTheBind() throws Exception {
        // The link of the sign-in, the link whose bind drops and the link that carries the mutation.
        int links = 3;
        try (ServerSocket socket = new ServerSocket(0, links, InetAddress.getLoopbackAddress())) {
            AtomicInteger binds = new AtomicInteger();
            List<Request> writes = new CopyOnWriteArrayList<>();
            var serving = serve(socket, links, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return binds.incrementAndGet() == 1
                            ? Response.disconnect()
                            : Response.success(OPERATION_NON_REPLICATED, bindBody(request));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    writes.add(request);
                    return Response.success(
                            OPERATION_CREATE_STREAM, Unpooled.buffer().writeIntLE(0));
                }
                return answerSignIn(request, socket);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                dropTheLink(client);

                client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(binds).hasValue(2);
                assertThat(writes).hasSize(1);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            serving.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /** No link opens after the link of the client drops, so a mutation that sent nothing ends with 58 at its deadline. */
    @Test
    void shouldEndAnUnsentMutationWith58WhenNoLinkOpensBeforeItsDeadline() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, BACKLOG, InetAddress.getLoopbackAddress())) {
            List<Request> writes = new CopyOnWriteArrayList<>();
            var serving = serve(socket, request -> {
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    writes.add(request);
                    return Response.success(
                            OPERATION_CREATE_STREAM, Unpooled.buffer().writeIntLE(0));
                }
                return answerSignIn(request, socket);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofHours(1))
                    .build();
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                dropTheLink(client);
                serving.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                socket.close();

                AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                        client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]),
                        IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
                assertThat(writes).isEmpty();
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    /** Sends a ping that the node answers by dropping the link. The ping went out, so it fails at once. */
    private static void dropTheLink(AsyncIggyTcpClient client) {
        assertThatThrownBy(() ->
                        client.sendBinaryRequest(PING_CODE, new byte[0]).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
    }

    private static Response answerSignIn(Request request, ServerSocket socket) {
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(1));
        }
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(
                    OPERATION_NON_REPLICATED,
                    AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata(socket.getLocalPort()));
        }
        if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
            return Response.disconnect();
        }
        throw new IllegalStateException("Unexpected request " + request);
    }

    private static Response answerDataSlot(Request request, Supplier<ByteBuf> roster) {
        if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
        }
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(OPERATION_NON_REPLICATED, roster.get());
        }
        throw new IllegalStateException("Unexpected request to a data slot: " + request);
    }
}
