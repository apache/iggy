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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.serde.CommandCode;
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

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.transientResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientRecoveryTest {
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
                assertThatThrownBy(() -> client.resolveOperation(uncertain.getOperationId(), Duration.ofSeconds(1))
                                .join())
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> assertThat(((IggyOperationException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.OPERATION_NOT_RETAINED));
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
}
