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
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientTransientFailoverTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;
    private static final int REPLY_COMMIT_OFFSET = 184;
    private static final int REQUEST_CLIENT_OFFSET = 128;
    private static final int EVICTION_REASON_OFFSET = 255;

    private static final int COMMAND_REPLY = 8;
    private static final int COMMAND_EVICTION = 13;
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int OPERATION_LOGOUT = 3;
    private static final int OPERATION_CREATE_STREAM = 128;
    private static final int GET_CLUSTER_METADATA_CODE = 12;
    private static final int CREATE_STREAM_CODE = 202;
    private static final int TRANSIENT_NOT_ACCEPTED = 58;
    private static final int EVICTION_STALE_CLIENT = 13;
    private static final int GET_POLL_ROUTING_CODE = 103;
    private static final int POLL_ON_PRIMARY_CODE = 104;
    private static final int ATTACH_CONSUMER_SESSION_CODE = 14;
    private static final int POLL_CODE = 100;
    private static final int GROUP_SYNC_CODE = 606;
    private static final int GROUP_JOIN_OPERATION = 148;
    private static final int UPDATE_USER_OPERATION = 142;
    private static final int CHANGE_PASSWORD_OPERATION = 144;
    private static final int TRANSIENT_NOT_COMMITTED = 57;
    private static final int POLL_PARAMETERS_BYTES = 14;

    @Test
    void shouldRetainSuccessfulSelfRenameAndPasswordChangeForConfiguredLogin() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            List<String> logins = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(serverSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    logins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(logins.size()));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == UPDATE_USER_OPERATION || request.operation() == CHANGE_PASSWORD_OPERATION) {
                    return Response.committed(
                            request.operation(), 11, Unpooled.buffer().writeIntLE(0));
                }
                if (request.operation() == OPERATION_LOGOUT) {
                    return Response.success(OPERATION_LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(serverSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                client.users()
                        .updateUser(UserId.of(1L), Optional.of("renamed-user"), Optional.empty())
                        .get(5, TimeUnit.SECONDS);
                client.users()
                        .changePassword(UserId.of("renamed-user"), "configured-password", "new-password")
                        .get(5, TimeUnit.SECONDS);
                client.users().logout().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                assertThat(logins).hasSize(2);
                assertThat(logins.get(0)).contains("configured-user").contains("configured-password");
                assertThat(logins.get(1))
                        .contains("renamed-user")
                        .contains("new-password")
                        .doesNotContain("configured-user")
                        .doesNotContain("configured-password");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldRecheckMetadataAfterWaitingForPrimaryAndAfterAttaching() throws Exception {
        for (boolean holdAttachment : new boolean[] {false, true}) {
            InetAddress loopback = InetAddress.getLoopbackAddress();
            try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                    ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
                CompletableFuture<Void> held = new CompletableFuture<>();
                CompletableFuture<Void> release = new CompletableFuture<>();
                AtomicInteger polls = new AtomicInteger();
                AtomicInteger routes = new AtomicInteger();
                List<Long> attachments = new CopyOnWriteArrayList<>();
                List<Long> pollWatermarks = new CopyOnWriteArrayList<>();
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
                        routes.incrementAndGet();
                        return Response.success(
                                OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primarySocket.getLocalPort()));
                    }
                    if (request.operation() == OPERATION_CREATE_STREAM) {
                        return Response.committed(
                                OPERATION_CREATE_STREAM, 11, Unpooled.buffer().writeIntLE(0));
                    }
                    throw new IllegalStateException("Unexpected coordinator request: " + request);
                });
                CompletableFuture<Void> primary = serve(primarySocket, request -> {
                    if (request.operation() == OPERATION_REGISTER) {
                        return Response.success(OPERATION_REGISTER, registerBody(2));
                    }
                    if (request.is(ATTACH_CONSUMER_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                        attachments.add(ByteBuffer.wrap(request.body())
                                .order(ByteOrder.LITTLE_ENDIAN)
                                .getLong(24));
                        if (holdAttachment && attachments.size() == 1) {
                            awaitRelease(held, release);
                        }
                        return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                    }
                    if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                        pollWatermarks.add(attachments.get(attachments.size() - 1));
                        if (!holdAttachment && polls.incrementAndGet() == 1) {
                            awaitRelease(held, release);
                        }
                        return Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                    }
                    throw new IllegalStateException("Unexpected primary request: " + request);
                });
                AsyncIggyTcpClient client = client(coordinatorSocket);
                try {
                    client.connect().get(5, TimeUnit.SECONDS);
                    client.users().login("manual-user", "manual-password").get(5, TimeUnit.SECONDS);
                    CompletableFuture<PolledMessages> first = poll(client, Optional.of(0L), true);
                    held.get(5, TimeUnit.SECONDS);
                    CompletableFuture<PolledMessages> queued = holdAttachment
                            ? CompletableFuture.completedFuture(null)
                            : poll(client, Optional.of(0L), true);
                    client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                    release.complete(null);
                    first.get(5, TimeUnit.SECONDS);
                    queued.get(5, TimeUnit.SECONDS);
                    assertThat(attachments).containsExactly(1L, 11L);
                    assertThat(pollWatermarks)
                            .containsExactlyElementsOf(holdAttachment ? List.of(11L) : List.of(1L, 11L));
                    assertThat(routes).hasValue(2);
                } finally {
                    release.complete(null);
                    client.close().get(5, TimeUnit.SECONDS);
                }
                coordinator.get(5, TimeUnit.SECONDS);
                primary.get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void awaitRelease(CompletableFuture<Void> held, CompletableFuture<Void> release) {
        held.complete(null);
        try {
            release.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the metadata acknowledgement", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("Timed out waiting for the metadata acknowledgement", error);
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldRetryOnlyNonAdmissionAndDiscardCancelledGroupDataConnection() throws Exception {
        for (boolean cancel : new boolean[] {false, true}) {
            InetAddress loopback = InetAddress.getLoopbackAddress();
            try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                    ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
                AtomicInteger polls = new AtomicInteger();
                AtomicInteger dataLogins = new AtomicInteger();
                AtomicInteger parentLogins = new AtomicInteger();
                AtomicInteger attachments = new AtomicInteger();
                CompletableFuture<Void> firstPoll = new CompletableFuture<>();
                CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                    if (request.operation() == OPERATION_REGISTER) {
                        parentLogins.incrementAndGet();
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
                        return Response.success(
                                OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primarySocket.getLocalPort()));
                    }
                    if (request.is(GROUP_SYNC_CODE, OPERATION_NON_REPLICATED)) {
                        return Response.success(
                                OPERATION_NON_REPLICATED,
                                Unpooled.buffer().writeLongLE(1).writeIntLE(1).writeIntLE(0));
                    }
                    throw new IllegalStateException("Unexpected coordinator request: " + request);
                });
                CompletableFuture<Void> primary = serve(primarySocket, cancel ? 2 : 1, request -> {
                    if (request.operation() == OPERATION_REGISTER) {
                        dataLogins.incrementAndGet();
                        return Response.success(OPERATION_REGISTER, registerBody(2));
                    }
                    if (request.is(ATTACH_CONSUMER_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                        attachments.incrementAndGet();
                        return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                    }
                    if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                        if (polls.incrementAndGet() == 1) {
                            firstPoll.complete(null);
                            return cancel
                                    ? Response.noReply()
                                    : Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
                        }
                        return Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                    }
                    throw new IllegalStateException("Unexpected primary request: " + request);
                });
                AsyncIggyTcpClient client = client(coordinatorSocket);
                try {
                    client.connect().get(5, TimeUnit.SECONDS);
                    client.users().login("manual-user", "manual-password").get(5, TimeUnit.SECONDS);
                    CompletableFuture<PolledMessages> result = poll(client, Optional.empty(), true);
                    firstPoll.get(5, TimeUnit.SECONDS);
                    if (cancel) {
                        assertThat(result.cancel(false)).isTrue();
                        poll(client, Optional.empty(), true).get(5, TimeUnit.SECONDS);
                    } else {
                        result.get(5, TimeUnit.SECONDS);
                    }
                    assertThat(polls).hasValue(2);
                    assertThat(dataLogins).hasValue(cancel ? 2 : 1);
                    assertThat(attachments).hasValue(2);
                    assertThat(parentLogins).hasValue(1);
                    assertThat(client.getConnectionInfo().port()).isEqualTo(coordinatorSocket.getLocalPort());
                } finally {
                    client.close().get(5, TimeUnit.SECONDS);
                }
                coordinator.get(5, TimeUnit.SECONDS);
                primary.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldKeepCoordinatorMembershipAndReuseSplitPrimaryConnections() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                ServerSocket firstSocket = new ServerSocket(0, 4, loopback);
                ServerSocket secondSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger logins = new AtomicInteger();
            AtomicInteger joins = new AtomicInteger();
            AtomicInteger routes = new AtomicInteger();
            AtomicInteger ordinaryPolls = new AtomicInteger();
            AtomicInteger firstPolls = new AtomicInteger();
            AtomicInteger secondPolls = new AtomicInteger();
            AtomicInteger dataLogins = new AtomicInteger();
            AtomicReference<Request> parent = new AtomicReference<>();
            List<Long> attachedWatermarks = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    logins.incrementAndGet();
                    parent.set(request);
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            threeNodeMetadata(
                                    coordinatorSocket.getLocalPort(),
                                    firstSocket.getLocalPort(),
                                    secondSocket.getLocalPort()));
                }
                if (request.operation() == GROUP_JOIN_OPERATION) {
                    joins.incrementAndGet();
                    return Response.success(
                            GROUP_JOIN_OPERATION, Unpooled.buffer().writeIntLE(0));
                }
                if (request.is(GROUP_SYNC_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            Unpooled.buffer()
                                    .writeLongLE(1)
                                    .writeIntLE(2)
                                    .writeIntLE(0)
                                    .writeIntLE(1));
                }
                if (request.is(GET_POLL_ROUTING_CODE, OPERATION_NON_REPLICATED)) {
                    routes.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(
                                    request,
                                    1,
                                    1,
                                    partition(request) == 0
                                            ? firstSocket.getLocalPort()
                                            : secondSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    return Response.committed(
                            OPERATION_CREATE_STREAM, 11, Unpooled.buffer().writeIntLE(0));
                }
                if (request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    ordinaryPolls.incrementAndGet();
                    return Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            RequestHandler primary = request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    dataLogins.incrementAndGet();
                    assertThat(request.bodyAsText()).contains("manual-user").doesNotContain("configured-user");
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                if (request.is(ATTACH_CONSUMER_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    ByteBuffer attachment = ByteBuffer.wrap(request.body()).order(ByteOrder.LITTLE_ENDIAN);
                    assertThat(attachment.getLong()).isEqualTo(parent.get().clientLow());
                    assertThat(attachment.getLong()).isEqualTo(parent.get().clientHigh());
                    assertThat(attachment.getLong()).isEqualTo(1);
                    attachedWatermarks.add(attachment.getLong());
                    return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                    (partition(request) == 0 ? firstPolls : secondPolls).incrementAndGet();
                    return Response.success(OPERATION_NON_REPLICATED, emptyPoll(partition(request)));
                }
                throw new IllegalStateException("Unexpected primary request: " + request);
            };
            CompletableFuture<Void> first = serve(firstSocket, primary);
            CompletableFuture<Void> second = serve(secondSocket, primary);
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("manual-user", "manual-password").get(5, TimeUnit.SECONDS);
                client.consumerGroups()
                        .joinConsumerGroup(StreamId.of(1L), TopicId.of(1L), ConsumerId.of(7L))
                        .get(5, TimeUnit.SECONDS);
                for (int count = 0; count < 4; count++) {
                    poll(client, Optional.empty(), true).get(5, TimeUnit.SECONDS);
                }
                assertThat(routes).hasValue(2);
                assertThat(dataLogins).hasValue(2);
                assertThat(firstPolls).hasValue(2);
                assertThat(secondPolls).hasValue(2);
                client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                poll(client, Optional.of(0L), true).get(5, TimeUnit.SECONDS);
                poll(client, Optional.of(0L), true).get(5, TimeUnit.SECONDS);
                poll(client, Optional.of(0L), false).get(5, TimeUnit.SECONDS);
                assertThat(attachedWatermarks).containsExactly(1L, 1L, 11L);
                assertThat(routes).hasValue(3);
                assertThat(ordinaryPolls).hasValue(1);
                assertThat(logins).hasValue(1);
                assertThat(joins).hasValue(1);
                assertThat(client.getConnectionInfo().port()).isEqualTo(coordinatorSocket.getLocalPort());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldNotReplayUnknownPollAndUseUpdatedCredentialsOnReplacement() throws Exception {
        for (int failure : new int[] {TRANSIENT_NOT_COMMITTED, -1, 30}) {
            InetAddress loopback = InetAddress.getLoopbackAddress();
            try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                    ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
                AtomicInteger polls = new AtomicInteger();
                AtomicInteger logins = new AtomicInteger();
                AtomicInteger parentLogins = new AtomicInteger();
                CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                    if (request.operation() == OPERATION_REGISTER) {
                        parentLogins.incrementAndGet();
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
                        return Response.success(
                                OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primarySocket.getLocalPort()));
                    }
                    if (request.operation() == UPDATE_USER_OPERATION
                            || request.operation() == CHANGE_PASSWORD_OPERATION) {
                        return Response.committed(
                                request.operation(), 11, Unpooled.buffer().writeIntLE(0));
                    }
                    throw new IllegalStateException("Unexpected coordinator request: " + request);
                });
                CompletableFuture<Void> primary = serve(primarySocket, 2, request -> {
                    if (request.operation() == OPERATION_REGISTER) {
                        int attempt = logins.incrementAndGet();
                        assertThat(request.bodyAsText())
                                .contains(attempt == 1 ? "manual-user" : "renamed-user")
                                .contains(attempt == 1 ? "manual-password" : "new-password")
                                .doesNotContain("configured-user");
                        return Response.success(OPERATION_REGISTER, registerBody(2));
                    }
                    if (request.is(ATTACH_CONSUMER_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                        return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                    }
                    if (request.is(POLL_ON_PRIMARY_CODE, OPERATION_NON_REPLICATED)) {
                        if (polls.incrementAndGet() == 1) {
                            return failure == -1
                                    ? Response.disconnect()
                                    : Response.error(OPERATION_NON_REPLICATED, failure);
                        }
                        return Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
                    }
                    throw new IllegalStateException("Unexpected primary request: " + request);
                });
                AsyncIggyTcpClient client = client(coordinatorSocket);
                try {
                    client.connect().get(5, TimeUnit.SECONDS);
                    client.users().login("manual-user", "manual-password").get(5, TimeUnit.SECONDS);
                    assertThatThrownBy(() -> poll(client, Optional.of(0L), true).get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(IggyServerException.class)
                            .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getRawErrorCode())
                                    .isEqualTo(TRANSIENT_NOT_COMMITTED));
                    assertThat(polls).hasValue(1);
                    client.users()
                            .updateUser(UserId.of(1L), Optional.of("renamed-user"), Optional.empty())
                            .get(5, TimeUnit.SECONDS);
                    client.users()
                            .changePassword(UserId.of("renamed-user"), "manual-password", "new-password")
                            .get(5, TimeUnit.SECONDS);
                    poll(client, Optional.of(0L), true).get(5, TimeUnit.SECONDS);
                    assertThat(polls).hasValue(2);
                    assertThat(logins).hasValue(2);
                    assertThat(parentLogins).hasValue(1);
                } finally {
                    client.close().get(5, TimeUnit.SECONDS);
                }
                coordinator.get(5, TimeUnit.SECONDS);
                primary.get(5, TimeUnit.SECONDS);
            }
        }
    }

    private static AsyncIggyTcpClient client(ServerSocket coordinator) {
        return AsyncIggyTcpClient.builder()
                .host(coordinator.getInetAddress().getHostAddress())
                .port(coordinator.getLocalPort())
                .credentials("configured-user", "configured-password")
                .requestTimeout(Duration.ofSeconds(2))
                .heartbeatInterval(Duration.ofMinutes(1))
                .build();
    }

    private static CompletableFuture<PolledMessages> poll(
            AsyncIggyTcpClient client, Optional<Long> partition, boolean autoCommit) {
        return client.messages()
                .pollMessages(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        partition,
                        Consumer.group(ConsumerId.of(7L)),
                        PollingStrategy.next(),
                        1L,
                        autoCommit);
    }

    private static int partition(Request request) {
        return ByteBuffer.wrap(request.body())
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt(request.body().length - POLL_PARAMETERS_BYTES - Integer.BYTES);
    }

    private static ByteBuf pollRoute(Request request, long session, long watermark, int port) {
        ByteBuf body = Unpooled.buffer()
                .writeLongLE(request.clientLow())
                .writeLongLE(request.clientHigh())
                .writeLongLE(session)
                .writeLongLE(watermark);
        writeNode(body, "primary", port, false);
        return body;
    }

    private static ByteBuf emptyPoll(int partition) {
        return Unpooled.buffer().writeIntLE(partition).writeLongLE(0).writeIntLE(0);
    }

    @Test
    void shouldWalkRosterAndReplayNotAcceptedMutation() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket oldLeaderSocket = new ServerSocket(0, 1, loopback);
                ServerSocket newLeaderSocket = new ServerSocket(0, 1, loopback)) {
            AtomicInteger denials = new AtomicInteger();
            AtomicInteger retriedMutations = new AtomicInteger();
            CompletableFuture<Void> oldLeader = serve(
                    oldLeaderSocket,
                    request -> handleOldLeader(
                            request, oldLeaderSocket.getLocalPort(), newLeaderSocket.getLocalPort(), denials));
            CompletableFuture<Void> newLeader = serve(
                    newLeaderSocket,
                    request -> handleNewLeader(
                            request, oldLeaderSocket.getLocalPort(), newLeaderSocket.getLocalPort(), retriedMutations));

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(oldLeaderSocket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(10))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                byte[] response = client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                        .get(10, TimeUnit.SECONDS);

                assertThat(response).isEmpty();
                assertThat(client.getConnectionInfo().port()).isEqualTo(newLeaderSocket.getLocalPort());
                assertThat(denials).hasValueGreaterThan(1);
                assertThat(retriedMutations).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            oldLeader.get(5, TimeUnit.SECONDS);
            newLeader.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldWalkPastTwoRefusingReplicasToThePartitionPrimary() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket metadataLeaderSocket = new ServerSocket(0, 1, loopback);
                ServerSocket followerSocket = new ServerSocket(0, 1, loopback);
                ServerSocket partitionPrimarySocket = new ServerSocket(0, 1, loopback)) {
            int metadataLeaderPort = metadataLeaderSocket.getLocalPort();
            int followerPort = followerSocket.getLocalPort();
            int partitionPrimaryPort = partitionPrimarySocket.getLocalPort();
            AtomicInteger accepted = new AtomicInteger();
            CompletableFuture<Void> metadataLeader = serve(metadataLeaderSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            threeNodeMetadata(metadataLeaderPort, followerPort, partitionPrimaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    return Response.error(OPERATION_CREATE_STREAM, TRANSIENT_NOT_ACCEPTED);
                }
                throw new IllegalStateException("Unexpected request to metadata leader: " + request);
            });
            CompletableFuture<Void> follower = serve(followerSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    return Response.error(OPERATION_CREATE_STREAM, TRANSIENT_NOT_ACCEPTED);
                }
                throw new IllegalStateException("Unexpected request to follower: " + request);
            });
            CompletableFuture<Void> partitionPrimary = serve(partitionPrimarySocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(3));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    accepted.incrementAndGet();
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                throw new IllegalStateException("Unexpected request to partition primary: " + request);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(metadataLeaderPort)
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(15))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                byte[] response = client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                        .get(15, TimeUnit.SECONDS);

                assertThat(response).isEmpty();
                assertThat(client.getConnectionInfo().port()).isEqualTo(partitionPrimaryPort);
                assertThat(accepted).hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            metadataLeader.get(5, TimeUnit.SECONDS);
            follower.get(5, TimeUnit.SECONDS);
            partitionPrimary.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldReplayTransientImplicitLoginAfterEviction() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 1, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            AtomicInteger mutations = new AtomicInteger();
            CompletableFuture<Void> server = serve(serverSocket, 2, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    int attempt = registrations.incrementAndGet();
                    if (attempt == 2) {
                        return Response.success(OPERATION_REGISTER, transientResult(TRANSIENT_NOT_ACCEPTED));
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(attempt));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (mutations.incrementAndGet() == 1) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations).hasValue(3);
                assertThat(mutations).hasValue(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Closing is caller intent, like a logout. A sign-in still in flight when
     * it happens must not put its credentials back: `connect()` clears the
     * closed flag, so the next connection loss would replay a session the
     * caller had ended.
     */
    @Test
    void shouldNotRememberASignInThatLandedAfterClose() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            CompletableFuture<Void> server = serve(serverSocket, 4, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    int attempt = registrations.incrementAndGet();
                    if (attempt > 1) {
                        // The second sign-in is the one racing the close.
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(attempt));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build();
            client.connect().get(5, TimeUnit.SECONDS);
            client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

            CompletableFuture<?> racing = client.users().login("iggy", "iggy");
            Thread.sleep(50);
            client.close().get(5, TimeUnit.SECONDS);
            racing.handle((ignored, error) -> null).get(5, TimeUnit.SECONDS);

            assertThat(client.hasRememberedLogin())
                    .as("a sign-in that landed after the close put its credentials back")
                    .isFalse();
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    /**
     * A stale-client eviction is not caller intent: the server's heartbeat
     * verifier sends it after a gc pause or a laptop sleep. A client that
     * signed in by hand recovers from it exactly like one whose credentials
     * were configured (the test above), and the sign-in it recovers with is the
     * one that last succeeded. Same rule in every SDK.
     */
    @Test
    void shouldReviveTheSignInAfterAStaleClientEviction() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            List<String> registeredLogins = new CopyOnWriteArrayList<>();
            AtomicBoolean evict = new AtomicBoolean(true);
            CompletableFuture<Void> server = serve(serverSocket, 6, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    registeredLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (evict.compareAndSet(true, false)) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            // No configured credentials: the only sign-in is the one run below.
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(2))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("handrun", "handrun").get(5, TimeUnit.SECONDS);
                int registrationsBeforeEviction = registrations.get();

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.hasRememberedLogin())
                        .as("an eviction is not a sign-out; the credentials stay")
                        .isTrue();

                // The next request brings the session back, under the sign-in
                // that last succeeded.
                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations.get())
                        .as("the evicted session was not re-established")
                        .isGreaterThan(registrationsBeforeEviction);
                assertThat(registeredLogins.get(registeredLogins.size() - 1)).contains("handrun");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    /**
     * Credentials on the builder and a hand-run sign-in for somebody else: the
     * revived session is the last sign-in, the same rule as on a redial and in
     * every other SDK. The connection re-authenticates a replacement channel
     * from the login it captured, which is that same sign-in, so replaying the
     * configured user here would make one eviction land on a different session
     * depending on which path got there first.
     */
    @Test
    void shouldReviveTheLastSignInRatherThanTheConfiguredOne() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            AtomicInteger registrations = new AtomicInteger();
            List<String> registeredLogins = new CopyOnWriteArrayList<>();
            AtomicBoolean evict = new AtomicBoolean(true);
            CompletableFuture<Void> server = serve(serverSocket, 6, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    registeredLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == OPERATION_CREATE_STREAM) {
                    if (evict.compareAndSet(true, false)) {
                        return Response.eviction(EVICTION_STALE_CLIENT);
                    }
                    ByteBuf body = Unpooled.buffer(Integer.BYTES);
                    body.writeIntLE(0);
                    return Response.success(OPERATION_CREATE_STREAM, body);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(serverSocket.getLocalPort())
                    .credentials("configured", "configured")
                    .requestTimeout(Duration.ofSeconds(2))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("handrun", "handrun").get(5, TimeUnit.SECONDS);
                int registrationsBeforeEviction = registrations.get();

                assertThatThrownBy(() -> client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                assertThat(client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isEmpty();
                assertThat(registrations.get())
                        .as("the evicted session was not re-established")
                        .isGreaterThan(registrationsBeforeEviction);
                assertThat(registeredLogins.get(registeredLogins.size() - 1))
                        .as("the revived session signed in as the configured user")
                        .contains("handrun")
                        .doesNotContain("configured");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.completeExceptionally(new IllegalStateException("test over"));
        }
    }

    private static Response handleOldLeader(
            Request request, int oldLeaderPort, int newLeaderPort, AtomicInteger denials) {
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            boolean demoted = denials.get() > 0;
            return Response.success(
                    OPERATION_NON_REPLICATED,
                    clusterMetadata(oldLeaderPort, newLeaderPort, demoted ? newLeaderPort : oldLeaderPort));
        }
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(1));
        }
        if (request.operation() == OPERATION_CREATE_STREAM) {
            denials.incrementAndGet();
            return Response.error(OPERATION_CREATE_STREAM, TRANSIENT_NOT_ACCEPTED);
        }
        throw new IllegalStateException("Unexpected request to old leader: " + request);
    }

    private static Response handleNewLeader(
            Request request, int oldLeaderPort, int newLeaderPort, AtomicInteger retriedMutations) {
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(
                    OPERATION_NON_REPLICATED, clusterMetadata(oldLeaderPort, newLeaderPort, newLeaderPort));
        }
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(2));
        }
        if (request.operation() == OPERATION_CREATE_STREAM) {
            retriedMutations.incrementAndGet();
            ByteBuf body = Unpooled.buffer(Integer.BYTES);
            body.writeIntLE(0);
            return Response.success(OPERATION_CREATE_STREAM, body);
        }
        throw new IllegalStateException("Unexpected request to new leader: " + request);
    }

    private static CompletableFuture<Void> serve(ServerSocket server, RequestHandler handler) {
        return serve(server, 1, handler);
    }

    /**
     * Runs blocking socket I/O on one dedicated daemon thread per mock node.
     * The common fork-join pool has only cores minus one workers on small CI
     * runners, so three blocking nodes can starve the client continuations the
     * test is waiting for when the full suite runs concurrently.
     */
    private static CompletableFuture<Void> serve(ServerSocket server, int connectionCount, RequestHandler handler) {
        CompletableFuture<Void> serving = new CompletableFuture<>();
        Thread serverThread = new Thread(
                () -> {
                    try {
                        for (int connection = 0; connection < connectionCount; connection++) {
                            try (Socket socket = server.accept()) {
                                InputStream input = socket.getInputStream();
                                OutputStream output = socket.getOutputStream();
                                Request request;
                                while ((request = readRequest(input)) != null) {
                                    Response response = handler.handle(request);
                                    if (response.command() == -1) {
                                        break;
                                    }
                                    writeResponse(output, request, response);
                                }
                            }
                        }
                        serving.complete(null);
                    } catch (IOException error) {
                        serving.completeExceptionally(new IllegalStateException("Mock VSR server failed", error));
                    } catch (RuntimeException error) {
                        serving.completeExceptionally(error);
                    }
                },
                "transient-failover-server-" + server.getLocalPort());
        serverThread.setDaemon(true);
        serverThread.start();
        return serving;
    }

    private static Request readRequest(InputStream input) throws IOException {
        byte[] header = input.readNBytes(HEADER_SIZE);
        if (header.length == 0) {
            return null;
        }
        if (header.length != HEADER_SIZE) {
            throw new EOFException("Truncated VSR request header");
        }
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        int size = fields.getInt(SIZE_OFFSET);
        byte[] body = input.readNBytes(size - HEADER_SIZE);
        if (body.length != size - HEADER_SIZE) {
            throw new EOFException("Truncated VSR request body");
        }
        return new Request(
                Byte.toUnsignedInt(header[REQUEST_OPERATION_OFFSET]),
                fields.getInt(REQUEST_CODE_OFFSET),
                fields.getLong(REQUEST_ID_OFFSET),
                fields.getLong(REQUEST_CLIENT_OFFSET),
                fields.getLong(REQUEST_CLIENT_OFFSET + Long.BYTES),
                body);
    }

    private static void writeResponse(OutputStream output, Request request, Response response) throws IOException {
        if (response.command() == -2) {
            return;
        }
        byte[] body = new byte[response.body().readableBytes()];
        response.body().readBytes(body);
        response.body().release();
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE + body.length);
        header[COMMAND_OFFSET] = (byte) response.command();
        if (response.command() == COMMAND_EVICTION) {
            header[EVICTION_REASON_OFFSET] = (byte) response.evictionReason();
        } else {
            fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
            header[REPLY_OPERATION_OFFSET] = (byte) response.operation();
            fields.putInt(REPLY_STATUS_OFFSET, response.status());
            fields.putLong(REPLY_COMMIT_OFFSET, response.commit());
        }
        output.write(header);
        output.write(body);
        output.flush();
    }

    private static ByteBuf registerBody(long session) {
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(1);
        body.writeLongLE(session);
        body.writeIntLE(11 << 10);
        body.writeByte(0);
        return body;
    }

    private static ByteBuf transientResult(int errorCode) {
        ByteBuf body = Unpooled.buffer(3 * Integer.BYTES);
        body.writeIntLE(1);
        body.writeIntLE(0);
        body.writeIntLE(errorCode);
        return body;
    }

    private static ByteBuf singleNodeMetadata(int port) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(1);
        writeNode(body, "node", port, true);
        return body;
    }

    private static ByteBuf clusterMetadata(int oldLeaderPort, int newLeaderPort, int leaderPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(2);
        writeNode(body, "old-node", oldLeaderPort, oldLeaderPort == leaderPort);
        writeNode(body, "new-node", newLeaderPort, newLeaderPort == leaderPort);
        return body;
    }

    private static ByteBuf threeNodeMetadata(int firstPort, int secondPort, int thirdPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(3);
        writeNode(body, "metadata-leader", firstPort, true);
        writeNode(body, "follower", secondPort, false);
        writeNode(body, "partition-primary", thirdPort, false);
        return body;
    }

    private static void writeNode(ByteBuf body, String name, int port, boolean leader) {
        writeString(body, name);
        writeString(body, InetAddress.getLoopbackAddress().getHostAddress());
        body.writeShortLE(port);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeShortLE(0);
        body.writeByte(leader ? 0 : 1);
        body.writeByte(0);
    }

    private static void writeString(ByteBuf body, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        body.writeIntLE(bytes.length);
        body.writeBytes(bytes);
    }

    private record Request(
            int operation, int commandCode, long requestId, long clientLow, long clientHigh, byte[] body) {
        boolean is(int expectedCode, int expectedOperation) {
            return commandCode == expectedCode && operation == expectedOperation;
        }

        /** The request body as text, for asserting which user a login names. */
        String bodyAsText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private record Response(int command, int operation, int status, int evictionReason, long commit, ByteBuf body) {
        static Response success(int operation, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, 0, body);
        }

        static Response error(int operation, int status) {
            return new Response(COMMAND_REPLY, operation, status, 0, 0, Unpooled.EMPTY_BUFFER);
        }

        static Response eviction(int reason) {
            return new Response(COMMAND_EVICTION, 0, 0, reason, 0, Unpooled.EMPTY_BUFFER);
        }

        static Response committed(int operation, long commit, ByteBuf body) {
            return new Response(COMMAND_REPLY, operation, 0, 0, commit, body);
        }

        static Response disconnect() {
            return new Response(-1, 0, 0, 0, 0, Unpooled.EMPTY_BUFFER);
        }

        static Response noReply() {
            return new Response(-2, 0, 0, 0, 0, Unpooled.EMPTY_BUFFER);
        }
    }

    @FunctionalInterface
    private interface RequestHandler {
        Response handle(Request request);
    }
}
