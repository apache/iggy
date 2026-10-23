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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.MockNode;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.iggy.client.async.AsyncClientIntegrationTest.clientChannels;
import static org.apache.iggy.client.async.AsyncClientIntegrationTest.field;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEventLoopGroupTest.await;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The session rules of the Rust SDK: a sign-in or a sign-out of the caller ends the calls made before it, a call
 * that waits for a session until its deadline ends with 58, and a refused automatic sign-in ends the waiting calls
 * with 40 and forgets the credentials.
 */
class AsyncIggyTcpClientSessionRulesTest {
    private static final int TIMEOUT_SECONDS = 5;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    // The second write needs it to reach the lane of the first one.
    private static final Duration QUEUE_TIME = Duration.ofMillis(200);
    private static final long FIRST_SESSION = 42;
    private static final int EVICTION_NO_SESSION = 1;
    private static final int DELETE_STREAM_CODE = CommandCode.Stream.DELETE.getValue();
    // Stream 1 as a numeric identifier.
    private static final byte[] DELETE_STREAM_BODY = {1, 4, 1, 0, 0, 0};
    private static final int GET_STATS_CODE = CommandCode.System.GET_STATS.getValue();

    /**
     * A write waits in the lane of a write without a reply while the caller signs out and in as another user. It
     * never goes out under the session of that user, and it ends with 40. The write that went out ends uncertain.
     */
    @ParameterizedTest(name = "signsOutFirst={0}")
    @ValueSource(booleans = {false, true})
    void shouldEndAWaitingWriteWith40WhenTheCallerSignsInAsAnotherUser(boolean signsOutFirst) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            List<Request> writes = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> firstWritten = new CompletableFuture<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(
                            VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.getAndIncrement()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.operation() == VsrOperation.DELETE_STREAM) {
                    writes.add(request);
                    firstWritten.complete(null);
                    // The first write holds its lane until its deadline.
                    return writes.size() == 1
                            ? Response.noReply()
                            : Response.committed(
                                    VsrOperation.DELETE_STREAM,
                                    2,
                                    Unpooled.buffer().writeIntLE(0));
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                var sent = client.sendBinaryRequest(DELETE_STREAM_CODE, DELETE_STREAM_BODY);
                firstWritten.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                var waiting = client.sendBinaryRequest(DELETE_STREAM_CODE, DELETE_STREAM_BODY);
                Thread.sleep(QUEUE_TIME.toMillis());

                if (signsOutFirst) {
                    client.users().logout().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThatThrownBy(() -> waiting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(
                                IggyServerException.class,
                                error -> assertThat(error.getRawErrorCode())
                                        .isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode()));
                assertThatThrownBy(() -> sent.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                            assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                            assertThat(uncertain.getOperationId())
                                    .isEqualTo(writes.get(0).requestId());
                        });
                assertThat(writes).as("the writes that reached the server").hasSize(1);
            } finally {
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    /**
     * The caller signs in as another user while signed in. The sign-in ends the lineage once, as it begins, and the
     * reply to the logout that it sends first keeps that lineage. So a wait for a session in that lineage lasts until
     * the sign-in binds its session.
     */
    @Test
    void shouldKeepTheLineageOfASignInWhenItsLogoutReplies() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            CompletableFuture<Void> logoutReceived = new CompletableFuture<>();
            CompletableFuture<Void> releaseLogout = new CompletableFuture<>();
            CompletableFuture<Void> secondRegisterReceived = new CompletableFuture<>();
            CompletableFuture<Void> releaseSecondRegister = new CompletableFuture<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    if (registers.getAndIncrement() == 1) {
                        secondRegisterReceived.complete(null);
                        releaseSecondRegister.join();
                    }
                    return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.get()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    logoutReceived.complete(null);
                    releaseLogout.join();
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                ConsensusSession session = (ConsensusSession) field(client, "session");
                var signIn = client.users().login("bob", "bob-password");
                logoutReceived.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                long lineage = session.lineage();
                releaseLogout.complete(null);
                secondRegisterReceived.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                var waiting = session.awaitSession(
                        lineage, System.nanoTime() + REQUEST_TIMEOUT.toNanos(), GlobalEventExecutor.INSTANCE);
                assertThat(waiting).isNotDone();
                releaseSecondRegister.complete(null);

                signIn.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                waiting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(session.lineage()).isEqualTo(lineage);
            } finally {
                releaseLogout.complete(null);
                releaseSecondRegister.complete(null);
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    /**
     * The caller signs in as another user while signed in, and a read and a routed poll start once the logout of
     * that sign-in replied. Both need a session, so they end with 40 at once, as in Rust, while the Register of the
     * sign-in still has no reply. The sign-in then completes.
     */
    @Test
    void shouldEndACallThatNeedsASessionWith40WhileTheCallerSignsIn() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            CompletableFuture<Void> secondRegisterReceived = new CompletableFuture<>();
            CompletableFuture<Void> releaseSecondRegister = new CompletableFuture<>();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    if (registers.getAndIncrement() == 1) {
                        secondRegisterReceived.complete(null);
                        releaseSecondRegister.join();
                    }
                    return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.get()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                var signIn = client.users().login("bob", "bob-password");
                secondRegisterReceived.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                var read = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                var poll = client.messages()
                        .pollMessages(
                                StreamId.of(1L),
                                TopicId.of(1L),
                                Optional.of(0L),
                                Consumer.of(ConsumerId.of(1L)),
                                PollingStrategy.offset(BigInteger.ZERO),
                                1L,
                                false);
                for (var call : List.of(read, poll)) {
                    assertThatThrownBy(() -> call.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    error -> assertThat(error.getRawErrorCode())
                                            .as(call == read ? "read" : "poll")
                                            .isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode()));
                }
                releaseSecondRegister.complete(null);

                signIn.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                releaseSecondRegister.complete(null);
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    @Test
    void shouldKeepANewerSignInWhenAnOlderLogoutCompletionResumes() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger registrations = new AtomicInteger();
            MockNode node = MockNode.serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(VsrOperation.REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .ioThreads(4)
                    .requestTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            CompletableFuture<Void> decoded = new CompletableFuture<>();
            CompletableFuture<Void> resume = new CompletableFuture<>();
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                for (var channel : clientChannels(client)) {
                    channel.eventLoop()
                            .submit(() -> {
                                var handler = channel.pipeline().get(VsrResponseHandler.class);
                                channel.pipeline()
                                        .addBefore(
                                                channel.pipeline()
                                                        .context(handler)
                                                        .name(),
                                                "hold-logout-completion",
                                                new ChannelInboundHandlerAdapter() {
                                                    @Override
                                                    public void channelRead(
                                                            ChannelHandlerContext context, Object message)
                                                            throws Exception {
                                                        if (message instanceof ByteBuf frame
                                                                && frame.getUnsignedByte(frame.readerIndex()
                                                                                + REPLY_OPERATION_OFFSET)
                                                                        == VsrOperation.LOGOUT) {
                                                            Map<?, ?> pending =
                                                                    (Map<?, ?>) field(handler, "pendingRequests");
                                                            var request = pending.get(frame.getLongLE(
                                                                    frame.readerIndex() + REPLY_REQUEST_ID_OFFSET));
                                                            CompletableFuture<?> response =
                                                                    (CompletableFuture<?>) field(request, "future");
                                                            response.whenComplete((value, error) -> {
                                                                decoded.complete(null);
                                                                resume.join();
                                                            });
                                                        }
                                                        context.fireChannelRead(message);
                                                    }
                                                });
                            })
                            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                var oldLogout = client.users().logout();
                decoded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(client.hasRememberedLogin()).isTrue();
                resume.complete(null);
                oldLogout.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(client.hasRememberedLogin()).isTrue();
                client.system().getClusterMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                resume.complete(null);
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = MultiRequest.class,
            names = {"BALANCED", "GROUP_RESYNC"})
    void shouldEndAMultiRequestCallWhenTheCallerChangesBetweenAcceptedReplies(MultiRequest operation) throws Exception {
        try (MultiRequestNode fixture = new MultiRequestNode()) {
            fixture.resync = operation == MultiRequest.GROUP_RESYNC;
            HeldRoutingMap<Object, Object> boundary = new HeldRoutingMap<>(!fixture.resync);
            var cache = ClientRoutingState.class.getDeclaredField(fixture.resync ? "assignments" : "balancedCursors");
            cache.setAccessible(true);
            cache.set(field(fixture.client, "routingState"), boundary);
            try {
                var call = fixture.call(operation);
                boundary.decoded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fixture.client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                fixture.client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                boundary.resume.complete(null);

                assertServerError(call, IggyErrorCode.UNAUTHENTICATED.getCode());
                assertThat(fixture.writes).isEmpty();
                assertThat(fixture.polls).hasValue(fixture.resync ? 1 : 0);
                assertThat(fixture.syncs).hasValue(fixture.resync ? 1 : 0);
                fixture.call(operation).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                boundary.resume.complete(null);
            }
        }
    }

    /** Pauses the decoded-count cursor or decoded re-sync invalidation inside the public continuation. */
    private static final class HeldRoutingMap<K, V> extends ConcurrentHashMap<K, V> {
        private final boolean balanced;
        private final CompletableFuture<Void> decoded = new CompletableFuture<>();
        private final CompletableFuture<Void> resume = new CompletableFuture<>();
        private final CompletableFuture<Void> published = new CompletableFuture<>();

        private HeldRoutingMap(boolean balanced) {
            this.balanced = balanced;
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            V value = super.compute(key, remappingFunction);
            published.complete(null);
            return value;
        }

        @Override
        public V computeIfAbsent(K key, Function<? super K, ? extends V> mapping) {
            V value = super.computeIfAbsent(key, mapping);
            if (balanced) {
                decoded.complete(null);
                resume.join();
            }
            return value;
        }

        @Override
        public V remove(Object key) {
            V value = super.remove(key);
            if (!balanced) {
                decoded.complete(null);
                resume.join();
            }
            return value;
        }
    }

    @Test
    void shouldPublishAnAssignmentWithoutTakingTheConnectionSessionMonitor() throws Exception {
        try (MultiRequestNode fixture = new MultiRequestNode()) {
            HeldGroupSync sync = new HeldGroupSync(fixture::connection);
            var syncField = MessagesTcpClient.class.getDeclaredField("consumerGroupsClient");
            syncField.setAccessible(true);
            syncField.set(fixture.client.messages(), sync);
            HeldRoutingMap<Object, Object> assignments = new HeldRoutingMap<>(true);
            var cache = ClientRoutingState.class.getDeclaredField("assignments");
            cache.setAccessible(true);
            cache.set(field(fixture.client, "routingState"), assignments);
            var poll = fixture.pollGroup();
            sync.decoded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            CompletableFuture<Void> publishing;
            synchronized (field(fixture.connection(), "consensusSession")) {
                publishing = CompletableFuture.runAsync(() -> sync.resume.complete(null));
                assignments.published.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            publishing.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).partitionId())
                    .isEqualTo(0);
        }
    }

    @ParameterizedTest(name = "automaticRecovery={0}")
    @ValueSource(booleans = {false, true})
    void shouldDiscardADecodedAssignmentAfterItsSessionChanges(boolean automaticRecovery) throws Exception {
        try (MultiRequestNode fixture = new MultiRequestNode()) {
            HeldGroupSync sync = new HeldGroupSync(fixture::connection);
            var syncField = MessagesTcpClient.class.getDeclaredField("consumerGroupsClient");
            syncField.setAccessible(true);
            syncField.set(fixture.client.messages(), sync);
            try {
                var oldPoll = fixture.pollGroup();
                sync.decoded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                long lineage = fixture.connection().sessionOwner().lineage();
                if (automaticRecovery) {
                    fixture.recover();
                    assertThat(fixture.connection().sessionOwner().lineage()).isEqualTo(lineage);
                } else {
                    fixture.client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                sync.resume.complete(null);
                if (automaticRecovery) {
                    assertThat(oldPoll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).partitionId())
                            .isEqualTo(1);
                    assertThat(fixture.syncs).hasValue(2);
                } else {
                    assertServerError(oldPoll, IggyErrorCode.UNAUTHENTICATED.getCode());
                    ClientRoutingState state = (ClientRoutingState) field(fixture.client, "routingState");
                    assertThat(state.assignment(MultiRequestNode.GROUP)).isEmpty();
                    assertThat(fixture.polls).hasValue(0);
                    assertThat(fixture.pollGroup()
                                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                    .partitionId())
                            .isEqualTo(1);
                }
            } finally {
                sync.resume.complete(null);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = MultiRequest.class,
            names = {"BALANCED", "KEYED"})
    void shouldContinueDiscoveryAndSendAfterAutomaticSessionRecovery(MultiRequest operation) throws Exception {
        try (MultiRequestNode fixture = new MultiRequestNode()) {
            fixture.loseRouteSession.set(true);
            fixture.node.rejectBindings = true;
            long lineage = fixture.connection().sessionOwner().lineage();
            fixture.call(operation).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(fixture.registrations.get()).isGreaterThan(1);
            assertThat(fixture.connection().sessionOwner().lineage()).isEqualTo(lineage);
            assertThat(fixture.writes).hasSize(1);
            long[] identity = fixture.connection().sessionIdentity();
            assertThat(fixture.writes.get(0).clientLow()).isEqualTo(identity[0]);
            assertThat(fixture.writes.get(0).clientHigh()).isEqualTo(identity[1]);
        }
    }

    private enum MultiRequest {
        BALANCED,
        KEYED,
        GROUP_RESYNC
    }

    /** Delays only the decoded Sync result, leaving the real connection available for session recovery. */
    private static final class HeldGroupSync extends ConsumerGroupsTcpClient {
        private final CompletableFuture<Void> decoded = new CompletableFuture<>();
        private final CompletableFuture<Void> resume = new CompletableFuture<>();

        private HeldGroupSync(Supplier<AsyncTcpConnection> connection) {
            super(connection);
        }

        @Override
        CompletableFuture<Optional<ConsumerGroupAssignment>> syncConsumerGroup(
                StreamId stream,
                TopicId topic,
                ConsumerId group,
                long deadline,
                AsyncTcpConnection.TransientFailoverState state) {
            return super.syncConsumerGroup(stream, topic, group, deadline, state)
                    .thenCompose(assignment -> {
                        decoded.complete(null);
                        return resume.thenApply(ignored -> assignment);
                    });
        }
    }

    private static final class MultiRequestNode implements AutoCloseable {
        private static final int POLL_PARAMETERS_BYTES = 14;
        private static final StreamId STREAM = StreamId.of(1L);
        private static final TopicId TOPIC = TopicId.of(1L);
        private static final Consumer CONSUMER = Consumer.group(ConsumerId.of(1L));
        private static final ClientRoutingState.GroupKey GROUP =
                ClientRoutingState.groupKey(STREAM, TOPIC, CONSUMER.id());
        private final ServerSocket socket;
        private final MockNode node;
        private final AsyncIggyTcpClient client;
        private final AtomicInteger registrations = new AtomicInteger();
        private final AtomicInteger syncs = new AtomicInteger();
        private final AtomicInteger polls = new AtomicInteger();
        private final List<Request> writes = new CopyOnWriteArrayList<>();
        private final AtomicBoolean loseRouteSession = new AtomicBoolean();
        private final AtomicBoolean loseMetadataSession = new AtomicBoolean();
        private volatile boolean resync;

        private MultiRequestNode() throws Exception {
            socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
            node = MockNode.serve(socket, this::respond);
            client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .ioThreads(4)
                    .requestTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                    .heartbeatInterval(Duration.ofMillis(100))
                    .build();
            client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
        private Response respond(Request request) {
            if (request.operation() == VsrOperation.REGISTER) {
                return Response.success(VsrOperation.REGISTER, registerBody(registrations.incrementAndGet()));
            }
            if (request.operation() == VsrOperation.LOGOUT) {
                return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
            }
            if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
                if (loseRouteSession.compareAndSet(true, false)) {
                    return Response.eviction(EVICTION_NO_SESSION);
                }
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        Unpooled.buffer()
                                .writeLongLE(1)
                                .writeLongLE(1)
                                .writeIntLE(0)
                                .writeIntLE(2)
                                .writeByte(0));
            }
            if (request.commandCode() == CommandCode.ConsumerGroup.SYNC.getValue()) {
                syncs.incrementAndGet();
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        Unpooled.buffer()
                                .writeLongLE(registrations.get())
                                .writeIntLE(1)
                                .writeIntLE(registrations.get() == 1 ? 0 : 1));
            }
            if (request.commandCode() == CommandCode.Messages.POLL.getValue()) {
                int number = polls.incrementAndGet();
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        emptyPoll(
                                resync && number == 1
                                        ? -1
                                        : ByteBuffer.wrap(request.body())
                                                .order(ByteOrder.LITTLE_ENDIAN)
                                                .getInt(request.body().length
                                                        - POLL_PARAMETERS_BYTES
                                                        - Integer.BYTES)));
            }
            if (request.operation() == VsrOperation.SEND_MESSAGES) {
                writes.add(request);
                return Response.committed(
                        VsrOperation.SEND_MESSAGES, 2, Unpooled.buffer().writeIntLE(0));
            }
            if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
                return loseMetadataSession.compareAndSet(true, false)
                        ? Response.eviction(EVICTION_NO_SESSION)
                        : metadata(socket, request);
            }
            if (request.commandCode() == CommandCode.System.PING.getValue()) {
                return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            }
            throw new IllegalStateException("Unexpected request: " + request);
        }

        private CompletableFuture<?> call(MultiRequest operation) {
            return operation == MultiRequest.GROUP_RESYNC
                    ? pollGroup()
                    : client.messages()
                            .sendMessages(
                                    STREAM,
                                    TOPIC,
                                    operation == MultiRequest.BALANCED
                                            ? Partitioning.balanced()
                                            : Partitioning.messagesKey("key"),
                                    List.of(Message.of("message")));
        }

        private CompletableFuture<PolledMessages> pollGroup() {
            return client.messages()
                    .pollMessages(
                            STREAM,
                            TOPIC,
                            Optional.empty(),
                            CONSUMER,
                            PollingStrategy.offset(BigInteger.ZERO),
                            1L,
                            false);
        }

        @SuppressWarnings("unchecked")
        private AsyncTcpConnection connection() {
            try {
                return ((AtomicReference<AsyncTcpConnection>) field(client, "connection")).get();
            } catch (ReflectiveOperationException error) {
                throw new IllegalStateException(error);
            }
        }

        private void recover() throws Exception {
            node.rejectBindings = true;
            loseMetadataSession.set(true);
            assertThatThrownBy(() -> client.system().getClusterMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            await(
                    () -> registrations.get() > 1
                            && connection().isAuthenticated()
                            && connection().sessionOwner().isBound(),
                    "automatic replacement session",
                    Duration.ofSeconds(TIMEOUT_SECONDS));
        }

        @Override
        public void close() throws Exception {
            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            node.close();
        }
    }

    enum RedialSignInEnd {
        RESUME,
        DEADLINE,
        CLOSE
    }

    @ParameterizedTest
    @EnumSource(RedialSignInEnd.class)
    void shouldWaitForTheAutomaticSignInOnAPublishedRedial(RedialSignInEnd ending) throws Exception {
        try (ServerSocket sourceSocket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress());
                ServerSocket targetSocket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            var registering = new CompletableFuture<Void>();
            var releaseRegister = new CompletableFuture<Void>();
            MockNode source = MockNode.serve(
                    sourceSocket,
                    request -> request.operation() == VsrOperation.REGISTER
                            ? Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION))
                            : request.commandCode() == CommandCode.System.PING.getValue()
                                    ? Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER)
                                    : metadata(sourceSocket, request));
            MockNode target = MockNode.serve(
                    targetSocket, request -> redialReply(targetSocket, request, registering, releaseRegister));
            try {
                AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                        .host(sourceSocket.getInetAddress().getHostAddress())
                        .port(sourceSocket.getLocalPort())
                        .requestTimeout(REQUEST_TIMEOUT)
                        .heartbeatInterval(Duration.ofMillis(50))
                        .build();
                try {
                    client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    ((ConsensusSession) field(client, "session")).reset();
                    client.retarget(new ConnectionInfo(
                                    targetSocket.getInetAddress().getHostAddress(), targetSocket.getLocalPort()))
                            .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    registering.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    var read = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                    var poll = client.messages()
                            .pollMessages(
                                    StreamId.of(1L),
                                    TopicId.of(1L),
                                    Optional.of(0L),
                                    Consumer.of(ConsumerId.of(1L)),
                                    PollingStrategy.offset(BigInteger.ZERO),
                                    1L,
                                    false);
                    Thread.sleep(QUEUE_TIME.toMillis());
                    assertThat(read).isNotDone();
                    assertThat(poll).isNotDone();
                    if (ending == RedialSignInEnd.RESUME) {
                        releaseRegister.complete(null);
                        read.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        poll.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    } else {
                        if (ending == RedialSignInEnd.CLOSE) {
                            client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        }
                        int code = ending == RedialSignInEnd.CLOSE
                                ? IggyErrorCode.UNAUTHENTICATED.getCode()
                                : IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode();
                        for (var waiting : List.of(read, poll)) {
                            assertThatThrownBy(() -> waiting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                                    .cause()
                                    .isInstanceOfSatisfying(
                                            IggyServerException.class,
                                            error -> assertThat(error.getRawErrorCode())
                                                    .as(waiting == read ? "read" : "poll")
                                                    .isEqualTo(code));
                        }
                    }
                } finally {
                    releaseRegister.complete(null);
                    client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            } finally {
                source.close();
                target.close();
            }
        }
    }

    private static Response redialReply(
            ServerSocket targetSocket,
            Request request,
            CompletableFuture<Void> registering,
            CompletableFuture<Void> releaseRegister) {

        if (request.operation() == VsrOperation.REGISTER) {
            registering.complete(null);
            releaseRegister.join();
            return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION + 1));
        }
        if (request.commandCode() == CommandCode.System.PING.getValue() || request.commandCode() == GET_STATS_CODE) {
            return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
        }
        if (request.commandCode() == CommandCode.Messages.GET_PARTITION_ROUTE.getValue()) {
            return Response.success(
                    VsrOperation.NON_REPLICATED,
                    AsyncIggyTcpClientTransientFailoverTest.pollRoute(request, 1, 1, targetSocket.getLocalPort()));
        }
        if (request.commandCode() == CommandCode.Messages.POLL.getValue()) {
            return Response.success(VsrOperation.NON_REPLICATED, emptyPoll(0));
        }
        return metadata(targetSocket, request);
    }

    /** A call whose automatic sign-in gets no reply sent nothing, so it ends with 58 at its deadline. */
    @Test
    void shouldEndACallWith58WhenTheSignInItWaitsForReachesItsDeadline() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return registers.getAndIncrement() == 0
                            ? Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION))
                            : Response.noReply();
                }
                return lostSession(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                loseTheSession(client);

                var waiting = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);

                assertThatThrownBy(() -> waiting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(
                                IggyServerException.class,
                                error -> assertThat(error.getRawErrorCode())
                                        .isEqualTo(IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()));
                assertThat(registers).as("the sign-ins").hasValue(2);
            } finally {
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    /**
     * The server refuses the automatic sign-in with a token that it no longer accepts. The client forgets the token,
     * so no other sign-in with it follows, and both calls that waited for the sign-in end with 40, with the refusal
     * as the cause.
     */
    @ParameterizedTest(name = "refusal={0}")
    @EnumSource(
            value = IggyErrorCode.class,
            names = {"INVALID_PERSONAL_ACCESS_TOKEN", "PERSONAL_ACCESS_TOKEN_EXPIRED"})
    void shouldForgetARefusedTokenAndEndTheWaitingCallsWith40(IggyErrorCode refusal) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            CompletableFuture<Void> refusalHeld = new CompletableFuture<>();
            CompletableFuture<Void> refuse = new CompletableFuture<>();
            var server = serve(socket, 2, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    if (registers.getAndIncrement() == 0) {
                        return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION));
                    }
                    // Holds the refusal until a second call waits for this sign-in.
                    refusalHeld.complete(null);
                    refuse.join();
                    return Response.error(VsrOperation.REGISTER, refusal.getCode());
                }
                return lostSession(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.personalAccessTokens()
                        .loginWithPersonalAccessToken("refused-token")
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                loseTheSession(client);

                var signingIn = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                refusalHeld.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                var waiting = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                refuse.complete(null);

                assertEndedByTheRefusal(signingIn, refusal);
                assertEndedByTheRefusal(waiting, refusal);
                assertSignedOut(client);
                assertThat(registers).as("the sign-ins with the token").hasValue(2);
            } finally {
                refuse.complete(null);
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    @Test
    void shouldReplaceAnAutomaticRegisterWithTheExplicitCallerCredentials() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> registers = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> automaticStarted = new CompletableFuture<>();
            AtomicBoolean bobSignedIn = new AtomicBoolean();
            var server = serve(socket, 4, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    if (request.bodyAsText().contains("bob")) {
                        bobSignedIn.set(true);
                        return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION + 1));
                    }
                    if (registers.size() == 1) {
                        return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION));
                    }
                    automaticStarted.complete(null);
                    return Response.noReply();
                }
                if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
                    return Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.commandCode() == GET_STATS_CODE) {
                    return bobSignedIn.get()
                            ? Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER)
                            : Response.eviction(EVICTION_NO_SESSION);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                loseTheSession(client);
                var oldCall = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                automaticStarted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThatThrownBy(() -> oldCall.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(
                                IggyServerException.class,
                                failure -> assertThat(failure.getRawErrorCode())
                                        .isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode()));
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(registers).hasSize(3);
                assertThat(registers.get(2).bodyAsText())
                        .contains("bob", "bob-password")
                        .doesNotContain("alice");
            } finally {
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    /**
     * Before its first sign-in, a call asks the caller to sign in. After a logout it fails with 40 until the caller
     * signs in again, as in Rust.
     */
    @Test
    void shouldEndCallsWith40AfterALogoutUntilTheCallerSignsInAgain() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            AtomicInteger reads = new AtomicInteger();
            var server = serve(socket, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(
                            VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.getAndIncrement()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.commandCode() == GET_STATS_CODE) {
                    reads.incrementAndGet();
                    return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThatThrownBy(() -> client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOf(IggyNotConnectedException.class);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().logout().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertSignedOut(client);
                assertThat(reads).hasValue(0);

                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(reads).hasValue(1);
            } finally {
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    @ParameterizedTest(name = "failure={0}")
    @EnumSource(ExplicitFailure.class)
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldRequireAnotherExplicitSignInAfterAnyFailedAttempt(ExplicitFailure failure) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean failing = new AtomicBoolean(true);
            List<Request> registers = new CopyOnWriteArrayList<>();
            AtomicInteger reads = new AtomicInteger();
            var server = serve(socket, 4, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    registers.add(request);
                    if (failing.get() && registers.size() > 1) {
                        if (failure == ExplicitFailure.REGISTER_REFUSED) {
                            return Response.error(VsrOperation.REGISTER, IggyErrorCode.INVALID_CREDENTIALS.getCode());
                        }
                        if (failure == ExplicitFailure.REGISTER_TIMEOUT) {
                            return Response.noReply();
                        }
                    }
                    return Response.success(VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.size()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    if (failing.get() && failure == ExplicitFailure.LOGOUT_REFUSED) {
                        return Response.error(VsrOperation.LOGOUT, IggyErrorCode.UNAUTHENTICATED.getCode());
                    }
                    return failing.get() && failure == ExplicitFailure.LOGOUT_TIMEOUT
                            ? Response.noReply()
                            : Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.commandCode() == GET_STATS_CODE) {
                    reads.incrementAndGet();
                    return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                Throwable rejected = catchThrowable(() -> {
                    switch (failure) {
                        case INVALID_USERNAME ->
                            client.users().login("", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        case INVALID_TOKEN ->
                            client.personalAccessTokens()
                                    .loginWithPersonalAccessToken("")
                                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        case MISSING_CREDENTIALS -> client.login().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        default -> client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                });
                Throwable cause = rejected instanceof ExecutionException ? rejected.getCause() : rejected;
                assertThat(cause).isInstanceOf(failure.errorType);
                if (cause instanceof IggyServerException refusal) {
                    assertThat(refusal.getRawErrorCode())
                            .isEqualTo(
                                    switch (failure) {
                                        case LOGOUT_REFUSED -> IggyErrorCode.UNAUTHENTICATED.getCode();
                                        case REGISTER_REFUSED -> IggyErrorCode.INVALID_CREDENTIALS.getCode();
                                        default -> IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode();
                                    });
                }
                assertSignedOut(client);
                assertThat(reads).hasValue(0);
                assertThat(registers)
                        .hasSize(
                                failure == ExplicitFailure.REGISTER_REFUSED
                                                || failure == ExplicitFailure.REGISTER_TIMEOUT
                                        ? 2
                                        : 1);

                failing.set(false);
                client.users().login("bob", "bob-password").get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(registers.get(registers.size() - 1).bodyAsText()).contains("bob", "bob-password");
                assertThat(reads).hasValue(1);
            } finally {
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    @ParameterizedTest(name = "ending={0}")
    @EnumSource(SettlementEnd.class)
    void shouldNotRestoreASignInAfterFailedSettlement(SettlementEnd ending) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicInteger registers = new AtomicInteger();
            var server = serve(socket, 4, request -> {
                if (request.operation() == VsrOperation.REGISTER) {
                    return Response.success(
                            VsrOperation.REGISTER, registerBody(FIRST_SESSION + registers.incrementAndGet()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.commandCode() == GET_STATS_CODE) {
                    return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                return metadata(socket, request);
            });
            var client = new HeldSettlementClient(socket);
            try {
                client.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                var first = client.users().login("alice", "alice-password");
                client.settlementStarted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                switch (ending) {
                    case QUEUED_TIMEOUT -> {
                        var later = client.users().login("bob", "bob-password");
                        assertServerError(later, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
                    }
                    case CANCELLED -> assertThat(first.cancel(false)).isTrue();
                    default -> {
                        client.settlement.completeExceptionally(new IllegalStateException("Roster unavailable"));
                        assertThatThrownBy(() -> first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                                .hasCauseInstanceOf(IllegalStateException.class);
                    }
                }
                client.settlement.complete(Optional.empty());
                first.handle((identity, error) -> null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertSignedOut(client);
                assertThat(registers).hasValue(1);
            } finally {
                client.settlement.complete(Optional.empty());
                client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            assertThat(server).isNotCompletedExceptionally();
        }
    }

    /** A call fails with 40 until the caller signs in again, as in Rust. */
    static void assertSignedOut(AsyncIggyTcpClient client) {
        assertServerError(
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]), IggyErrorCode.UNAUTHENTICATED.getCode());
    }

    private enum SettlementEnd {
        CANCELLED,
        QUEUED_TIMEOUT,
        ROSTER_FAILURE
    }

    private enum ExplicitFailure {
        INVALID_USERNAME(IggyInvalidArgumentException.class),
        INVALID_TOKEN(IggyInvalidArgumentException.class),
        MISSING_CREDENTIALS(IggyMissingCredentialsException.class),
        LOGOUT_REFUSED(IggyServerException.class),
        LOGOUT_TIMEOUT(IggyServerException.class),
        REGISTER_REFUSED(IggyServerException.class),
        REGISTER_TIMEOUT(IggyServerException.class);

        private final Class<? extends Throwable> errorType;

        ExplicitFailure(Class<? extends Throwable> errorType) {
            this.errorType = errorType;
        }
    }

    private static final class HeldSettlementClient extends AsyncIggyTcpClient {
        private final CompletableFuture<Void> settlementStarted = new CompletableFuture<>();
        private final CompletableFuture<Optional<ConnectionInfo>> settlement = new CompletableFuture<>();

        private HeldSettlementClient(ServerSocket socket) {
            super(
                    socket.getInetAddress().getHostAddress(),
                    socket.getLocalPort(),
                    null,
                    null,
                    null,
                    null,
                    REQUEST_TIMEOUT,
                    Duration.ofHours(1),
                    VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                    null,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    AsyncTcpConnection.DEFAULT_IO_THREADS);
        }

        @Override
        CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget, long deadline) {
            settlementStarted.complete(null);
            return settlement;
        }
    }

    private static AsyncIggyTcpClient client(ServerSocket socket) {
        return AsyncIggyTcpClient.builder()
                .host(socket.getInetAddress().getHostAddress())
                .port(socket.getLocalPort())
                .requestTimeout(REQUEST_TIMEOUT)
                .heartbeatInterval(Duration.ofHours(1))
                .build();
    }

    /** A read loses the session to an eviction, and the next call signs in again because no bind succeeds. */
    private static void loseTheSession(AsyncIggyTcpClient client) {
        assertThatThrownBy(() ->
                        client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
    }

    private static Response lostSession(ServerSocket socket, Request request) {
        if (request.commandCode() == GET_STATS_CODE) {
            return Response.eviction(EVICTION_NO_SESSION);
        }
        if (request.commandCode() == CommandCode.System.BIND_SESSION.getValue()) {
            return Response.error(VsrOperation.NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
        }
        return metadata(socket, request);
    }

    private static Response metadata(ServerSocket socket, Request request) {
        if (request.commandCode() == CommandCode.System.GET_CLUSTER_METADATA.getValue()) {
            return Response.success(VsrOperation.NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
        }
        throw new IllegalStateException("Unexpected request: " + request);
    }

    static void assertEndedByTheRefusal(CompletableFuture<byte[]> call, IggyErrorCode refusal) {
        assertThatThrownBy(() -> call.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .cause()
                .isInstanceOfSatisfying(IggyServerException.class, error -> {
                    assertThat(error.getRawErrorCode()).isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode());
                    assertThat(error.getCause())
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    cause -> assertThat(cause.getRawErrorCode()).isEqualTo(refusal.getCode()));
                });
    }
}
