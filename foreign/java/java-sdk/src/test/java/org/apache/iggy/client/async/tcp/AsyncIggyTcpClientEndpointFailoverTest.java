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
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The node a client signed in on dies; its next request has to complete on a
 * survivor the roster named, under a session established there. Mirrors
 * {@code core/integration/tests/cluster/failover_client_continuity.rs}. The
 * mock VSR framing matches {@link AsyncIggyTcpClientTransientFailoverTest},
 * kept separate so a death mid-connection cannot disturb that suite's server.
 */
class AsyncIggyTcpClientEndpointFailoverTest {
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int GET_CLUSTER_METADATA_CODE = 12;
    private static final int PING_CODE = 1;
    private static final int GET_STATS_CODE = 10;
    private static final int EVICTION_INVALID_CREDENTIALS = 9;
    private static final int EVICTION_SESSION_ERROR = 12;
    private static final int EVICTION_STALE_CLIENT = 13;
    private static final Duration HEARTBEAT = Duration.ofMillis(100);
    // One retry per burst, so an outage of a second outlasts many bursts.
    private static final RetryPolicy ONE_RETRY = RetryPolicy.fixedDelay(1, Duration.ofMillis(50));
    private static final Duration OUTAGE = Duration.ofSeconds(1);
    private static final Duration RECOVERY = Duration.ofSeconds(5);
    private static final int LOGIN_CODE = CommandCode.User.LOGIN.getValue();
    private static final int CHANGE_PASSWORD_CODE = CommandCode.User.CHANGE_PASSWORD.getValue();
    private static final int UPDATE_USER_CODE = CommandCode.User.UPDATE.getValue();
    // The user that registerBody signs in.
    private static final long SIGNED_IN_USER_ID = 1;
    private static final long OTHER_USER_ID = 2;
    private static final String USERNAME = "alice";
    private static final String RENAMED = "carol";
    private static final String OTHER_USERNAME = "bob";
    private static final String FIRST_PASSWORD = "first-secret";
    private static final String SECOND_PASSWORD = "second-secret";

    @Test
    void shouldCloseAnUnmanagedBootstrapAfterRetargeting() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket bootstrap = new ServerSocket(0, 4, loopback);
                ServerSocket target = new ServerSocket(0, 4, loopback)) {
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(bootstrap.getLocalPort())
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                try (Socket previous = bootstrap.accept()) {
                    previous.setSoTimeout(1000);
                    client.retarget(new ConnectionInfo(loopback.getHostAddress(), target.getLocalPort()))
                            .get(5, TimeUnit.SECONDS);
                    assertThat(previous.getInputStream().read()).isEqualTo(-1);
                }
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldKeepTheLatestLoginAfterReplicaPromotionAndSessionExpiration() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger leaderPort = new AtomicInteger(primaryPort);
            AtomicInteger primaryRegistrations = new AtomicInteger();
            AtomicBoolean expireNextRequest = new AtomicBoolean();
            List<String> survivorLogins = new CopyOnWriteArrayList<>();
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, leaderPort.get()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(primaryRegistrations.incrementAndGet()));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return expireNextRequest.compareAndSet(true, false)
                            ? Response.eviction(EVICTION_STALE_CLIENT)
                            : Response.success(
                                    OPERATION_NON_REPLICATED,
                                    clusterMetadata(primaryPort, survivorPort, leaderPort.get()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(primaryRegistrations.get() + 1));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(2))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("alice", "alice-password").get(5, TimeUnit.SECONDS);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), survivorPort))
                        .get(5, TimeUnit.SECONDS);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), primaryPort))
                        .get(5, TimeUnit.SECONDS);

                client.users().login("bob", "bob-password").get(5, TimeUnit.SECONDS);
                leaderPort.set(survivorPort);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), survivorPort))
                        .get(5, TimeUnit.SECONDS);
                assertThat(survivorLogins).isEmpty();

                survivor.rejectBindings = true;
                expireNextRequest.set(true);
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class)
                        .satisfies(error -> assertThat(((IggyServerException) error.getCause()).getErrorCode())
                                .isEqualTo(IggyErrorCode.STALE_CLIENT));
                client.system().getClusterMetadata().get(5, TimeUnit.SECONDS);

                assertThat(survivorLogins)
                        .singleElement()
                        .asString()
                        .contains("bob", "bob-password")
                        .doesNotContain("alice");
                assertThat(client.getConnectionInfo().port()).isEqualTo(survivorPort);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                survivor.close();
            }
        }
    }

    @Test
    void shouldResumeOnASurvivorAfterTheSignedInNodeDies() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger survivorRegistrations = new AtomicInteger();
            AtomicInteger survivorPings = new AtomicInteger();
            List<String> survivorLogins = new CopyOnWriteArrayList<>();

            // The primary leads, so the sign-in settles there and the roster is
            // only remembered -- not acted on -- until the node dies.
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, survivorPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorRegistrations.incrementAndGet();
                    survivorLogins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
                    survivorPings.incrementAndGet();
                }
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            });

            // Credentials on the builder and a hand-run login for somebody
            // else. The redial replays the sign-in that last succeeded, which
            // is also what the connection replays from the login it captured
            // when the pool swaps a channel: replaying a different user here
            // would make the same failure land on a different session depending
            // on which path got there first.
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .credentials("configured", "configured")
                    .requestTimeout(Duration.ofSeconds(2))
                    // A whole rotation dials every endpoint the client knows, so
                    // the survivor is reached before this delay is ever spent.
                    // One endpoint per attempt would need it first.
                    .retryPolicy(RetryPolicy.fixedDelay(8, Duration.ofSeconds(5)))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("handrun", "handrun").get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(PING_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(client.getConnectionInfo().port()).isEqualTo(primaryPort);

                primary.kill();

                // The request in flight when the node died is allowed to fail;
                // what is not allowed is never completing one, which is what a
                // client that only knows the dead endpoint does.
                assertThat(resumeWithin(client, Duration.ofSeconds(4)))
                        .as("the client has to resume on the survivor inside the first rotation")
                        .isTrue();

                assertThat(client.getConnectionInfo().port())
                        .as("the client moved off the dead endpoint")
                        .isEqualTo(survivorPort);
                assertThat(survivorRegistrations).hasValue(0);
                assertThat(survivor.bindings).hasValueGreaterThanOrEqualTo(1);
                assertThat(survivorPings)
                        .as("the request landed on the survivor")
                        .hasValueGreaterThanOrEqualTo(1);
                assertThat(survivorLogins).isEmpty();
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                survivor.close();
            }
        }
    }

    /**
     * An explicit sign-out is caller intent, like a close: neither the redial
     * after it nor a heartbeat may sign back in, with the credentials that
     * sign-in used or with the ones configured on the builder.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldNotResurrectASignedOutSessionOnASurvivor(boolean configured) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger survivorRegistrations = new AtomicInteger();

            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, survivorPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorRegistrations.incrementAndGet();
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                // Echoed, so a logout is answered as a logout: the client
                // checks the reply's operation against the request's.
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(2))
                    .heartbeatInterval(HEARTBEAT)
                    .retryPolicy(RetryPolicy.fixedDelay(4, Duration.ofMillis(50)));
            AsyncIggyTcpClient client = (configured ? builder.credentials("iggy", "iggy") : builder).build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                (configured ? client.login() : client.users().login("iggy", "iggy")).get(5, TimeUnit.SECONDS);
                client.users().logout().get(5, TimeUnit.SECONDS);

                primary.kill();

                // Every attempt is allowed to fail; none of them may register.
                resumeWithin(client, Duration.ofSeconds(2));

                assertThat(survivorRegistrations)
                        .as("a signed-out client has no session to restore")
                        .hasValue(0);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                survivor.close();
            }
        }
    }

    /**
     * A single node advertises its own view of its address, which a port map
     * or a port-forward makes unreachable. The client keeps to the endpoint it
     * was configured with: it adopts its link for that node, redials it after
     * a drop and replays a lost mutation on it.
     */
    @Test
    void shouldKeepASingleNodeOnTheConfiguredEndpoint() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket configuredSocket = new ServerSocket(0, 4, loopback);
                ServerSocket advertisedSocket = new ServerSocket(0, 4, loopback)) {
            int configuredPort = configuredSocket.getLocalPort();
            AtomicInteger deletions = new AtomicInteger();
            MockNode node = MockNode.serve(configuredSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata(
                                    advertisedSocket.getLocalPort()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == VsrOperation.DELETE_STREAM) {
                    return deletions.incrementAndGet() == 1
                            ? Response.disconnect()
                            : Response.committed(
                                    VsrOperation.DELETE_STREAM,
                                    2,
                                    Unpooled.buffer().writeIntLE(0));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode advertised = MockNode.serve(advertisedSocket, request -> {
                throw new IllegalStateException("The advertised address served " + request);
            });
            advertised.down();
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(configuredPort)
                    .requestTimeout(Duration.ofSeconds(2))
                    .heartbeatInterval(Duration.ofMillis(100))
                    .retryPolicy(RetryPolicy.fixedDelay(4, Duration.ofMillis(50)))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.down();
                Thread.sleep(300);
                node.up();
                assertThat(resumeWithin(client, Duration.ofSeconds(4))).isTrue();

                client.sendBinaryRequest(CommandCode.Stream.DELETE.getValue(), new byte[] {1, 4, 1, 0, 0, 0})
                        .get(5, TimeUnit.SECONDS);

                assertThat(deletions).as("the lost mutation was replayed").hasValue(2);
                assertThat(advertised.dials)
                        .as("dials to the advertised address")
                        .hasValue(0);
                assertThat(client.getConnectionInfo().port()).isEqualTo(configuredPort);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
                advertised.close();
            }
        }
    }

    @Test
    void shouldFailOverWhenTheCoordinatorStopsReplyingWithoutClosingItsSocket() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger leaderPort = new AtomicInteger(primaryPort);
            AtomicInteger survivorPings = new AtomicInteger();
            RequestHandler handler = request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, leaderPort.get()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            };
            MockNode primary = MockNode.serve(primarySocket, handler);
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
                    survivorPings.incrementAndGet();
                }
                return handler.handle(request);
            });
            AsyncIggyTcpClient client = limitedClient(primaryPort);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);
                leaderPort.set(survivorPort);
                primary.silent = true;
                int pingsBeforeFailure = survivorPings.get();

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> client.getConnectionInfo().port() == survivorPort
                                && survivorPings.get() > pingsBeforeFailure,
                        "automatic recovery from an open but silent coordinator",
                        RECOVERY);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                survivor.close();
            }
        }
    }

    /**
     * A retry budget bounds one redial burst, not the life of the client. After
     * an outage that outlasted many bursts, the heartbeat brings the link back
     * without a call, and the session resumes by binding.
     */
    @Test
    void shouldRestoreTheLinkAfterAnOutageLongerThanTheRetryBudget() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            int port = nodeSocket.getLocalPort();
            AtomicInteger registrations = new AtomicInteger();
            AtomicInteger pings = new AtomicInteger();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata(port));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
                    pings.incrementAndGet();
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(port);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.down();
                Thread.sleep(OUTAGE.toMillis());
                int pingsBeforeRecovery = pings.get();
                int bindingsBeforeRecovery = node.bindings.get();
                node.up();

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> pings.get() > pingsBeforeRecovery, "a heartbeat on the restored link", RECOVERY);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(node.bindings).hasValueGreaterThan(bindingsBeforeRecovery);
                assertThat(registrations)
                        .as("the client signed in again instead of resuming")
                        .hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * One stopped link among live ones: the replica link comes back by itself
     * after an outage that outlasted many bursts, while the coordinator stays
     * up.
     */
    @Test
    void shouldRestoreAReplicaLinkAfterAnOutageLongerThanTheRetryBudget() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket replicaSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int replicaPort = replicaSocket.getLocalPort();
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, replicaPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode replica = MockNode.serve(
                    replicaSocket, request -> Response.success(request.operation(), Unpooled.EMPTY_BUFFER));
            AsyncIggyTcpClient client = limitedClient(primaryPort);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> replica.bindings.get() > 0, "the replica link", RECOVERY);

                replica.down();
                Thread.sleep(OUTAGE.toMillis());
                int bindingsBeforeRecovery = replica.bindings.get();
                replica.up();

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> replica.bindings.get() > bindingsBeforeRecovery, "the restored replica link", RECOVERY);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                replica.close();
            }
        }
    }

    /**
     * The session was lost, and its sign-in lost the link every time until the
     * retry budget ran out. The heartbeat signs in again with a fresh budget,
     * because nothing else would: a ping needs no session, and a request
     * without one fails before it is sent.
     */
    @Test
    void shouldSignInAgainAfterTheRetryBudgetRanOutOnALostSession() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            AtomicInteger registrations = new AtomicInteger();
            AtomicInteger signIns = new AtomicInteger();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    registrations.incrementAndGet();
                    return hangUpOnRegister.get()
                            ? Response.disconnect()
                            : Response.success(OPERATION_REGISTER, registerBody(signIns.incrementAndGet()));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.rejectBindings = true;
                hangUpOnRegister.set(true);
                node.dropConnections();
                assertThatThrownBy(() -> client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                // The request's own sign-in, then the one the redial replays.
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> registrations.get() >= 3, "the redial sign-in", RECOVERY);
                Thread.sleep(OUTAGE.toMillis());
                node.rejectBindings = false;
                hangUpOnRegister.set(false);

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> signIns.get() >= 2, "a sign-in without a call", RECOVERY);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(signIns).as("sign-ins once the session was restored").hasValue(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * The server ends the session and the link that carries a request. The
     * node that refuses to bind the session is up, so the redial signs in
     * there instead of giving it up.
     */
    @Test
    void shouldSignInAgainOnANodeThatRefusesToBindTheSession() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean endSession = new AtomicBoolean();
            AtomicInteger signIns = new AtomicInteger();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(signIns.incrementAndGet()));
                }
                if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED) && endSession.getAndSet(false)) {
                    return Response.disconnect();
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.rejectBindings = true;
                endSession.set(true);
                assertThatThrownBy(() -> client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> signIns.get() >= 2, "a sign-in without a call", RECOVERY);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(signIns).as("sign-ins once the session was restored").hasValue(2);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * The redial gives the node that refused to bind the session one more try,
     * and that dial fails. The connection the client keeps still counts as
     * signed in, so no heartbeat signs in again, and the next request signs in
     * before it is sent instead of failing.
     */
    @Test
    void shouldSignInOnTheNextRequestWhenTheRetryAfterARefusedBindFails() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean endSession = new AtomicBoolean();
            AtomicInteger signIns = new AtomicInteger();
            AtomicInteger pings = new AtomicInteger();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(signIns.incrementAndGet()));
                }
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)) {
                    pings.incrementAndGet();
                }
                if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED) && endSession.getAndSet(false)) {
                    return Response.disconnect();
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.bindingHold = new CompletableFuture<>();
                node.rejectBindings = true;
                endSession.set(true);
                assertThatThrownBy(() -> client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                AsyncIggyTcpClientEventLoopGroupTest.await(() -> node.bindings.get() > 0, "the redial bind", RECOVERY);
                // The held bind blocks its own link, so a ping now comes from the kept connection on a new link.
                int pingsAtBind = pings.get();
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> pings.get() > pingsAtBind, "a heartbeat of the kept connection", RECOVERY);
                // The node stops accepting, so the retry after the refused bind fails to dial.
                nodeSocket.close();
                node.bindingHold.complete(null);
                Thread.sleep(OUTAGE.toMillis());
                assertThat(signIns).as("sign-ins without a request").hasValue(1);

                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(signIns).as("sign-ins once a request ran").hasValue(2);
            } finally {
                node.bindingHold.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * A poll link to the node of the client's own connection is not that
     * connection. When the router closes it on purpose while it carries a
     * request, the client keeps its connection and starts no redial.
     */
    @Test
    void shouldNotRedialWhenTheRouterClosesAPollLinkToTheSameNode() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicInteger signIns = new AtomicInteger();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(signIns.incrementAndGet()));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                // A move with a bound session opens a poll link and binds the session there before it moves.
                node.bindingHold = new CompletableFuture<>();
                client.retarget(client.getConnectionInfo());
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> node.bindings.get() > 0, "the bind on the poll link", RECOVERY);
                int dials = node.dials.get();

                // Signing out closes every poll link while the bind is still in flight.
                client.users().logout().get(5, TimeUnit.SECONDS);
                Thread.sleep(OUTAGE.toMillis());
                assertThat(node.dials).as("dials after the poll link closed").hasValue(dials);
                client.sendBinaryRequest(PING_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
                assertThat(node.dials)
                        .as("dials once the kept connection served a request")
                        .hasValue(dials);
            } finally {
                node.bindingHold.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * An explicit sign-in ends the automatic one as it starts. When the server
     * rejects it, the client stays signed out: a heartbeat that signed in as
     * the user before would undo the switch. A sign-in queued behind another
     * one keeps that rule too.
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldNotSignInAsThePreviousUserAfterARejectedSignIn(boolean queued) throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicInteger signIns = new AtomicInteger();
            CompletableFuture<Void> registered = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    if (request.bodyAsText().contains("other")) {
                        return Response.eviction(EVICTION_INVALID_CREDENTIALS);
                    }
                    int epoch = signIns.incrementAndGet();
                    registered.complete(null);
                    if (queued) {
                        release.join();
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(epoch));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                var signedIn = client.users().login("iggy", "iggy");
                registered.get(5, TimeUnit.SECONDS);
                if (!queued) {
                    signedIn.get(5, TimeUnit.SECONDS);
                }
                var rejected = client.users().login("other", "secret");
                release.complete(null);
                if (queued) {
                    assertThatThrownBy(() -> signedIn.get(5, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    error -> assertThat(error.getRawErrorCode())
                                            .isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode()));
                }
                assertThatThrownBy(() -> rejected.get(5, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(
                                IggyServerException.class,
                                error -> assertThat(error.getRawErrorCode())
                                        .isEqualTo(IggyErrorCode.INVALID_CREDENTIALS.getCode()));

                Thread.sleep(OUTAGE.toMillis());

                assertThat(signIns).as("sign-ins as the previous user").hasValue(1);
            } finally {
                release.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * The server keeps the user of each client id, also after its session
     * ended, and refuses another user on it. A sign-in that a lost session
     * owed can register the client id without its reply arriving, so an
     * explicit sign-in as another user has to start a new session.
     */
    @Test
    void shouldSignInAsAnotherUserAfterAnOwedSignInRegisteredTheClient() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            Map<List<Long>, String> owners = new ConcurrentHashMap<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() != OPERATION_REGISTER) {
                    return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
                }
                registers.add(request);
                String user = request.bodyAsText().contains("other") ? "other" : "iggy";
                String owner = owners.putIfAbsent(List.of(request.clientLow(), request.clientHigh()), user);
                if (owner != null && !owner.equals(user)) {
                    return Response.eviction(EVICTION_SESSION_ERROR);
                }
                return hangUpOnRegister.get() && "iggy".equals(user)
                        ? Response.disconnect()
                        : Response.success(OPERATION_REGISTER, registerBody(registers.size()));
            });
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);

                node.rejectBindings = true;
                hangUpOnRegister.set(true);
                node.dropConnections();
                assertThatThrownBy(() -> client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                                .get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                AsyncIggyTcpClientEventLoopGroupTest.await(() -> registers.size() >= 3, "the redial sign-in", RECOVERY);
                node.rejectBindings = false;

                client.users().login("other", "secret").get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);

                Request owed = registers.stream()
                        .filter(request -> !request.bodyAsText().contains("other"))
                        .reduce((first, second) -> second)
                        .orElseThrow();
                Request explicit = registers.stream()
                        .filter(request -> request.bodyAsText().contains("other"))
                        .findFirst()
                        .orElseThrow();
                assertThat(List.of(explicit.clientLow(), explicit.clientHigh()))
                        .isNotEqualTo(List.of(owed.clientLow(), owed.clientHigh()));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * An explicit sign-in does not wait behind an automatic one whose Register reply is late, because that one holds
     * the sign-in gate until its own deadline. It ends that sign-in, the late reply binds nothing, and the client
     * does not sign in as the previous user again.
     */
    @Test
    void shouldEndAnAutomaticSignInThatWaitsForItsReply() throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean lost = new AtomicBoolean();
            AtomicInteger owedSignIns = new AtomicInteger();
            CompletableFuture<Void> held = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<Request> calls = new CopyOnWriteArrayList<>();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() != OPERATION_REGISTER) {
                    calls.add(request);
                    return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
                }
                registers.add(request);
                if (lost.get() && request.bodyAsText().contains(USERNAME)) {
                    // The sign-in on the next link hangs up, and the one that the redial replays waits.
                    int owed = owedSignIns.incrementAndGet();
                    if (owed == 1) {
                        return Response.disconnect();
                    }
                    if (owed == 2) {
                        held.complete(null);
                        release.join();
                    }
                }
                return Response.success(OPERATION_REGISTER, registerBody(registers.size()));
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(InetAddress.getLoopbackAddress().getHostAddress())
                    .port(nodeSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(HEARTBEAT)
                    // Retries left after the held sign-in, which a redial that kept going would spend.
                    .retryPolicy(RetryPolicy.fixedDelay(4, Duration.ofMillis(50)))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                node.rejectBindings = true;
                lost.set(true);
                node.dropConnections();
                // A roster read never waits for a session, so it ends when the sign-in on the next link hangs up.
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                held.get(RECOVERY.toMillis(), TimeUnit.MILLISECONDS);
                node.rejectBindings = false;

                client.users().login(OTHER_USERNAME, SECOND_PASSWORD).get(5, TimeUnit.SECONDS);
                int dials = node.dials.get();
                release.complete(null);
                Thread.sleep(OUTAGE.toMillis());
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);

                assertThat(node.dials).as("dials after the explicit sign-in").hasValue(dials);

                List<Request> explicitSignIns = registers.stream()
                        .filter(register -> register.bodyAsText().contains(OTHER_USERNAME))
                        .toList();
                assertThat(explicitSignIns).as("the explicit sign-ins").hasSize(1);
                Request explicit = explicitSignIns.get(0);
                assertThat(registers).as("the sign-ins").endsWith(explicit);
                Request call = calls.get(calls.size() - 1);
                assertThat(List.of(call.clientLow(), call.clientHigh()))
                        .as("the client of the last call")
                        .isEqualTo(List.of(explicit.clientLow(), explicit.clientHigh()));
            } finally {
                release.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * A password change that commits during a reconnect burst survives a lost link, a refusal of the old
     * password, and an accepted older Register followed by another recovery.
     */
    @ParameterizedTest(name = "rejected={0}, accepted={1}")
    @CsvSource({"false,false", "true,false", "false,true"})
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldContinueTheBurstWithTheLoginThatARefreshReplaced(boolean rejected, boolean accepted) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, loopback);
                ServerSocket changeSocket = new ServerSocket(0, 4, loopback)) {
            AtomicBoolean lost = new AtomicBoolean();
            AtomicInteger owedSignIns = new AtomicInteger();
            CompletableFuture<Void> held = new CompletableFuture<>();
            CompletableFuture<Void> release = new CompletableFuture<>();
            CompletableFuture<Void> settled = new CompletableFuture<>();
            List<Request> registers = new CopyOnWriteArrayList<>();
            List<String> signInsAfterRelease = new CopyOnWriteArrayList<>();
            MockNode node = MockNode.serve(nodeSocket, request -> {
                if (request.operation() != OPERATION_REGISTER) {
                    if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED) && release.isDone()) {
                        settled.complete(null);
                    }
                    return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
                }
                registers.add(request);
                if (release.isDone()) {
                    signInsAfterRelease.add(request.bodyAsText());
                }
                if (!lost.get() || request.bodyAsText().contains(SECOND_PASSWORD)) {
                    return Response.success(OPERATION_REGISTER, registerBody(registers.size()));
                }
                // The sign-in on the next link hangs up, and the one that the redial replays waits for the change.
                int owed = owedSignIns.incrementAndGet();
                if (owed == 1) {
                    return Response.disconnect();
                }
                if (owed == 2) {
                    held.complete(null);
                    release.join();
                    if (accepted) {
                        return Response.success(OPERATION_REGISTER, registerBody(registers.size()));
                    }
                }
                return rejected ? Response.eviction(EVICTION_INVALID_CREDENTIALS) : Response.disconnect();
            });
            MockNode change = MockNode.serve(
                    changeSocket,
                    request -> request.operation() == OPERATION_REGISTER
                            ? Response.success(OPERATION_REGISTER, registerBody(1))
                            : Response.success(request.operation(), Unpooled.EMPTY_BUFFER));
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(nodeSocket.getLocalPort())
                    .requestTimeout(Duration.ofSeconds(1))
                    // No heartbeat in the test, so only the burst signs the client in again.
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .retryPolicy(RetryPolicy.fixedDelay(4, Duration.ofMillis(50)))
                    .build();
            // Holds the changed login, as the connection that saw the change commit does.
            AsyncTcpConnection changed = AsyncIggyTcpClientTransientFailoverTest.connection(changeSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                node.rejectBindings = true;
                lost.set(true);
                node.dropConnections();
                // A roster read never waits for a session, so it ends when the sign-in on the next link hangs up.
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class);
                held.get(RECOVERY.toMillis(), TimeUnit.MILLISECONDS);
                node.rejectBindings = false;

                changed.connect().get(5, TimeUnit.SECONDS);
                changed.send(LOGIN_CODE, Unpooled.wrappedBuffer(loginBody(USERNAME, SECOND_PASSWORD)))
                        .get(5, TimeUnit.SECONDS)
                        .release();
                client.refreshLogin(changed, USERNAME, Optional.empty(), Optional.of(SECOND_PASSWORD));
                release.complete(null);
                if (accepted) {
                    settled.get(RECOVERY.toMillis(), TimeUnit.MILLISECONDS);
                    node.rejectBindings = true;
                    node.dropConnections();
                    client.sendBinaryRequest(GET_STATS_CODE, new byte[0])
                            .handle((response, error) -> null)
                            .get(5, TimeUnit.SECONDS);
                }

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> !signInsAfterRelease.isEmpty(), "a sign-in after the change", RECOVERY);
                assertThat(signInsAfterRelease.get(0)).contains(SECOND_PASSWORD);
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(5, TimeUnit.SECONDS);
            } finally {
                release.complete(null);
                changed.close().get(5, TimeUnit.SECONDS);
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
                change.close();
            }
        }
    }

    /**
     * An explicit sign-in ends a waiting automatic sign-in on the connection that carries its Register. After
     * connect() replaced that connection, the new connection keeps its channels.
     */
    @Test
    void shouldKeepTheNewConnectionWhenAnAutomaticSignInWaitsOnTheReplacedOne() throws Exception {
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            MockNode node = MockNode.serve(
                    nodeSocket,
                    request -> request.operation() == OPERATION_REGISTER
                            ? Response.success(OPERATION_REGISTER, registerBody(1))
                            : Response.success(request.operation(), Unpooled.EMPTY_BUFFER));
            AtomicInteger attempts = new AtomicInteger();
            CompletableFuture<Void> replaying = new CompletableFuture<>();
            CompletableFuture<IdentityInfo> replay = new CompletableFuture<>();
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(InetAddress.getLoopbackAddress().getHostAddress())
                    .port(nodeSocket.getLocalPort())
                    .heartbeatInterval(HEARTBEAT)
                    .eventLoopGroup(group)
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                // The first attempt sends no Register, so the heartbeat finds a sign-in owed and replays the attempt.
                client.loginOnLeader(deadline -> {
                            if (attempts.incrementAndGet() == 1) {
                                return CompletableFuture.completedFuture(new IdentityInfo(1L, Optional.empty()));
                            }
                            replaying.complete(null);
                            return replay;
                        })
                        .get(5, TimeUnit.SECONDS);
                replaying.get(RECOVERY.toMillis(), TimeUnit.MILLISECONDS);
                client.connect().get(5, TimeUnit.SECONDS);
                client.system().ping().get(5, TimeUnit.SECONDS);
                int dials = node.dials.get();

                var signIn = client.users().login(USERNAME, FIRST_PASSWORD);
                // Every task queued on the only event loop ran, so an abort on the new connection took effect.
                group.submit(() -> {}).get(5, TimeUnit.SECONDS);
                // The close of the replaced connection ends the Register that the automatic sign-in waits for.
                replay.completeExceptionally(new IggyNotConnectedException("The connection was replaced"));

                assertThat(signIn.get(5, TimeUnit.SECONDS).userId()).isEqualTo(SIGNED_IN_USER_ID);
                assertThat(node.dials).as("dials after connect()").hasValue(dials);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Closing a connection that the client replaced fails the requests still in flight on it as lost links. The
     * client does not redial for them, because the redial would dial again and sign in again behind the caller. A
     * sent mutation among them still replays on the new connection.
     */
    @ParameterizedTest(name = "signedIn={0}")
    @ValueSource(booleans = {false, true})
    void shouldNotRedialForTheRequestsOfAReplacedConnection(boolean signedIn) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket firstSocket = new ServerSocket(0, 4, loopback);
                ServerSocket secondSocket = new ServerSocket(0, 4, loopback)) {
            CompletableFuture<Void> sent = new CompletableFuture<>();
            List<Long> held = new CopyOnWriteArrayList<>();
            List<Long> replayed = new CopyOnWriteArrayList<>();
            MockNode first = MockNode.serve(firstSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                // No reply, so the request is still in flight when the client replaces the connection.
                if (request.is(PING_CODE, OPERATION_NON_REPLICATED)
                        || request.operation() == VsrOperation.CREATE_STREAM) {
                    held.add(request.requestId());
                    sent.complete(null);
                    return Response.noReply();
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode second = MockNode.serve(secondSocket, request -> {
                if (request.operation() == VsrOperation.CREATE_STREAM) {
                    replayed.add(request.requestId());
                    return Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(firstSocket.getLocalPort())
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .retryPolicy(ONE_RETRY)
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                if (signedIn) {
                    client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                }
                var pending = client.sendBinaryRequest(
                        signedIn ? CommandCode.Stream.CREATE.getValue() : PING_CODE, new byte[0]);
                sent.get(5, TimeUnit.SECONDS);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), secondSocket.getLocalPort()))
                        .get(5, TimeUnit.SECONDS);

                if (signedIn) {
                    pending.get(5, TimeUnit.SECONDS);
                    assertThat(replayed).as("the replayed mutation").isEqualTo(held);
                } else {
                    assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOf(IggyConnectionException.class);
                }
                Thread.sleep(OUTAGE.toMillis());
                assertThat(second.dials).as("dials after the replacement").hasValue(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                first.close();
                second.close();
            }
        }
    }

    /**
     * A committed password change or rename of the signed-in user reaches every later sign-in, also when the
     * change was sent raw, or ended uncertain until a resolve proved it. Only the request names the new
     * credentials.
     */
    @ParameterizedTest(name = "raw={0}, rename={1}")
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    @SuppressWarnings("checkstyle:NPathComplexity")
    void shouldSignInWithTheCommittedChangeOfTheSignedInUser(boolean raw, boolean rename) throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean committing = new AtomicBoolean(raw);
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            List<String> signIns = new CopyOnWriteArrayList<>();
            MockNode node = credentialsNode(
                    nodeSocket,
                    hangUpOnRegister,
                    signIns,
                    request -> committing.get()
                            ? Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0))
                            : Response.error(request.operation(), AsyncTcpConnection.TRANSIENT_NOT_COMMITTED));
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                // A rename names the user by name and a password change by id, so both ways must match.
                UserId self = rename ? UserId.of(USERNAME) : UserId.of(SIGNED_IN_USER_ID);
                if (raw) {
                    client.sendBinaryRequest(
                                    rename ? UPDATE_USER_CODE : CHANGE_PASSWORD_CODE,
                                    rename
                                            ? renameBody(self, RENAMED)
                                            : changePasswordBody(self, FIRST_PASSWORD, SECOND_PASSWORD))
                            .get(5, TimeUnit.SECONDS);
                } else {
                    CompletableFuture<Void> change = rename
                            ? client.users().updateUser(self, Optional.of(RENAMED), Optional.empty())
                            : client.users().changePassword(self, FIRST_PASSWORD, SECOND_PASSWORD);
                    long operation = uncertainOperation(change);
                    committing.set(true);
                    client.resolveOperation(operation, Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS);
                }

                assertThat(signInsAfterTheSessionIsLost(client, node, hangUpOnRegister, signIns))
                        .allSatisfy(signIn -> assertThat(signIn)
                                .contains(rename ? RENAMED : USERNAME, rename ? FIRST_PASSWORD : SECOND_PASSWORD)
                                .doesNotContain(rename ? USERNAME : FIRST_PASSWORD));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * A change that the server committed as rejected, such as a wrong current password, and a change of another
     * user leave the sign-in as it was.
     */
    @ParameterizedTest(name = "rejected={0}")
    @ValueSource(booleans = {false, true})
    void shouldKeepTheSignInAfterARejectedChangeOrAChangeOfAnotherUser(boolean rejected) throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            List<String> signIns = new CopyOnWriteArrayList<>();
            MockNode node = credentialsNode(
                    nodeSocket,
                    hangUpOnRegister,
                    signIns,
                    request -> Response.committed(
                            request.operation(),
                            2,
                            rejected
                                    ? AsyncIggyTcpClientTransientFailoverTest.transientResult(
                                            IggyErrorCode.INVALID_CREDENTIALS.getCode())
                                    : Unpooled.buffer().writeIntLE(0)));
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                var change = client.sendBinaryRequest(
                        CHANGE_PASSWORD_CODE,
                        changePasswordBody(
                                UserId.of(rejected ? SIGNED_IN_USER_ID : OTHER_USER_ID),
                                FIRST_PASSWORD,
                                SECOND_PASSWORD));
                if (rejected) {
                    assertThatThrownBy(() -> change.get(5, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    error -> assertThat(error.getRawErrorCode())
                                            .isEqualTo(IggyErrorCode.INVALID_CREDENTIALS.getCode()));
                } else {
                    change.get(5, TimeUnit.SECONDS);
                }

                assertThat(signInsAfterTheSessionIsLost(client, node, hangUpOnRegister, signIns))
                        .allSatisfy(signIn -> assertThat(signIn)
                                .contains(USERNAME, FIRST_PASSWORD)
                                .doesNotContain(SECOND_PASSWORD));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * Every link of the session signs in with the committed password of the signed-in user. Here that is the poll
     * link on the survivor, which the coordinator moves to and back from before the change commits, and which
     * becomes the coordinator when the leader moves there. An uncertain change commits only when a resolve proves
     * it, after those links were bound.
     */
    @ParameterizedTest(name = "uncertain={0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("checkstyle:NPathComplexity")
    void shouldRegisterEveryLinkOfTheSessionWithTheCommittedPassword(boolean uncertain) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger leaderPort = new AtomicInteger(primaryPort);
            AtomicInteger registrations = new AtomicInteger();
            AtomicBoolean committing = new AtomicBoolean(!uncertain);
            AtomicBoolean endSession = new AtomicBoolean();
            List<String> survivorSignIns = new CopyOnWriteArrayList<>();
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, leaderPort.get()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == VsrOperation.CHANGE_PASSWORD) {
                    return committing.get()
                            ? Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0))
                            : Response.error(request.operation(), AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return endSession.compareAndSet(true, false)
                            ? Response.eviction(EVICTION_SESSION_ERROR)
                            : Response.success(
                                    OPERATION_NON_REPLICATED,
                                    clusterMetadata(primaryPort, survivorPort, leaderPort.get()));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorSignIns.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(1))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            ConnectionInfo primaryEndpoint = new ConnectionInfo(loopback.getHostAddress(), primaryPort);
            ConnectionInfo survivorEndpoint = new ConnectionInfo(loopback.getHostAddress(), survivorPort);
            UserId self = UserId.of(SIGNED_IN_USER_ID);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                long operation = uncertain
                        ? uncertainOperation(client.users().changePassword(self, FIRST_PASSWORD, SECOND_PASSWORD))
                        : 0;
                client.retarget(survivorEndpoint).get(5, TimeUnit.SECONDS);
                client.retarget(primaryEndpoint).get(5, TimeUnit.SECONDS);
                if (uncertain) {
                    committing.set(true);
                    client.resolveOperation(operation, Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS);
                } else {
                    client.users()
                            .changePassword(self, FIRST_PASSWORD, SECOND_PASSWORD)
                            .get(5, TimeUnit.SECONDS);
                }
                leaderPort.set(survivorPort);
                client.retarget(survivorEndpoint).get(5, TimeUnit.SECONDS);
                assertThat(survivorSignIns).as("the sign-ins on the survivor").isEmpty();

                // The server ends the session and refuses to bind it, so the next link registers the session again.
                survivor.rejectBindings = true;
                endSession.set(true);
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);
                client.system().getClusterMetadata().get(5, TimeUnit.SECONDS);

                assertThat(survivorSignIns)
                        .singleElement()
                        .asString()
                        .contains(USERNAME, SECOND_PASSWORD)
                        .doesNotContain(FIRST_PASSWORD);
                assertThat(client.getConnectionInfo().port()).isEqualTo(survivorPort);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                survivor.close();
            }
        }
    }

    /**
     * A password change that commits while a link waits for the reply to its BindSession reaches the Register
     * that follows when the server refuses that bind. The link that waits is on the survivor, and the change
     * commits on the primary, where the coordinator moved meanwhile.
     */
    @Test
    void shouldRegisterWithThePasswordThatCommittedWhileTheBindWaited() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger registrations = new AtomicInteger();
            AtomicBoolean endSession = new AtomicBoolean();
            List<String> survivorSignIns = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> releaseBinding = new CompletableFuture<>();
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == VsrOperation.CHANGE_PASSWORD) {
                    return Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return endSession.compareAndSet(true, false)
                            ? Response.eviction(EVICTION_SESSION_ERROR)
                            : Response.success(
                                    OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorSignIns.add(request.bodyAsText());
                    return request.bodyAsText().contains(SECOND_PASSWORD)
                            ? Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()))
                            : Response.eviction(EVICTION_INVALID_CREDENTIALS);
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(5))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), survivorPort))
                        .get(5, TimeUnit.SECONDS);

                // The survivor ends the session, so the next request on its link binds the session again and
                // waits for the reply.
                survivor.rejectBindings = true;
                survivor.bindingHold = releaseBinding;
                endSession.set(true);
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);
                int bindings = survivor.bindings.get();
                var waiting = client.system().getClusterMetadata();
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> survivor.bindings.get() > bindings, "the held bind", RECOVERY);

                client.retarget(new ConnectionInfo(loopback.getHostAddress(), primaryPort))
                        .get(5, TimeUnit.SECONDS);
                client.users()
                        .changePassword(UserId.of(SIGNED_IN_USER_ID), FIRST_PASSWORD, SECOND_PASSWORD)
                        .get(5, TimeUnit.SECONDS);
                releaseBinding.complete(null);

                waiting.get(5, TimeUnit.SECONDS);
                assertThat(survivorSignIns)
                        .singleElement()
                        .asString()
                        .contains(USERNAME, SECOND_PASSWORD)
                        .doesNotContain(FIRST_PASSWORD);
            } finally {
                releaseBinding.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                survivor.close();
            }
        }
    }

    @ParameterizedTest(name = "evicted={0}")
    @ValueSource(booleans = {false, true})
    void shouldRetryTheRefreshedPasswordWhenAnOlderAutomaticRegisterIsRejected(boolean evicted) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            CompletableFuture<Void> changing = new CompletableFuture<>();
            CompletableFuture<Void> releaseChange = new CompletableFuture<>();
            CompletableFuture<Void> registering = new CompletableFuture<>();
            CompletableFuture<Void> releaseRegister = new CompletableFuture<>();
            AtomicBoolean endSession = new AtomicBoolean();
            List<Request> registers = new CopyOnWriteArrayList<>();
            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.operation() == VsrOperation.CHANGE_PASSWORD) {
                    changing.complete(null);
                    releaseChange.join();
                    return Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return endSession.getAndSet(false)
                            ? Response.eviction(EVICTION_SESSION_ERROR)
                            : Response.success(
                                    OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    registers.add(request);
                    if (request.bodyAsText().contains(FIRST_PASSWORD)) {
                        registering.complete(null);
                        releaseRegister.join();
                        return evicted
                                ? Response.eviction(EVICTION_INVALID_CREDENTIALS)
                                : Response.error(OPERATION_REGISTER, IggyErrorCode.INVALID_CREDENTIALS.getCode());
                    }
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(5))
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                var change =
                        client.users().changePassword(UserId.of(SIGNED_IN_USER_ID), FIRST_PASSWORD, SECOND_PASSWORD);
                changing.get(5, TimeUnit.SECONDS);
                client.retarget(new ConnectionInfo(loopback.getHostAddress(), survivorPort))
                        .get(5, TimeUnit.SECONDS);
                survivor.rejectBindings = true;
                endSession.set(true);
                assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);
                var waiting = client.system().getClusterMetadata();
                registering.get(5, TimeUnit.SECONDS);
                releaseChange.complete(null);
                change.get(5, TimeUnit.SECONDS);
                releaseRegister.complete(null);

                waiting.get(5, TimeUnit.SECONDS);
                assertThat(client.hasRememberedLogin()).isTrue();
                assertThat(registers).hasSize(2);
                assertThat(registers.get(1).bodyAsText())
                        .contains(USERNAME, SECOND_PASSWORD)
                        .doesNotContain(FIRST_PASSWORD);
                assertThat(List.of(
                                registers.get(1).clientLow(), registers.get(1).clientHigh()))
                        .isEqualTo(List.of(
                                registers.get(0).clientLow(), registers.get(0).clientHigh()));
            } finally {
                releaseChange.complete(null);
                releaseRegister.complete(null);
                client.close().get(5, TimeUnit.SECONDS);
                primary.close();
                survivor.close();
            }
        }
    }

    /**
     * Records every Register and hangs up on the first one after {@code hangUpOnRegister} is set. A password
     * change or a rename gets the reply of {@code change}.
     */
    private static MockNode credentialsNode(
            ServerSocket socket, AtomicBoolean hangUpOnRegister, List<String> signIns, RequestHandler change) {
        return MockNode.serve(socket, request -> {
            if (request.operation() == OPERATION_REGISTER) {
                signIns.add(request.bodyAsText());
                return hangUpOnRegister.getAndSet(false)
                        ? Response.disconnect()
                        : Response.success(OPERATION_REGISTER, registerBody(signIns.size()));
            }
            if (request.operation() == VsrOperation.CHANGE_PASSWORD
                    || request.operation() == VsrOperation.UPDATE_USER) {
                return change.handle(request);
            }
            return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
        });
    }

    /**
     * Ends the session and returns the sign-ins that restore it: the one on the next link of the connection,
     * which hangs up, and the one that the redial replays. A roster read never waits for a session, so it ends
     * when that sign-in hangs up.
     */
    private static List<String> signInsAfterTheSessionIsLost(
            AsyncIggyTcpClient client, MockNode node, AtomicBoolean hangUpOnRegister, List<String> signIns)
            throws Exception {
        int before = signIns.size();
        node.rejectBindings = true;
        hangUpOnRegister.set(true);
        node.dropConnections();
        assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        AsyncIggyTcpClientEventLoopGroupTest.await(() -> signIns.size() >= before + 2, "the redial sign-in", RECOVERY);
        List<String> all = List.copyOf(signIns);
        return all.subList(before, all.size());
    }

    private static long uncertainOperation(CompletableFuture<?> call) {
        AtomicLong operation = new AtomicLong();
        assertThatThrownBy(() -> call.get(5, TimeUnit.SECONDS))
                .cause()
                .isInstanceOfSatisfying(IggyOperationException.class, uncertain -> {
                    assertThat(uncertain.getErrorCode()).isEqualTo(IggyErrorCode.OPERATION_UNCERTAIN);
                    operation.set(uncertain.getOperationId());
                });
        return operation.get();
    }

    /** The body that {@code UsersClient.login} writes. */
    private static byte[] loginBody(String username, String password) {
        return ByteBufUtil.getBytes(BytesSerializer.toBytes(username, "username")
                .writeBytes(BytesSerializer.toBytes(password, "password")));
    }

    /** The body that {@code UsersClient.changePassword} writes. */
    private static byte[] changePasswordBody(UserId user, String currentPassword, String newPassword) {
        return ByteBufUtil.getBytes(BytesSerializer.toBytes(user)
                .writeBytes(BytesSerializer.toBytes(currentPassword, "current password"))
                .writeBytes(BytesSerializer.toBytes(newPassword, "new password")));
    }

    /** The body that {@code UsersClient.updateUser} writes for a rename: a username and no status. */
    private static byte[] renameBody(UserId user, String username) {
        return ByteBufUtil.getBytes(BytesSerializer.toBytes(user)
                .writeByte(1)
                .writeBytes(BytesSerializer.toBytes(username, "username"))
                .writeByte(0));
    }

    private static AsyncIggyTcpClient limitedClient(int port) {
        return AsyncIggyTcpClient.builder()
                .host(InetAddress.getLoopbackAddress().getHostAddress())
                .port(port)
                .requestTimeout(Duration.ofSeconds(1))
                .heartbeatInterval(HEARTBEAT)
                .retryPolicy(ONE_RETRY)
                .build();
    }

    /** Retries until one request completes, or the budget runs out. */
    private static boolean resumeWithin(AsyncIggyTcpClient client, Duration budget) throws InterruptedException {
        long deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                client.sendBinaryRequest(PING_CODE, new byte[0]).get(2, TimeUnit.SECONDS);
                return true;
            } catch (ExecutionException | TimeoutException stillDown) {
                Thread.sleep(50);
            }
        }
        return false;
    }

    private static ByteBuf registerBody(long session) {
        ByteBuf body = Unpooled.buffer();
        body.writeIntLE(0);
        body.writeIntLE(1);
        body.writeLongLE(session);
        body.writeIntLE((11 << 10) | 1);
        body.writeByte(0);
        return body;
    }

    private static ByteBuf clusterMetadata(int primaryPort, int survivorPort, int leaderPort) {
        ByteBuf body = Unpooled.buffer();
        writeString(body, "test-cluster");
        body.writeIntLE(2);
        writeNode(body, "primary", primaryPort, primaryPort == leaderPort);
        writeNode(body, "survivor", survivorPort, survivorPort == leaderPort);
        return body;
    }

    private static void writeNode(ByteBuf body, String name, int port, boolean leader) {
        body.writeByte("primary".equals(name) ? 0 : 1);
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

    /**
     * A loopback VSR node that keeps serving every connection it accepts until
     * it is killed, which drops the live sockets and stops accepting so a
     * redial is refused the way a dead process refuses one.
     *
     * <p>Dedicated daemon threads, not {@code CompletableFuture.runAsync}: the
     * accept loop and every connection handler block indefinitely, and parking
     * them on the common pool starves it on a low-core CI runner (parallelism
     * is cores minus one), which stalls the client's own async continuations
     * and times the login out before the test does anything.
     */
    static final class MockNode {
        volatile boolean rejectBindings;
        // A BindSession waits for this before its reply, which follows rejectBindings as it is then.
        volatile CompletableFuture<Void> bindingHold = CompletableFuture.completedFuture(null);
        final AtomicInteger bindings = new AtomicInteger();
        private final ServerSocket server;
        private final List<Socket> accepted = new CopyOnWriteArrayList<>();
        private volatile boolean killed;
        private volatile boolean down;
        private volatile boolean silent;
        private final AtomicInteger dials = new AtomicInteger();

        private MockNode(ServerSocket server) {
            this.server = server;
        }

        static MockNode serve(ServerSocket server, RequestHandler handler) {
            MockNode node = new MockNode(server);
            Thread acceptor = new Thread(
                    () -> {
                        while (!node.killed) {
                            try {
                                Socket socket = server.accept();
                                node.dials.incrementAndGet();
                                if (node.down) {
                                    socket.close();
                                    continue;
                                }
                                node.accepted.add(socket);
                                Thread exchange = new Thread(() -> node.exchange(socket, handler));
                                exchange.setDaemon(true);
                                exchange.start();
                            } catch (IOException accepted) {
                                return;
                            }
                        }
                    },
                    "mock-vsr-acceptor-" + server.getLocalPort());
            acceptor.setDaemon(true);
            acceptor.start();
            return node;
        }

        private void exchange(Socket socket, RequestHandler handler) {
            try (socket) {
                InputStream input = socket.getInputStream();
                OutputStream output = socket.getOutputStream();
                Request request;
                while (!killed && (request = AsyncIggyTcpClientTransientFailoverTest.readRequest(input, 0)) != null) {
                    if (silent) {
                        continue;
                    }
                    Response response;
                    if (request.is(
                            AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                        bindings.incrementAndGet();
                        bindingHold.join();
                        response = rejectBindings
                                ? Response.error(OPERATION_NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode())
                                : Response.success(
                                        OPERATION_NON_REPLICATED,
                                        AsyncIggyTcpClientTransientFailoverTest.bindBody(request));
                    } else {
                        response = handler.handle(request);
                    }
                    if (response.command() == Response.disconnect().command()) {
                        return;
                    }
                    AsyncIggyTcpClientTransientFailoverTest.writeResponse(output, request, response);
                }
            } catch (IOException closed) {
                // A killed node and a client that went away look the same here.
            }
        }

        /**
         * Hangs up every connection until {@link #up()}, the way a port map
         * or a port-forward does while nothing listens behind it.
         */
        void down() throws IOException {
            down = true;
            dropConnections();
        }

        void up() {
            down = false;
        }

        void dropConnections() throws IOException {
            for (Socket socket : accepted) {
                socket.close();
            }
        }

        void kill() throws IOException {
            killed = true;
            dropConnections();
            server.close();
        }

        void close() throws IOException {
            kill();
        }
    }

    @FunctionalInterface
    interface RequestHandler {
        Response handle(Request request);
    }

    /**
     * A retry budget of zero still gets one rotation when several endpoints are
     * known: those endpoints -- the address the client was configured with, the
     * nodes the roster named -- were made known in order to be tried, and every
     * other SDK sweeps them once too. With one endpoint known, zero retries
     * redials nothing, which is what it asked for.
     */
    @Test
    void shouldSweepTheKnownEndpointsOnceWithNoRetries() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket primarySocket = new ServerSocket(0, 4, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, 4, loopback)) {
            int primaryPort = primarySocket.getLocalPort();
            int survivorPort = survivorSocket.getLocalPort();
            AtomicInteger survivorRegistrations = new AtomicInteger();

            MockNode primary = MockNode.serve(primarySocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, primaryPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            });
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED, clusterMetadata(primaryPort, survivorPort, survivorPort));
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorRegistrations.incrementAndGet();
                    return Response.success(OPERATION_REGISTER, registerBody(2));
                }
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            });

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .credentials("iggy", "iggy")
                    .requestTimeout(Duration.ofSeconds(2))
                    .retryPolicy(RetryPolicy.noRetry())
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(PING_CODE, new byte[0]).get(5, TimeUnit.SECONDS);

                primary.kill();

                assertThat(resumeWithin(client, Duration.ofSeconds(4)))
                        .as("zero retries skipped the one rotation the known endpoints are for")
                        .isTrue();
                assertThat(client.getConnectionInfo().port()).isEqualTo(survivorPort);
                assertThat(survivorRegistrations).hasValue(0);
                assertThat(survivor.bindings).hasValueGreaterThanOrEqualTo(1);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                survivor.close();
            }
        }
    }

    /**
     * Every dial is capped once more than one endpoint is known, a configured
     * connection timeout included: it says how long one endpoint may take, and a
     * rotation that spends it on each of them reaches the survivor long after
     * the caller gave up. A timeout shorter than the cap is what the caller
     * asked for and stands; with one endpoint known nothing is queued behind
     * the dial, so the configured value stands there too.
     */
    @Test
    void shouldCapEveryDialWhenMoreThanOneEndpointIsKnown() {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        List<ConnectionInfo> roster = List.of(new ConnectionInfo("iggy-1", 8091));

        AsyncIggyTcpClient patient = AsyncIggyTcpClient.builder()
                .host(loopback.getHostAddress())
                .port(8090)
                .connectionTimeout(Duration.ofSeconds(30))
                .build();
        assertThat(patient.dialTimeout()).contains(Duration.ofSeconds(30));
        patient.rememberRoster(new LeaderAwareness.LeaderLookup(Optional.empty(), roster));
        assertThat(patient.dialTimeout())
                .as("a configured timeout exempted the dial from the failover cap")
                .contains(AsyncIggyTcpClient.FAILOVER_DIAL_TIMEOUT);

        AsyncIggyTcpClient impatient = AsyncIggyTcpClient.builder()
                .host(loopback.getHostAddress())
                .port(8090)
                .connectionTimeout(Duration.ofMillis(500))
                .build();
        impatient.rememberRoster(new LeaderAwareness.LeaderLookup(Optional.empty(), roster));
        assertThat(impatient.dialTimeout())
                .as("a timeout shorter than the cap is what the caller asked for")
                .contains(Duration.ofMillis(500));

        AsyncIggyTcpClient unconfigured = AsyncIggyTcpClient.builder()
                .host(loopback.getHostAddress())
                .port(8090)
                .build();
        assertThat(unconfigured.dialTimeout()).isEmpty();
        unconfigured.rememberRoster(new LeaderAwareness.LeaderLookup(Optional.empty(), roster));
        assertThat(unconfigured.dialTimeout()).contains(AsyncIggyTcpClient.FAILOVER_DIAL_TIMEOUT);
    }

    /**
     * A roster read that learned nothing must leave the last one standing:
     * emptying the redial candidates when the cluster is unreachable takes them
     * away exactly when they are needed.
     */
    @Test
    void shouldKeepTheLastRosterWhenALookupLearnedNothing() {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                .host(loopback.getHostAddress())
                .port(8090)
                .build();
        List<ConnectionInfo> roster = List.of(new ConnectionInfo("iggy-0", 8091), new ConnectionInfo("iggy-1", 8092));

        client.rememberRoster(new LeaderAwareness.LeaderLookup(Optional.empty(), roster));
        assertThat(client.rosterTargets()).isEqualTo(roster);

        client.rememberRoster(LeaderAwareness.LeaderLookup.inconclusive());
        assertThat(client.rosterTargets())
                .as("an inconclusive check erased the endpoints the client still needs")
                .isEqualTo(roster);
    }

    /**
     * Only the server refusing the credentials, with a code of the Rust SDK,
     * ends a redial. A channel that closed before the reply, or a node that
     * refuses because it is not the primary, says nothing about the
     * credentials -- treated as a rejection, they are dropped and the client
     * is published on a node that is already gone.
     */
    @Test
    void shouldTreatOnlyACredentialRefusalAsASignInRejection() {
        for (IggyErrorCode refusal : List.of(
                IggyErrorCode.INVALID_CREDENTIALS,
                IggyErrorCode.INVALID_FORMAT,
                IggyErrorCode.INVALID_PERSONAL_ACCESS_TOKEN,
                IggyErrorCode.PERSONAL_ACCESS_TOKEN_EXPIRED)) {
            assertThat(AsyncIggyTcpClient.isSignInRejection(
                            IggyServerException.fromTcpResponse(refusal.getCode(), new byte[0])))
                    .as(refusal.name())
                    .isTrue();
        }
        assertThat(AsyncIggyTcpClient.isSignInRejection(
                        IggyServerException.fromTcpResponse(AsyncTcpConnection.NOT_PRIMARY, new byte[0])))
                .as("a node that is not the primary says nothing about the credentials")
                .isFalse();
        assertThat(AsyncIggyTcpClient.isSignInRejection(new IggyConnectionException("channel closed")))
                .as("a channel that died mid sign-in is not a rejected credential")
                .isFalse();
        assertThat(AsyncIggyTcpClient.isSignInRejection(new IOException("connection reset")))
                .isFalse();
        assertThat(AsyncIggyTcpClient.isSignInRejection(
                        IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED, new byte[0])))
                .as("a node that is not the primary refuses transiently; another endpoint answers")
                .isFalse();
        assertThat(AsyncIggyTcpClient.isSignInRejection(
                        IggyServerException.fromTcpResponse(AsyncTcpConnection.TRANSIENT_NOT_COMMITTED, new byte[0])))
                .isFalse();
    }
}
