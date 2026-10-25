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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.MockNode;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.threeNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A request that a node refused before admission fails over to another node of the roster, and the session signs
 * in there again before the request is sent.
 *
 * <p>The router keeps a link to every node of the roster and binds the session on each, so a node whose bind waits
 * holds the walk too. The source answers the request after the connection stopped retrying the refusal on the
 * same node, so the client fails the request over as it reads that reply.
 */
class AsyncIggyTcpClientFailoverSignInTest {
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final int BACKLOG = 4;
    private static final int PING_CODE = CommandCode.System.PING.getValue();
    private static final int GET_STATS_CODE = CommandCode.System.GET_STATS.getValue();
    private static final int EVICTION_INVALID_CREDENTIALS = 9;
    private static final int EVICTION_SESSION_ERROR = 12;
    private static final String SIGNED_IN_USER = "manual-user";
    private static final String FIRST_PASSWORD = "first-password";
    private static final String SECOND_PASSWORD = "second-password";
    private static final String REFUSED_TOKEN = "refused-token";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REFUSAL_MARGIN = Duration.ofMillis(250);
    // Long enough that a sign-in which waits behind the failover times out first.
    private static final Duration STALLED_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration QUIET_PERIOD = Duration.ofMillis(500);

    /**
     * A committed password change reaches the Register after a roster walk, a retry of an older refused
     * Register, and the next recovery after an older accepted Register.
     */
    @ParameterizedTest(name = "timing={0}")
    @EnumSource(PasswordChangeTiming.class)
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    void shouldUseThePasswordCommittedDuringTheWalkOrItsRegister(PasswordChangeTiming timing) throws Exception {
        boolean afterRegister = timing != PasswordChangeTiming.DURING_WALK;
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket sourceSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket refusingSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, BACKLOG, loopback)) {
            Supplier<ByteBuf> roster = () -> threeNodeMetadata(
                    sourceSocket.getLocalPort(),
                    refusingSocket.getLocalPort(),
                    survivorSocket.getLocalPort(),
                    sourceSocket.getLocalPort());
            CompletableFuture<Void> refusalStarted = new CompletableFuture<>();
            CompletableFuture<Void> releaseBind = new CompletableFuture<>();
            CompletableFuture<Void> registering = new CompletableFuture<>();
            CompletableFuture<Void> releaseRegister = new CompletableFuture<>();
            CompletableFuture<Void> releaseChange = new CompletableFuture<>();
            AtomicBoolean endSession = new AtomicBoolean();
            List<String> survivorSignIns = new CopyOnWriteArrayList<>();
            MockNode source = MockNode.serve(sourceSocket, request -> {
                if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED)) {
                    refusalStarted.complete(null);
                    return refusedAfterTheRetryBudget();
                }
                if (request.operation() == VsrOperation.CHANGE_PASSWORD) {
                    if (afterRegister) {
                        releaseChange.join();
                    }
                    return Response.committed(
                            VsrOperation.CHANGE_PASSWORD, 2, Unpooled.buffer().writeIntLE(0));
                }
                return answer(request, roster);
            });
            MockNode refusing = MockNode.serve(refusingSocket, request -> answer(request, roster));
            refusing.bindingHold = releaseBind;
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED) && endSession.getAndSet(false)) {
                    return Response.eviction(EVICTION_SESSION_ERROR);
                }
                if (request.operation() == OPERATION_REGISTER) {
                    survivorSignIns.add(request.bodyAsText());
                    if (afterRegister && request.bodyAsText().contains(FIRST_PASSWORD)) {
                        registering.complete(null);
                        releaseRegister.join();
                    }
                    // The session that this Register starts binds on the refusing node too.
                    refusing.rejectBindings = false;
                    return request.bodyAsText().contains(SECOND_PASSWORD)
                                    || (timing == PasswordChangeTiming.REGISTER_SUCCEEDS && survivorSignIns.size() == 1)
                            ? Response.success(OPERATION_REGISTER, registerBody(2))
                            : Response.eviction(EVICTION_INVALID_CREDENTIALS);
                }
                return answer(request, roster);
            });
            WalkingClient client = new WalkingClient(sourceSocket);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login(SIGNED_IN_USER, FIRST_PASSWORD).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> refusing.bindings.get() > 0, "the bind on the refusing node", REQUEST_TIMEOUT);

                CompletableFuture<byte[]> failedOver = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                refusalStarted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                // The source answers the change after the refusal, so it commits after the request failed over.
                var changed = client.users().changePassword(UserId.of(SIGNED_IN_USER), FIRST_PASSWORD, SECOND_PASSWORD);
                if (!afterRegister) {
                    changed.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                }
                client.walking.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                refusing.rejectBindings = true;
                releaseBind.complete(null);
                if (afterRegister) {
                    registering.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                    releaseChange.complete(null);
                    changed.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                    releaseRegister.complete(null);
                }

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> !survivorSignIns.isEmpty(), "the sign-in on the survivor", REQUEST_TIMEOUT);
                assertThat(failedOver.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                        .isEmpty();
                if (timing == PasswordChangeTiming.REGISTER_SUCCEEDS) {
                    survivor.rejectBindings = true;
                    endSession.set(true);
                    assertThatThrownBy(() ->
                                    client.system().getClusterMetadata().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(IggyServerException.class);
                    client.system().getClusterMetadata().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                assertThat(survivorSignIns).hasSize(afterRegister ? 2 : 1);
                assertThat(survivorSignIns.get(survivorSignIns.size() - 1))
                        .contains(SECOND_PASSWORD)
                        .doesNotContain(FIRST_PASSWORD);
                assertThat(client.getConnectionInfo().port()).isEqualTo(survivorSocket.getLocalPort());
            } finally {
                releaseBind.complete(null);
                releaseChange.complete(null);
                releaseRegister.complete(null);
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                source.close();
                refusing.close();
                survivor.close();
            }
        }
    }

    /**
     * A sign-in refusal during the first failover ends both waiting calls with 40 and the refusal as the cause.
     * The queued failover must not sign in again, and subsequent calls need an explicit sign-in.
     */
    @ParameterizedTest(name = "refusal={0}")
    @EnumSource(
            value = IggyErrorCode.class,
            names = {"INVALID_PERSONAL_ACCESS_TOKEN", "PERSONAL_ACCESS_TOKEN_EXPIRED"})
    void shouldEndQueuedFailoversWhenAutomaticSignInIsRefused(IggyErrorCode refusal) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket sourceSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket refusingSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket survivorSocket = new ServerSocket(0, BACKLOG, loopback)) {
            Supplier<ByteBuf> roster = () -> threeNodeMetadata(
                    sourceSocket.getLocalPort(),
                    refusingSocket.getLocalPort(),
                    survivorSocket.getLocalPort(),
                    sourceSocket.getLocalPort());
            AtomicInteger refusals = new AtomicInteger();
            CompletableFuture<Void> bothRefused = new CompletableFuture<>();
            CompletableFuture<Void> releaseBind = new CompletableFuture<>();
            AtomicInteger survivorSignIns = new AtomicInteger();
            MockNode source = MockNode.serve(sourceSocket, request -> {
                if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED)) {
                    Response refused = refusedAfterTheRetryBudget();
                    if (refusals.incrementAndGet() == 2) {
                        bothRefused.complete(null);
                    }
                    return refused;
                }
                return answer(request, roster);
            });
            MockNode refusing = MockNode.serve(refusingSocket, request -> answer(request, roster));
            refusing.bindingHold = releaseBind;
            MockNode survivor = MockNode.serve(survivorSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    survivorSignIns.incrementAndGet();
                    return Response.error(OPERATION_REGISTER, refusal.getCode());
                }
                return answer(request, roster);
            });
            AsyncIggyTcpClient client = client(sourceSocket, REQUEST_TIMEOUT);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.personalAccessTokens()
                        .loginWithPersonalAccessToken(REFUSED_TOKEN)
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> refusing.bindings.get() > 0, "the bind on the refusing node", REQUEST_TIMEOUT);

                CompletableFuture<byte[]> first = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                CompletableFuture<byte[]> second = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                bothRefused.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                // The failover of the second request queues behind the first one, whose walk waits for the bind.
                Thread.sleep(QUIET_PERIOD.toMillis());
                refusing.rejectBindings = true;
                releaseBind.complete(null);

                AsyncIggyTcpClientSessionRulesTest.assertEndedByTheRefusal(first, refusal);
                AsyncIggyTcpClientSessionRulesTest.assertEndedByTheRefusal(second, refusal);
                AsyncIggyTcpClientSessionRulesTest.assertSignedOut(client);
                assertThat(survivorSignIns)
                        .as("the sign-ins with the refused token")
                        .hasValue(1);
            } finally {
                releaseBind.complete(null);
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                source.close();
                refusing.close();
                survivor.close();
            }
        }
    }

    /**
     * An explicit sign-in does not wait behind a failover whose walk waits for a bind until the request deadline.
     * It ends that failover, whose request ends with 40 because the caller signed in after it started, and the walk
     * moves the client nowhere.
     */
    @Test
    void shouldEndAFailoverThatWaitsForABindWhenTheCallerSignsIn() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket sourceSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket stalledSocket = new ServerSocket(0, BACKLOG, loopback);
                ServerSocket otherSocket = new ServerSocket(0, BACKLOG, loopback)) {
            Supplier<ByteBuf> roster = () -> threeNodeMetadata(
                    sourceSocket.getLocalPort(),
                    stalledSocket.getLocalPort(),
                    otherSocket.getLocalPort(),
                    sourceSocket.getLocalPort());
            AtomicBoolean refuse = new AtomicBoolean(true);
            CompletableFuture<Void> refusalStarted = new CompletableFuture<>();
            CompletableFuture<Void> releaseBind = new CompletableFuture<>();
            MockNode source = MockNode.serve(sourceSocket, request -> {
                if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED) && refuse.getAndSet(false)) {
                    refusalStarted.complete(null);
                    return refusedAfterTheRetryBudget();
                }
                return answer(request, roster);
            });
            MockNode stalled = MockNode.serve(stalledSocket, request -> answer(request, roster));
            stalled.bindingHold = releaseBind;
            MockNode other = MockNode.serve(otherSocket, request -> answer(request, roster));
            AsyncIggyTcpClient client = client(sourceSocket, STALLED_REQUEST_TIMEOUT);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.users().login(SIGNED_IN_USER, FIRST_PASSWORD).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> stalled.bindings.get() > 0, "the bind on the stalled node", REQUEST_TIMEOUT);

                CompletableFuture<byte[]> failedOver = client.sendBinaryRequest(GET_STATS_CODE, new byte[0]);
                refusalStarted.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                // The source answers the ping after the refusal, so the failover holds the sign-in gate by then.
                client.sendBinaryRequest(PING_CODE, new byte[0]).get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                // The sign-in settles on the leader, which undoes a move that a walk makes during it. A held bind
                // delays such a move until after the sign-in.
                other.bindingHold = releaseBind;

                client.users().login(SIGNED_IN_USER, SECOND_PASSWORD).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThatThrownBy(() -> failedOver.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .cause()
                        .isInstanceOfSatisfying(IggyServerException.class, error -> {
                            assertThat(error.getRawErrorCode()).isEqualTo(IggyErrorCode.UNAUTHENTICATED.getCode());
                            assertThat(error).hasRootCauseInstanceOf(IggyNotConnectedException.class);
                        });
                releaseBind.complete(null);
                // A walk that went on would reach a node within this period.
                Thread.sleep(QUIET_PERIOD.toMillis());
                assertThat(client.getConnectionInfo().port()).isEqualTo(sourceSocket.getLocalPort());
                client.sendBinaryRequest(GET_STATS_CODE, new byte[0]).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                releaseBind.complete(null);
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                source.close();
                stalled.close();
                other.close();
            }
        }
    }

    private enum PasswordChangeTiming {
        DURING_WALK,
        REGISTER_REFUSED,
        REGISTER_SUCCEEDS
    }

    private static final class WalkingClient extends AsyncIggyTcpClient {
        private final CompletableFuture<Void> walking = new CompletableFuture<>();

        private WalkingClient(ServerSocket source) {
            super(
                    source.getInetAddress().getHostAddress(),
                    source.getLocalPort(),
                    null,
                    null,
                    null,
                    null,
                    REQUEST_TIMEOUT,
                    Duration.ofMinutes(1),
                    VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                    null,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    AsyncTcpConnection.DEFAULT_IO_THREADS);
        }

        @Override
        CompletableFuture<Void> retarget(ConnectionInfo target, long deadline, BooleanSupplier wanted) {
            var result = super.retarget(target, deadline, wanted);
            walking.complete(null);
            return result;
        }
    }

    private static AsyncIggyTcpClient client(ServerSocket source, Duration requestTimeout) {
        return AsyncIggyTcpClient.builder()
                .host(source.getInetAddress().getHostAddress())
                .port(source.getLocalPort())
                .requestTimeout(requestTimeout)
                .heartbeatInterval(Duration.ofMinutes(1))
                .build();
    }

    private static Response answer(Request request, Supplier<ByteBuf> roster) {
        if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
            return Response.success(OPERATION_NON_REPLICATED, roster.get());
        }
        if (request.operation() == OPERATION_REGISTER) {
            return Response.success(OPERATION_REGISTER, registerBody(1));
        }
        return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
    }

    /** Refuses the request once the connection no longer retries a refusal on the same node. */
    private static Response refusedAfterTheRetryBudget() {
        try {
            Thread.sleep(AsyncTcpConnection.NOT_ACCEPTED_RETRY_BUDGET
                    .plus(REFUSAL_MARGIN)
                    .toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while holding the refusal", interrupted);
        }
        return Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
    }
}
