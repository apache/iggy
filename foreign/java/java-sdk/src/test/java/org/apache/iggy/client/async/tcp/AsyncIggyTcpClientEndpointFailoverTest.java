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
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The node a client signed in on dies; its next request has to complete on a
 * survivor the roster named, under a session established there. Mirrors
 * {@code core/integration/tests/cluster/failover_client_continuity.rs}. The
 * mock VSR framing matches {@link AsyncIggyTcpClientTransientFailoverTest},
 * kept separate so a death mid-connection cannot disturb that suite's server.
 */
class AsyncIggyTcpClientEndpointFailoverTest {
    private static final int HEADER_SIZE = 256;
    private static final int SIZE_OFFSET = 48;
    private static final int COMMAND_OFFSET = 60;
    private static final int REQUEST_ID_OFFSET = 168;
    private static final int REQUEST_OPERATION_OFFSET = 176;
    private static final int REQUEST_CODE_OFFSET = 196;
    private static final int REPLY_REQUEST_ID_OFFSET = 200;
    private static final int REPLY_OPERATION_OFFSET = 208;
    private static final int REPLY_STATUS_OFFSET = 216;

    private static final int COMMAND_REPLY = 8;
    private static final int OPERATION_REGISTER = 1;
    private static final int OPERATION_NON_REPLICATED = 2;
    private static final int GET_CLUSTER_METADATA_CODE = 12;
    private static final int PING_CODE = 1;

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
                assertThat(survivorRegistrations)
                        .as("the login was replayed on the survivor")
                        .hasValueGreaterThanOrEqualTo(1);
                assertThat(survivorPings)
                        .as("the request landed on the survivor")
                        .hasValueGreaterThanOrEqualTo(1);
                assertThat(survivorLogins.get(survivorLogins.size() - 1))
                        .as("the redial replayed the configured user instead of the last sign-in")
                        .contains("handrun")
                        .doesNotContain("configured");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                survivor.close();
            }
        }
    }

    /**
     * An explicit sign-out is caller intent, like a close: the redial after it
     * must not sign back in with the credentials that sign-in used.
     */
    @Test
    void shouldNotResurrectASignedOutSessionOnASurvivor() throws Exception {
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

            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(primaryPort)
                    .requestTimeout(Duration.ofSeconds(2))
                    .retryPolicy(RetryPolicy.fixedDelay(4, Duration.ofMillis(50)))
                    .build();
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login("iggy", "iggy").get(5, TimeUnit.SECONDS);
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
        body.writeIntLE(11 << 10);
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
    private static final class MockNode {
        private final ServerSocket server;
        private final List<Socket> accepted = new CopyOnWriteArrayList<>();
        private volatile boolean killed;

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
                while (!killed && (request = readRequest(input)) != null) {
                    writeResponse(output, request, handler.handle(request));
                }
            } catch (IOException closed) {
                // A killed node and a client that went away look the same here.
            }
        }

        void kill() throws IOException {
            killed = true;
            for (Socket socket : accepted) {
                socket.close();
            }
            server.close();
        }

        void close() throws IOException {
            kill();
        }
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
                body);
    }

    private static void writeResponse(OutputStream output, Request request, Response response) throws IOException {
        byte[] body = new byte[response.body().readableBytes()];
        response.body().readBytes(body);
        response.body().release();
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        fields.putInt(SIZE_OFFSET, HEADER_SIZE + body.length);
        header[COMMAND_OFFSET] = (byte) COMMAND_REPLY;
        fields.putLong(REPLY_REQUEST_ID_OFFSET, request.requestId());
        header[REPLY_OPERATION_OFFSET] = (byte) response.operation();
        fields.putInt(REPLY_STATUS_OFFSET, 0);
        output.write(header);
        output.write(body);
        output.flush();
    }

    private record Request(int operation, int commandCode, long requestId, byte[] body) {
        boolean is(int expectedCode, int expectedOperation) {
            return commandCode == expectedCode && operation == expectedOperation;
        }

        /** The request body as text, for asserting which user a login names. */
        String bodyAsText() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private record Response(int operation, ByteBuf body) {
        static Response success(int operation, ByteBuf body) {
            return new Response(operation, body);
        }
    }

    @FunctionalInterface
    private interface RequestHandler {
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
                assertThat(survivorRegistrations)
                        .as("the sign-in was replayed on the survivor")
                        .hasValueGreaterThanOrEqualTo(1);
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
     * Only the server answering "no" ends a redial. A channel that closed
     * before the reply, or a node that refuses because it is not the primary,
     * says nothing about the credentials -- treated as a rejection, they are
     * dropped and the client is published on a node that is already gone.
     */
    @Test
    void shouldTreatOnlyANonTransientServerVerdictAsASignInRejection() {
        assertThat(AsyncIggyTcpClient.isSignInRejection(
                        IggyServerException.fromTcpResponse(IggyErrorCode.INVALID_CREDENTIALS.getCode(), new byte[0])))
                .isTrue();
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
