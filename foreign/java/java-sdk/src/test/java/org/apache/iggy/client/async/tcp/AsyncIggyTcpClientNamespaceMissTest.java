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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.clusterMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.threeNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A partition primary that is a metadata backup can miss a stream, topic or partition that the metadata primary
 * already committed. Such a miss routes the send again from the metadata primary until the deadline, and only a
 * miss of the metadata primary is final.
 */
class AsyncIggyTcpClientNamespaceMissTest {
    // The scripted status of a request that a fake node answers with success.
    static final int ANSWERED = 0;
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int OPERATION_STORE_CONSUMER_OFFSET = 161;
    // The deadline has to find a lookup in flight. Instant answers leave that to where it lands between attempts.
    private static final Duration LOOKUP_HELD_AFTER = Duration.ofSeconds(1);

    @ParameterizedTest
    @EnumSource(
            value = IggyErrorCode.class,
            names = {
                "STREAM_ID_NOT_FOUND",
                "STREAM_NAME_NOT_FOUND",
                "TOPIC_ID_NOT_FOUND",
                "TOPIC_NAME_NOT_FOUND",
                "PARTITION_NOT_FOUND"
            })
    void shouldRouteAgainFromTheMetadataPrimaryAfterABackupMiss(IggyErrorCode missing) throws Exception {
        try (Cluster cluster = new Cluster()) {
            cluster.backup.sendStatus = sends -> sends == 1 ? missing.getCode() : ANSWERED;
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(2);
            assertThat(cluster.backup.lookups).isEmpty();
            assertThat(cluster.backup.sends).hasSize(2);
            // The miss refused the operation before admission, so the same operation goes out again.
            assertThat(cluster.backup.sends.get(1).requestId())
                    .isEqualTo(cluster.backup.sends.get(0).requestId());
        }
    }

    @Test
    void shouldReturnTheRefusalOfTheMetadataPrimaryAfterABackupMiss() throws Exception {
        try (Cluster cluster = new Cluster()) {
            cluster.backup.sendStatus = sends -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            cluster.leader.lookupStatus = lookups -> lookups == 1 ? ANSWERED : IggyErrorCode.UNAUTHORIZED.getCode();
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                assertServerError(send(client), IggyErrorCode.UNAUTHORIZED.getCode());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(2);
            assertThat(cluster.backup.sends).hasSize(1);
        }
    }

    @Test
    void shouldKeepAMissOfTheMetadataPrimaryFinal() throws Exception {
        try (Cluster cluster = new Cluster()) {
            cluster.primaryPort = cluster.leader.socket.getLocalPort();
            cluster.leader.sendStatus = sends -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                assertServerError(send(client), IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(1);
            assertThat(cluster.leader.sends).hasSize(1);
        }
    }

    @Test
    void shouldReturnNonAdmissionWhenTheBackupMissesUntilTheDeadline() throws Exception {
        try (Cluster cluster = new Cluster()) {
            CompletableFuture<Void> release = new CompletableFuture<>();
            cluster.backup.sendStatus = sends -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            AsyncIggyTcpClient client = cluster.signIn();
            long sendStart = System.nanoTime();
            cluster.leader.lookupStatus = lookups -> {
                if (System.nanoTime() - sendStart > LOOKUP_HELD_AFTER.toNanos()) {
                    release.join();
                }
                return ANSWERED;
            };
            try {
                assertServerError(send(client), TRANSIENT_NOT_ACCEPTED);
            } finally {
                release.complete(null);
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.backup.sends).hasSizeGreaterThan(1);
            assertThat(cluster.leader.lookups).hasSize(cluster.backup.sends.size() + 1);
        }
    }

    /** The coordinator can lag behind a metadata primary that moved, so the lookup after a miss skips it. */
    @Test
    void shouldLookUpTheRouteOnTheMetadataPrimaryRatherThanTheCoordinatorAfterABackupMiss() throws Exception {
        try (Cluster cluster = new Cluster()) {
            int coordinatorPort = cluster.leader.socket.getLocalPort();
            cluster.primaryPort = coordinatorPort;
            cluster.leader.sendStatus = sends -> sends == 1 ? IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode() : ANSWERED;
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                cluster.leaderPort = cluster.backup.socket.getLocalPort();
                client.findLeaderElsewhere(client.getConnectionInfo()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertThat(client.getConnectionInfo().port()).isEqualTo(coordinatorPort);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(1);
            assertThat(cluster.backup.lookups).hasSize(1);
            assertThat(cluster.leader.sends).hasSize(2);
        }
    }

    /** The metadata primary that the client believes in was deposed, and its roster names the elected one. */
    @ParameterizedTest(name = "missed by the send: {0}")
    @ValueSource(booleans = {false, true})
    void shouldRouteFromTheElectedMetadataPrimaryWhenTheDeposedOneMisses(boolean missedBySend) throws Exception {
        try (Cluster cluster = new Cluster()) {
            if (missedBySend) {
                cluster.primaryPort = cluster.leader.socket.getLocalPort();
                cluster.leader.sendStatus = sends -> sends == 1 ? IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode() : ANSWERED;
            } else {
                cluster.leader.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            }
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                cluster.leaderPort = cluster.backup.socket.getLocalPort();
                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(1);
            assertThat(cluster.backup.lookups).hasSize(1);
            Node primary = missedBySend ? cluster.leader : cluster.backup;
            assertThat(primary.sends)
                    .hasSize(missedBySend ? 2 : 1)
                    .extracting(Request::requestId)
                    .containsOnly(primary.sends.get(0).requestId());
        }
    }

    /** A client configured with a hostname names the node of its connection otherwise than a roster of addresses. */
    @Test
    void shouldKeepAMissOfTheMetadataPrimaryFinalWhenTheClientNamesItByHostname() throws Exception {
        try (Cluster cluster = new Cluster()) {
            cluster.leader.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            AsyncIggyTcpClient client =
                    cluster.signIn(InetAddress.getLoopbackAddress().getHostName());
            try {
                assertServerError(send(client), IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(1);
            assertThat(cluster.leader.codes).endsWith(GET_PARTITION_ROUTE_CODE, GET_CLUSTER_METADATA_CODE);
            assertThat(cluster.backup.lookups).isEmpty();
        }
    }

    @Test
    void shouldLookUpOnTheMetadataPrimaryNextAfterABackupMissesALookup() throws Exception {
        try (Cluster cluster = new Cluster(true)) {
            cluster.leader.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            cluster.backup.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            cluster.primaryPort = cluster.third.socket.getLocalPort();
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                cluster.leaderPort = cluster.third.socket.getLocalPort();
                client.findLeaderElsewhere(client.getConnectionInfo()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.lookups).hasSize(1);
            assertThat(cluster.backup.lookups).isEmpty();
            assertThat(cluster.third.lookups).hasSize(1);
            assertThat(cluster.third.sends).hasSize(1);
        }
    }

    /** No lookup reaches a believed metadata primary that is down, so a roster read elsewhere has to move the belief. */
    @Test
    void shouldKeepAMissOfTheElectedMetadataPrimaryFinalWhileTheBelievedOneIsDown() throws Exception {
        try (Cluster cluster = new Cluster(true)) {
            cluster.backup.down = true;
            cluster.leader.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            cluster.third.lookupStatus = lookups -> IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode();
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                cluster.leaderPort = cluster.backup.socket.getLocalPort();
                client.findLeaderElsewhere(client.getConnectionInfo()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                cluster.leaderPort = cluster.third.socket.getLocalPort();
                assertServerError(send(client), IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.third.codes).endsWith(GET_PARTITION_ROUTE_CODE, GET_CLUSTER_METADATA_CODE);
        }
    }

    /** The miss refused the send before admission, so a deadline during its roster read retains no operation. */
    @Test
    void shouldEndAsNotAcceptedWhenTheDeadlineFindsTheRosterReadOfAMiss() throws Exception {
        try (Cluster cluster = new Cluster()) {
            cluster.primaryPort = cluster.leader.socket.getLocalPort();
            cluster.leader.sendStatus = sends -> sends == 1 ? IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode() : ANSWERED;
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                cluster.leader.rosterHeld = new CompletableFuture<>();
                assertServerError(send(client), TRANSIENT_NOT_ACCEPTED);
                cluster.leader.rosterHeld.complete(null);
                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                cluster.leader.rosterHeld.complete(null);
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(cluster.leader.sends).hasSize(2);
        }
    }

    /**
     * The metadata primary drops the link of the client after it routes the send, and it refuses the bind on the next
     * link, so the client registers a new session before the retry of the miss. No node admitted the send, so it
     * goes out again under that session with a new request id and does not end uncertain.
     */
    @Test
    @SuppressWarnings("checkstyle:NPathComplexity")
    void shouldSendABackupMissAgainUnderTheNextSessionWhenTheSessionIsLostBeforeItsRetry() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        // The link that routes the send and the link that finds the session lost.
        int leaderLinks = 2;
        try (ServerSocket leader = new ServerSocket(0, leaderLinks, loopback);
                ServerSocket backup = new ServerSocket(0, 1, loopback)) {
            List<Request> registers = new CopyOnWriteArrayList<>();
            AtomicInteger lookups = new AtomicInteger();
            var leaderServed = serve(leader, leaderLinks, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    registers.add(request);
                    return Response.success(OPERATION_REGISTER, registerBody(registers.size()));
                }
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.error(OPERATION_NON_REPLICATED, IggyErrorCode.UNAUTHENTICATED.getCode());
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            clusterMetadata(leader.getLocalPort(), backup.getLocalPort(), leader.getLocalPort()));
                }
                if (!request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    throw new IllegalStateException("Unexpected leader request: " + request);
                }
                ByteBuf route = pollRoute(request, 1, 1, backup.getLocalPort());
                return lookups.incrementAndGet() == 1
                        ? Response.successAndDisconnect(OPERATION_NON_REPLICATED, route)
                        : Response.success(OPERATION_NON_REPLICATED, route);
            });
            List<Request> sends = new CopyOnWriteArrayList<>();
            var backupServed = serve(backup, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            clusterMetadata(leader.getLocalPort(), backup.getLocalPort(), leader.getLocalPort()));
                }
                if (request.operation() != OPERATION_SEND_MESSAGES) {
                    throw new IllegalStateException("Unexpected backup request: " + request);
                }
                sends.add(request);
                return sends.size() == 1
                        ? Response.error(OPERATION_SEND_MESSAGES, IggyErrorCode.TOPIC_ID_NOT_FOUND.getCode())
                        : Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
            });
            AsyncIggyTcpClient client = client(leader);
            try {
                client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(registers).hasSize(2);
                assertThat(sends).hasSize(2);
                Request retried = sends.get(1);
                assertThat(retried.requestId()).isNotEqualTo(sends.get(0).requestId());
                assertThat(retried.clientLow()).isEqualTo(registers.get(1).clientLow());
                assertThat(retried.clientHigh()).isEqualTo(registers.get(1).clientHigh());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            leaderServed.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            backupServed.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<SendMessagesResponse> send(AsyncIggyTcpClient client) {
        return client.messages()
                .sendMessages(StreamId.of(1L), TopicId.of(1L), Partitioning.partitionId(0L), List.of(Message.of("m")));
    }

    /**
     * Two or three nodes. The client signs in on the leader, which leads metadata until a test moves it, and every
     * lookup routes the partition to the backup unless a test names another primary.
     */
    static final class Cluster implements AutoCloseable {
        final Node leader = new Node();
        final Node backup = new Node();
        private final Node third;
        private final List<CompletableFuture<Void>> served = new ArrayList<>();
        private volatile int leaderPort = leader.socket.getLocalPort();
        private volatile int primaryPort = backup.socket.getLocalPort();

        Cluster() throws IOException {
            this(false);
        }

        Cluster(boolean threeNodes) throws IOException {
            third = threeNodes ? new Node() : null;
            for (Node node : nodes()) {
                served.add(serve(node.socket, request -> node.answer(request, this)));
            }
        }

        AsyncIggyTcpClient signIn() throws Exception {
            return signIn(leader.socket.getInetAddress().getHostAddress());
        }

        AsyncIggyTcpClient signIn(String host) throws Exception {
            AsyncIggyTcpClient client = client(host, leader.socket.getLocalPort());
            client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return client;
        }

        /** Raises what a node failed on, such as a request it did not expect. */
        void awaitServed() throws Exception {
            for (CompletableFuture<Void> node : served) {
                node.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        }

        private List<Node> nodes() {
            return third == null ? List.of(leader, backup) : List.of(leader, backup, third);
        }

        private ByteBuf roster() {
            return third == null
                    ? clusterMetadata(leader.socket.getLocalPort(), backup.socket.getLocalPort(), leaderPort)
                    : threeNodeMetadata(
                            leader.socket.getLocalPort(),
                            backup.socket.getLocalPort(),
                            third.socket.getLocalPort(),
                            leaderPort);
        }

        @Override
        public void close() throws IOException {
            for (Node node : nodes()) {
                node.socket.close();
            }
        }
    }

    /** A fake node that answers its lookups and writes with the status a test scripts for each of them. */
    static final class Node {
        final List<Request> sends = new CopyOnWriteArrayList<>();
        final List<Request> offsetStores = new CopyOnWriteArrayList<>();
        volatile IntUnaryOperator sendStatus = sends -> ANSWERED;
        volatile IntUnaryOperator offsetStoreStatus = stores -> ANSWERED;
        private final ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        // The command code of every request, in arrival order. A replicated request carries none.
        private final List<Integer> codes = new CopyOnWriteArrayList<>();
        private final List<Request> lookups = new CopyOnWriteArrayList<>();
        private volatile IntUnaryOperator lookupStatus = lookups -> ANSWERED;
        // Every roster read waits for it.
        private volatile CompletableFuture<Void> rosterHeld = CompletableFuture.completedFuture(null);
        // A node that is down drops its connection at the first request.
        private volatile boolean down;

        Node() throws IOException {}

        Response answer(Request request, Cluster cluster) {
            codes.add(request.commandCode());
            if (down) {
                return Response.disconnect();
            }
            if (request.operation() == OPERATION_REGISTER) {
                return Response.success(OPERATION_REGISTER, registerBody(1));
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                rosterHeld.join();
                return Response.success(OPERATION_NON_REPLICATED, cluster.roster());
            }
            if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
            }
            if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                lookups.add(request);
                int status = lookupStatus.applyAsInt(lookups.size());
                return status == ANSWERED
                        ? Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, cluster.primaryPort))
                        : Response.error(OPERATION_NON_REPLICATED, status);
            }
            if (request.operation() == OPERATION_SEND_MESSAGES) {
                return write(request, sends, sendStatus, Unpooled.EMPTY_BUFFER);
            }
            if (request.operation() == OPERATION_STORE_CONSUMER_OFFSET) {
                return write(
                        request,
                        offsetStores,
                        offsetStoreStatus,
                        Unpooled.buffer().writeIntLE(0));
            }
            throw new IllegalStateException("Unexpected request: " + request);
        }

        private static Response write(Request request, List<Request> writes, IntUnaryOperator status, ByteBuf body) {
            writes.add(request);
            int answer = status.applyAsInt(writes.size());
            if (answer == ANSWERED) {
                return Response.success(request.operation(), body);
            }
            body.release();
            return Response.error(request.operation(), answer);
        }
    }
}
