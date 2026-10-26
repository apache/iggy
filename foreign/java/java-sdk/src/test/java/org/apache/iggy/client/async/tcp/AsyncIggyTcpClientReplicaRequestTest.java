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
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.CREATE_STREAM_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_CREATE_STREAM;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.clusterMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.assertj.core.api.Assertions.assertThat;

class AsyncIggyTcpClientReplicaRequestTest {
    private static final int GET_STATS_CODE = 10;
    private static final int LOGIN_CODE = 38;
    private static final int LOGOUT_CODE = 39;
    private static final int POLL_DEFERRED_CODE = 106;
    private static final int WAIT_FOR_MESSAGES_CODE = 107;
    private static final int CANCEL_DEFERRED_REQUEST_CODE = 108;
    private static final int GET_OFFSET_CODE = 120;
    private static final int NOT_PRIMARY = 89;
    private static final long METADATA_COMMIT = 11;
    private static final int TEST_TIMEOUT_SECONDS = 5;
    private static final byte[] LEADER_STATS = "leader".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BACKUP_STATS = "backup".getBytes(StandardCharsets.UTF_8);

    @Test
    void shouldSendToTheChosenReplicaWithTheSessionWatermark() throws Exception {
        try (Cluster cluster = new Cluster(1, stats -> Response.success(OPERATION_NON_REPLICATED, backupStats()))) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                client.sendBinaryRequest(CREATE_STREAM_CODE, new byte[0]).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                byte[] backup = client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[] {1}, 1)
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                byte[] leader = client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[] {0}, 0)
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(backup).isEqualTo(BACKUP_STATS);
                assertThat(leader).isEqualTo(LEADER_STATS);
                assertThat(cluster.backupStats).singleElement().satisfies(request -> {
                    assertThat(request.body()).containsExactly(1);
                    assertThat(request.metadataOp()).isEqualTo(METADATA_COMMIT);
                });
                assertThat(cluster.leaderStats).singleElement().satisfies(request -> {
                    assertThat(request.body()).containsExactly(0);
                    assertThat(request.metadataOp()).isEqualTo(METADATA_COMMIT);
                });
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    @Test
    void shouldReturnNotPrimaryOfTheChosenReplicaWithoutSendingElsewhere() throws Exception {
        try (Cluster cluster = new Cluster(1, stats -> Response.error(OPERATION_NON_REPLICATED, NOT_PRIMARY))) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                assertServerError(client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[0], 1), NOT_PRIMARY);

                assertThat(cluster.backupStats).hasSize(1);
                assertThat(cluster.leaderStats).isEmpty();
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    @Test
    void shouldAskTheSameReplicaAgainAfterNotAcceptedAndALostLink() throws Exception {
        try (Cluster cluster = new Cluster(2, stats -> switch (stats) {
            case 1 -> Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
            case 2 -> Response.disconnect();
            default -> Response.success(OPERATION_NON_REPLICATED, backupStats());
        })) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                byte[] backup = client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[0], 1)
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(backup).isEqualTo(BACKUP_STATS);
                assertThat(cluster.backupStats).hasSize(3);
                assertThat(cluster.leaderStats).isEmpty();
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    @Test
    void shouldEndWithNotAcceptedWhenTheReplicaRefusesUntilTheDeadline() throws Exception {
        try (Cluster cluster =
                new Cluster(1, stats -> Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_ACCEPTED))) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                assertServerError(
                        client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[0], 1), TRANSIENT_NOT_ACCEPTED);

                assertThat(cluster.backupStats.size()).isGreaterThan(1);
                assertThat(cluster.leaderStats).isEmpty();
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    @Test
    void shouldRejectAReplicaThatTheRosterDoesNotNameAfterOneRosterRead() throws Exception {
        try (Cluster cluster = new Cluster(1, stats -> Response.success(OPERATION_NON_REPLICATED, backupStats()))) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                int rosterReads = cluster.rosterReads.get();

                assertServerError(
                        client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[0], 2),
                        IggyErrorCode.INVALID_IDENTIFIER.getCode());

                assertThat(cluster.rosterReads).hasValue(rosterReads + 1);
                assertThat(cluster.backupStats).isEmpty();
                assertThat(cluster.leaderStats).isEmpty();
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    @Test
    void shouldRejectReplicatedPartitionReadAndSessionControlCodesBeforeSending() throws Exception {
        try (Cluster cluster = new Cluster(1, stats -> Response.success(OPERATION_NON_REPLICATED, backupStats()))) {
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                int requests = cluster.leaderRequests.get();

                for (int code : List.of(
                        CREATE_STREAM_CODE,
                        POLL_CODE,
                        POLL_DEFERRED_CODE,
                        WAIT_FOR_MESSAGES_CODE,
                        CANCEL_DEFERRED_REQUEST_CODE,
                        GET_OFFSET_CODE,
                        LOGIN_CODE,
                        LOGOUT_CODE)) {
                    assertServerError(
                            client.sendBinaryRequestToReplica(code, new byte[0], 1),
                            IggyErrorCode.INVALID_COMMAND.getCode());
                }

                assertThat(cluster.leaderRequests).hasValue(requests);
                assertThat(cluster.backupStats).isEmpty();
                assertThat(client.sendBinaryRequestToReplica(GET_STATS_CODE, new byte[0], 1)
                                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isEqualTo(BACKUP_STATS);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
        }
    }

    private static ByteBuf backupStats() {
        return Unpooled.wrappedBuffer(BACKUP_STATS);
    }

    /**
     * Replica 0 leads metadata and is the node the client signs in on. Replica 1 is a backup that answers the stats
     * requests with what the test scripts for each of them, numbered from 1.
     */
    private static final class Cluster implements AutoCloseable {
        final List<Request> leaderStats = new CopyOnWriteArrayList<>();
        final List<Request> backupStats = new CopyOnWriteArrayList<>();
        final AtomicInteger rosterReads = new AtomicInteger();
        final AtomicInteger leaderRequests = new AtomicInteger();
        private final ServerSocket leader = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        private final ServerSocket backup = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
        private final Function<Integer, Response> backupAnswer;
        private final CompletableFuture<Void> leaderServed;
        private final CompletableFuture<Void> backupServed;

        Cluster(int backupLinks, Function<Integer, Response> backupAnswer) throws Exception {
            this.backupAnswer = backupAnswer;
            leaderServed = serve(leader, this::answerOnLeader);
            backupServed = serve(backup, backupLinks, this::answerOnBackup);
        }

        AsyncIggyTcpClient signIn() throws Exception {
            AsyncIggyTcpClient client = client(leader);
            client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return client;
        }

        /** Raises what a node failed on, such as a request it did not expect. */
        void awaitServed() throws Exception {
            leaderServed.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            backupServed.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        private Response answerOnLeader(Request request) {
            leaderRequests.incrementAndGet();
            if (request.operation() == OPERATION_REGISTER) {
                return Response.success(OPERATION_REGISTER, registerBody(1));
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                rosterReads.incrementAndGet();
                return Response.success(OPERATION_NON_REPLICATED, roster());
            }
            if (request.operation() == OPERATION_CREATE_STREAM) {
                return Response.committed(
                        OPERATION_CREATE_STREAM,
                        METADATA_COMMIT,
                        Unpooled.buffer().writeIntLE(0));
            }
            if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED)) {
                leaderStats.add(request);
                return Response.success(OPERATION_NON_REPLICATED, Unpooled.wrappedBuffer(LEADER_STATS));
            }
            throw new IllegalStateException("Unexpected leader request: " + request);
        }

        private Response answerOnBackup(Request request) {
            if (request.operation() == OPERATION_REGISTER) {
                return Response.success(OPERATION_REGISTER, registerBody(2));
            }
            if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, roster());
            }
            if (request.is(GET_STATS_CODE, OPERATION_NON_REPLICATED)) {
                backupStats.add(request);
                return backupAnswer.apply(backupStats.size());
            }
            throw new IllegalStateException("Unexpected backup request: " + request);
        }

        private ByteBuf roster() {
            return clusterMetadata(leader.getLocalPort(), backup.getLocalPort(), leader.getLocalPort());
        }

        @Override
        public void close() throws Exception {
            leader.close();
            backup.close();
        }
    }
}
