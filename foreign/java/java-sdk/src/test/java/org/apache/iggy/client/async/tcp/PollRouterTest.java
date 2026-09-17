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
import org.apache.iggy.client.async.tcp.VsrLoopbackPeer.Request;
import org.apache.iggy.client.async.tcp.VsrLoopbackPeer.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.VsrLoopbackPeer.clusterMetadata;
import static org.apache.iggy.client.async.tcp.VsrLoopbackPeer.registerBody;
import static org.apache.iggy.client.async.tcp.VsrLoopbackPeer.serveConcurrently;
import static org.apache.iggy.client.async.tcp.VsrLoopbackPeer.singleNodeMetadata;
import static org.apache.iggy.client.async.tcp.VsrLoopbackPeer.writeNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Deferred polling through {@link PollRouter}: the wire trailer, the dedicated
 * data connection and its exclusive lease, one monotonic budget, and the single
 * retry rule.
 */
class PollRouterTest {

    private static final int TRAILER_BYTES = 24;
    private static final int ATTACHMENT_BYTES = 32;
    private static final int PING_CODE = CommandCode.System.PING.getValue();
    private static final int POLL_CODE = CommandCode.Messages.POLL.getValue();
    private static final int POLL_DEFERRED_CODE = CommandCode.Messages.POLL_DEFERRED.getValue();
    private static final int POLL_DEFERRED_ON_PRIMARY_CODE = CommandCode.Messages.POLL_DEFERRED_ON_PRIMARY.getValue();
    private static final int GET_POLL_ROUTING_CODE = CommandCode.Messages.GET_POLL_ROUTING.getValue();
    private static final int GET_CLUSTER_METADATA_CODE = CommandCode.System.GET_CLUSTER_METADATA.getValue();
    private static final int ATTACH_CONSUMER_SESSION_CODE = CommandCode.System.ATTACH_CONSUMER_SESSION.getValue();
    private static final int TRANSIENT_NOT_COMMITTED = 57;
    private static final int TRANSIENT_NOT_ACCEPTED = 58;
    private static final long SESSION_EPOCH = 7;
    private static final int TEST_TIMEOUT_SECONDS = 15;

    @Test
    void shouldAppendTheTrailerToAnUnchangedPollBodyOnADedicatedConnection() throws Exception {
        var peer = new DeferredPeer();
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);
                poll(client);
                Request immediate = peer.immediatePolls.get(0);

                var options = DeferredPollOptions.defaults();
                PolledMessages polled =
                        deferredPoll(client, false, options).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(polled.messages()).isEmpty();
                Request deferred = peer.deferredPolls.get(0);
                assertThat(deferred.operation()).isEqualTo(VsrOperation.NON_REPLICATED);
                assertThat(deferred.body()).hasSize(immediate.body().length + TRAILER_BYTES);
                assertThat(Arrays.copyOf(deferred.body(), immediate.body().length))
                        .as("the immediate poll body travels unchanged")
                        .isEqualTo(immediate.body());

                var trailer = trailer(deferred.body());
                assertThat(trailer.waitMicros()).isPositive().isLessThanOrEqualTo(options.maxWaitMicros());
                assertThat(trailer.minCount()).isEqualTo(options.minCount());
                assertThat(trailer.maxBytes()).isEqualTo(options.maxBytes());
                assertThat(trailer.requestTimeoutMicros())
                        .isPositive()
                        .isLessThanOrEqualTo(options.requestTimeoutMicros());
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldAttachTheParentSessionToTheDataConnection() throws Exception {
        var peer = new DeferredPeer();
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);
                poll(client);
                Request parentRequest = peer.immediatePolls.get(0);

                deferredPoll(client, false, DeferredPollOptions.defaults()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(peer.attachments).hasSize(1);
                byte[] body = peer.attachments.get(0).body();
                assertThat(body).hasSize(ATTACHMENT_BYTES);
                var attachment = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
                assertThat(attachment.getLong(0)).isEqualTo(parentRequest.clientLow());
                assertThat(attachment.getLong(Long.BYTES)).isEqualTo(parentRequest.clientHigh());
                assertThat(attachment.getLong(2 * Long.BYTES)).isEqualTo(SESSION_EPOCH);

                assertThat(peer.deferredPolls.get(0).clientLow())
                        .as("the data connection has its own client id, so the session travels in the attachment")
                        .isNotEqualTo(parentRequest.clientLow());

                assertThat(peer.connections())
                        .as("a held poll never occupies the coordinator's own drain")
                        .anyMatch(connection ->
                                connection.contains(POLL_DEFERRED_CODE) && !connection.contains(POLL_CODE));
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldRouteAClusteredAutoCommitPollToThePrimaryWithCommand106() throws Exception {
        var coordinator = new DeferredPeer();
        var primary = new DeferredPeer();
        try (ServerSocket coordinatorSocket = coordinator.listen();
                ServerSocket primarySocket = primary.listen()) {
            coordinator.clusterPeerPort = primary.port;
            serveConcurrently(coordinatorSocket, coordinator::handle);
            serveConcurrently(primarySocket, primary::handle);
            AsyncIggyTcpClient client = client(coordinatorSocket, Duration.ofMinutes(1));
            try {
                connect(client);

                PolledMessages polled = deferredPoll(client, true, DeferredPollOptions.defaults())
                        .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(polled.messages()).isEmpty();
                assertThat(coordinator.routingRequests).hasValue(1);
                assertThat(coordinator.deferredPolls).isEmpty();
                assertThat(primary.deferredPolls).hasSize(1);
                assertThat(primary.deferredPolls.get(0).commandCode()).isEqualTo(POLL_DEFERRED_ON_PRIMARY_CODE);
                assertThat(primary.attachments).hasSize(1);
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldNotPingTheConnectionHoldingAPoll() throws Exception {
        var peer = new DeferredPeer();
        peer.holdMillis = 900;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMillis(100));
            try {
                connect(client);

                deferredPoll(client, false, DeferredPollOptions.defaults()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                List<List<Integer>> dataConnections = peer.connections().stream()
                        .filter(connection -> connection.contains(POLL_DEFERRED_CODE))
                        .toList();
                assertThat(dataConnections).hasSize(1);
                assertThat(dataConnections.get(0))
                        .as("a ping queued behind a held reply would expire and close the channel")
                        .doesNotContain(PING_CODE);
                assertThat(peer.pings)
                        .as("the coordinator keeps its own heartbeat")
                        .hasValueGreaterThan(0);
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldGiveEachConcurrentPollItsOwnConnection() throws Exception {
        var peer = new DeferredPeer();
        peer.holdMillis = 400;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);

                var first = deferredPoll(client, false, DeferredPollOptions.defaults());
                var second = deferredPoll(client, false, DeferredPollOptions.defaults());
                first.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                second.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(peer.deferredPolls).hasSize(2);
                assertThat(peer.maxConcurrentPolls)
                        .as("a lease is exclusive, so two waits can only overlap on two connections")
                        .hasValue(2);
                assertThat(peer.connections().stream()
                                .filter(connection -> connection.contains(POLL_DEFERRED_CODE))
                                .count())
                        .isEqualTo(2);
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldRetryAnExplicitRefusalInsideTheOriginalBudget() throws Exception {
        var peer = new DeferredPeer();
        peer.refusals = 2;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);

                deferredPoll(client, false, DeferredPollOptions.defaults()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(peer.deferredPolls).hasSize(3);
                var first = trailer(peer.deferredPolls.get(0).body());
                var last = trailer(peer.deferredPolls.get(2).body());
                assertThat(last.waitMicros())
                        .as("a retry inherits what is left of the readiness wait")
                        .isLessThan(first.waitMicros());
                assertThat(last.requestTimeoutMicros())
                        .as("and what is left of the total budget")
                        .isLessThan(first.requestTimeoutMicros());
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldNotReplayAnAmbiguousRefusal() throws Exception {
        var peer = new DeferredPeer();
        peer.uncommitted = true;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);

                assertThatThrownBy(() -> deferredPoll(client, false, DeferredPollOptions.defaults())
                                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .cause()
                        .isInstanceOf(IggyServerException.class)
                        .extracting(error -> ((IggyServerException) error).getRawErrorCode())
                        .isEqualTo(TRANSIENT_NOT_COMMITTED);
                assertThat(peer.deferredPolls)
                        .as("a poll that may have been served and auto-committed is never replayed")
                        .hasSize(1);
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldExpireOnTheCallersBudgetAsALocalTimeout() throws Exception {
        var peer = new DeferredPeer();
        peer.silentPolls = true;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);
                var options = DeferredPollOptions.defaults()
                        .withMaxWait(Duration.ofMillis(200))
                        .withRequestTimeout(Duration.ofMillis(400));

                long startedNanos = System.nanoTime();
                assertThatThrownBy(
                                () -> deferredPoll(client, false, options).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .hasCauseInstanceOf(IggyTimeoutException.class);

                assertThat(Duration.ofNanos(System.nanoTime() - startedNanos))
                        .as("the caller's budget bounds the call, not the transport's own timeout")
                        .isLessThan(Duration.ofSeconds(4));
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldDiscardTheDataConnectionWhenACallerCancels() throws Exception {
        var peer = new DeferredPeer();
        peer.holdMillis = 2000;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);
                var cancelled = deferredPoll(client, false, DeferredPollOptions.defaults());
                peer.awaitFirstPoll();

                assertThat(cancelled.cancel(false)).isTrue();

                peer.holdMillis = 0;
                deferredPoll(client, false, DeferredPollOptions.defaults()).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(peer.connections().stream()
                                .filter(connection -> connection.contains(POLL_DEFERRED_CODE))
                                .count())
                        .as("a late reply on the abandoned channel would desynchronize the next exchange")
                        .isEqualTo(2);
                assertThat(client.system().ping().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isNotNull();
            } finally {
                close(client);
            }
        }
    }

    @Test
    void shouldRefuseAReplyLargerThanTheDeclaredByteLimit() throws Exception {
        var peer = new DeferredPeer();
        peer.replyPaddingBytes = 4096;
        try (ServerSocket socket = peer.listen()) {
            serveConcurrently(socket, peer::handle);
            AsyncIggyTcpClient client = client(socket, Duration.ofMinutes(1));
            try {
                connect(client);
                var options = DeferredPollOptions.defaults().withMaxBytes(1024);

                assertThatThrownBy(
                                () -> deferredPoll(client, false, options).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class)
                        .rootCause()
                        .as("the cap is the header plus maxBytes, checked before the body is accumulated")
                        .hasMessageContaining("Invalid VSR frame size " + (VsrHeaders.HEADER_SIZE + 16 + 4096));
            } finally {
                close(client);
            }
        }
    }

    private static AsyncIggyTcpClient client(ServerSocket coordinator, Duration heartbeatInterval) {
        return AsyncIggyTcpClient.builder()
                .host(coordinator.getInetAddress().getHostAddress())
                .port(coordinator.getLocalPort())
                .credentials("iggy", "iggy")
                .requestTimeout(Duration.ofSeconds(5))
                .heartbeatInterval(heartbeatInterval)
                .build();
    }

    private static void connect(AsyncIggyTcpClient client) throws Exception {
        client.connect().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.login().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void close(AsyncIggyTcpClient client) throws Exception {
        client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static void poll(AsyncIggyTcpClient client) throws Exception {
        client.messages()
                .pollMessages(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.of(0L),
                        Consumer.of(3L),
                        PollingStrategy.next(),
                        10L,
                        false)
                .get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static CompletableFuture<PolledMessages> deferredPoll(
            AsyncIggyTcpClient client, boolean autoCommit, DeferredPollOptions options) {
        return client.messages()
                .pollMessagesDeferred(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.of(0L),
                        Consumer.of(3L),
                        PollingStrategy.next(),
                        10L,
                        autoCommit,
                        options);
    }

    private static Trailer trailer(byte[] body) {
        var fields = ByteBuffer.wrap(body, body.length - TRAILER_BYTES, TRAILER_BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        return new Trailer(
                fields.getLong(),
                Integer.toUnsignedLong(fields.getInt()),
                Integer.toUnsignedLong(fields.getInt()),
                fields.getLong());
    }

    private record Trailer(long waitMicros, long minCount, long maxBytes, long requestTimeoutMicros) {}

    /**
     * A node that records what each connection was asked for, so a test can tell
     * the coordinator's own drain from a deferred data connection.
     */
    private static final class DeferredPeer {
        private final ThreadLocal<List<Integer>> connection = ThreadLocal.withInitial(CopyOnWriteArrayList::new);
        private final List<List<Integer>> connections = new CopyOnWriteArrayList<>();
        private final List<Request> deferredPolls = new CopyOnWriteArrayList<>();
        private final List<Request> immediatePolls = new CopyOnWriteArrayList<>();
        private final List<Request> attachments = new CopyOnWriteArrayList<>();
        private final AtomicInteger routingRequests = new AtomicInteger();
        private final AtomicInteger pings = new AtomicInteger();
        private final AtomicInteger activePolls = new AtomicInteger();
        private final AtomicInteger maxConcurrentPolls = new AtomicInteger();
        private final CompletableFuture<Void> firstPoll = new CompletableFuture<>();
        private int port;
        private int clusterPeerPort;
        private volatile long holdMillis;
        private int refusals;
        private boolean uncommitted;
        private boolean silentPolls;
        private int replyPaddingBytes;

        private ServerSocket listen() throws IOException {
            ServerSocket socket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
            port = socket.getLocalPort();
            return socket;
        }

        private void awaitFirstPoll() throws Exception {
            firstPoll.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        private List<List<Integer>> connections() {
            return List.copyOf(connections);
        }

        private Response handle(Request request) {
            List<Integer> seen = connection.get();
            if (seen.isEmpty()) {
                connections.add(seen);
            }
            seen.add(request.commandCode());
            if (request.is(POLL_DEFERRED_CODE, VsrOperation.NON_REPLICATED)
                    || request.is(POLL_DEFERRED_ON_PRIMARY_CODE, VsrOperation.NON_REPLICATED)) {
                return handleDeferred(request);
            }
            if (request.is(POLL_CODE, VsrOperation.NON_REPLICATED)) {
                immediatePolls.add(request);
                return Response.success(VsrOperation.NON_REPLICATED, emptyPoll());
            }
            return handleSession(request);
        }

        private Response handleSession(Request request) {
            if (request.operation() == VsrOperation.REGISTER) {
                return Response.success(VsrOperation.REGISTER, registerBody(SESSION_EPOCH));
            }
            if (request.is(PING_CODE, VsrOperation.NON_REPLICATED)) {
                pings.incrementAndGet();
                return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, VsrOperation.NON_REPLICATED)) {
                return Response.success(
                        VsrOperation.NON_REPLICATED,
                        clusterPeerPort == 0 ? singleNodeMetadata(port) : clusterMetadata(port, clusterPeerPort, port));
            }
            if (request.is(GET_POLL_ROUTING_CODE, VsrOperation.NON_REPLICATED)) {
                routingRequests.incrementAndGet();
                return Response.success(VsrOperation.NON_REPLICATED, route(request));
            }
            if (request.is(ATTACH_CONSUMER_SESSION_CODE, VsrOperation.NON_REPLICATED)) {
                attachments.add(request);
                return Response.success(VsrOperation.NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            }
            throw new IllegalStateException("Unexpected request: " + request.commandCode());
        }

        private Response handleDeferred(Request request) {
            deferredPolls.add(request);
            firstPoll.complete(null);
            if (silentPolls) {
                return Response.noReply();
            }
            if (uncommitted) {
                return Response.error(VsrOperation.NON_REPLICATED, TRANSIENT_NOT_COMMITTED);
            }
            if (refusals > 0) {
                refusals--;
                return Response.error(VsrOperation.NON_REPLICATED, TRANSIENT_NOT_ACCEPTED);
            }
            hold();
            ByteBuf body = emptyPoll();
            if (replyPaddingBytes > 0) {
                body.writeZero(replyPaddingBytes);
            }
            return Response.success(VsrOperation.NON_REPLICATED, body);
        }

        private void hold() {
            long millis = holdMillis;
            if (millis == 0) {
                return;
            }
            int active = activePolls.incrementAndGet();
            maxConcurrentPolls.accumulateAndGet(active, Math::max);
            try {
                Thread.sleep(millis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } finally {
                activePolls.decrementAndGet();
            }
        }

        private ByteBuf route(Request request) {
            ByteBuf body = Unpooled.buffer()
                    .writeLongLE(request.clientLow())
                    .writeLongLE(request.clientHigh())
                    .writeLongLE(SESSION_EPOCH)
                    .writeLongLE(0);
            writeNode(body, "primary", clusterPeerPort, false);
            return body;
        }

        private static ByteBuf emptyPoll() {
            return Unpooled.buffer().writeIntLE(0).writeLongLE(0).writeIntLE(0);
        }
    }
}
