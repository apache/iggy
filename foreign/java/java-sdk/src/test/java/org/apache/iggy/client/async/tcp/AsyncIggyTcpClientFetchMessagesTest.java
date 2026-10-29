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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.RequestHandler;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.threeNodeMetadata;
import static org.apache.iggy.client.async.tcp.FetchMessagesTest.GOLDEN_LIMITS;
import static org.apache.iggy.client.async.tcp.FetchMessagesTest.GOLDEN_MAX_BYTES;
import static org.apache.iggy.client.async.tcp.FetchMessagesTest.GOLDEN_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The typed fetch against mock nodes: the encoded body and its fences, the primary that gets it, the routes that its
 * refusals mark moved, the fetches that the client never resends or cancels, and the reply that it reads.
 */
class AsyncIggyTcpClientFetchMessagesTest {
    private static final int POLL_DEFERRED_CODE = CommandCode.Messages.POLL_DEFERRED.getValue();
    private static final String DEFERRED_POLL_BODY = "0201040700000001040100000002066f72646572730103000000050000000000"
            + "000000640000000140420f00000000000100000000001000c0d8a70000000000";
    private static final int DEFERRED_LIMITS_BYTES = 2 * Long.BYTES + 2 * Integer.BYTES;
    private static final Duration ROUTING_ALLOWANCE = Duration.ofSeconds(1);
    private static final Duration LATE_REPLY = Duration.ofMillis(200);
    private static final int FETCH_CODE = CommandCode.Messages.FETCH_MESSAGES.getValue();
    private static final int CANCEL_CODE = CommandCode.Messages.CANCEL_DEFERRED_REQUEST.getValue();
    private static final int NOT_PRIMARY = IggyErrorCode.NOT_PRIMARY.getCode();
    private static final int TRANSIENT_NOT_COMMITTED = IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode();
    private static final int HISTORY_UNAVAILABLE = IggyErrorCode.HISTORY_UNAVAILABLE.getCode();
    private static final HexFormat HEX = HexFormat.of();
    // The consumer, stream and topic of a fetch whose ids are all numeric, which the limits follow.
    private static final int NUMERIC_LIMITS = 19;
    // The routes of partition p carry the incarnation ROUTE_INCARNATION + p.
    private static final long ROUTE_INCARNATION = 10;
    private static final long COUNT = 100;
    private static final StreamId STREAM = StreamId.of(1L);
    private static final TopicId TOPIC = TopicId.of(1L);
    private static final Consumer GROUP = Consumer.group(ConsumerId.of(7L));
    private static final DeferredPollOptions FETCH = DeferredPollOptions.defaults();
    // A fetch without a wait still needs the reply margin, and this budget leaves it room for a few retries.
    private static final DeferredPollOptions IMMEDIATE =
            FETCH.withMaxWait(Duration.ZERO).withRequestTimeout(Duration.ofSeconds(2));
    // The limits of the golden request, whose budgets the call counts down.
    private static final DeferredPollOptions GOLDEN =
            new DeferredPollOptions(Duration.ofSeconds(5), 1, GOLDEN_MAX_BYTES, Duration.ofSeconds(6));
    private static final PartitionContext GOLDEN_CONTEXT =
            new PartitionContext(BigInteger.valueOf(3), BigInteger.valueOf(9), BigInteger.valueOf(12));
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /**
     * The typed call fills each target cap with the byte limit of the request, so the golden request, whose targets
     * have smaller caps, goes out with only the caps changed. Each target carries the fence of its strategy, and the
     * header carries none.
     */
    @Test
    void shouldSendTheGoldenBodyWithEachTargetCappedByTheByteLimit() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
                targets.put(1L, PollingStrategy.offset(BigInteger.valueOf(41)).withContext(GOLDEN_CONTEXT));
                targets.put(2L, PollingStrategy.next().withContext(GOLDEN_CONTEXT));

                assertThat(client.messages()
                                .fetchMessages(STREAM, orders(), targets, GROUP, COUNT, true, GOLDEN)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();

                ByteBuf expected = Unpooled.wrappedBuffer(HEX.parseHex(GOLDEN_REQUEST));
                for (int target = 0; target < targets.size(); target++) {
                    expected.setIntLE(target(GOLDEN_LIMITS, target) + FetchMessages.TARGET_MAX_BYTES_OFFSET, (int)
                            GOLDEN_MAX_BYTES);
                }
                Request fetch = fetches.get(0);
                assertGoldenBody(
                        fetch.body(), HEX.formatHex(expected.array()), GOLDEN_LIMITS, GOLDEN_LIMITS + Long.BYTES);
                assertThat(List.of(fetch.incarnation(), fetch.ownerGeneration()))
                        .containsOnly(0L);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** One fetch goes to the primary of its first target, and a target without a fence carries the one of its route. */
    @Test
    void shouldSendAFetchToThePrimaryOfItsFirstTargetWithTheFenceOfEachRoute() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket first = new ServerSocket(0, 4, loopback);
                ServerSocket second = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), first.getLocalPort(), second.getLocalPort(), leader.getLocalPort());
            CompletableFuture<Void> leaderServed = node(leader, metadata, request -> {
                if (!request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                int partition = lastInt(request.body());
                int primary = partition == 1 ? first.getLocalPort() : second.getLocalPort();
                return Response.success(
                        OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primary, ROUTE_INCARNATION + partition));
            });
            List<Request> misrouted = new CopyOnWriteArrayList<>();
            node(first, metadata, request -> {
                misrouted.add(request);
                return unexpected(request);
            });
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> secondServed = node(second, metadata, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
                targets.put(2L, PollingStrategy.next());
                targets.put(1L, PollingStrategy.offset(BigInteger.valueOf(5)));

                client.messages()
                        .fetchMessages(STREAM, TOPIC, targets, GROUP, COUNT, false, FETCH)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                Request fetch = fetches.get(0);
                assertThat(List.of(fetch.incarnation(), fetch.ownerGeneration()))
                        .containsOnly(0L);
                assertThat(targets(fetch))
                        .containsExactly(
                                new Target(2, PollingKind.Next.asCode(), 0, FETCH.maxBytes(), ROUTE_INCARNATION + 2, 0),
                                new Target(
                                        1, PollingKind.Offset.asCode(), 5, FETCH.maxBytes(), ROUTE_INCARNATION + 1, 0));
                assertThat(misrouted).isEmpty();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            CompletableFuture.allOf(leaderServed, secondServed).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Each readiness group gets its own fetch, so the reply of one primary neither waits for nor ends the fetch that
     * another primary holds.
     */
    @Test
    void shouldServeTheFetchOfOneGroupWhileAnotherPrimaryHoldsItsFetch() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket first = new ServerSocket(0, 4, loopback);
                ServerSocket second = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), first.getLocalPort(), second.getLocalPort(), leader.getLocalPort());
            CompletableFuture<Void> leaderServed = node(leader, metadata, routes(first, second));
            CompletableFuture<Void> firstServed = node(
                    first,
                    metadata,
                    request -> request.is(FETCH_CODE, OPERATION_NON_REPLICATED)
                            ? Response.success(OPERATION_NON_REPLICATED, emptySection(1))
                            : unexpected(request));
            CompletableFuture<Request> held = new CompletableFuture<>();
            CompletableFuture<Void> released = new CompletableFuture<>();
            List<Request> cancels = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> secondServed = node(second, metadata, request -> {
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED)) {
                    cancels.add(request);
                    return Response.noReply();
                }
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED) || !held.complete(request)) {
                    return unexpected(request);
                }
                after(released);
                return Response.success(OPERATION_NON_REPLICATED, emptySection(2));
            });
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                var messages = client.messages();
                assertThat(messages.readinessGroups(STREAM, TOPIC, List.of(1L, 2L))
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .containsExactlyInAnyOrder(List.of(1L), List.of(2L));
                var slow = messages.fetchMessages(STREAM, TOPIC, offsetsOf(2L), GROUP, COUNT, false, FETCH);
                held.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(messages.fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, false, FETCH)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .extracting(FetchedMessages::partitionId)
                        .containsExactly(1L);
                assertThat(slow).isNotDone();
                released.complete(null);

                assertThat(slow.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .extracting(FetchedMessages::partitionId)
                        .containsExactly(2L);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            CompletableFuture.allOf(leaderServed, firstServed, secondServed)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(cancels).isEmpty();
        }
    }

    /** A NOT_PRIMARY section marks the route of its partition moved, so the next grouping looks it up again. */
    @Test
    void shouldLookUpTheRouteOfAPartitionWhoseSectionIsNotPrimaryAgain() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Integer> lookups = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = node(socket, () -> singleNodeMetadata(socket.getLocalPort()), request -> {
                if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    lookups.add(lastInt(request.body()));
                    return Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, socket.getLocalPort()));
                }
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                ByteBuf reply = Unpooled.buffer().writeIntLE(2);
                section(reply, 1, FetchedMessages.STATUS_OK, 0);
                section(reply, 2, NOT_PRIMARY, 0);
                return Response.success(OPERATION_NON_REPLICATED, reply);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertThat(client.messages()
                                .readinessGroups(STREAM, TOPIC, List.of(1L, 2L))
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .containsExactly(List.of(1L, 2L));
                List<FetchedMessages> sections = client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, false, FETCH)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(sections).extracting(FetchedMessages::status).containsExactly(0L, (long) NOT_PRIMARY);
                assertThat(lookups).containsExactlyInAnyOrder(1, 2);

                client.messages()
                        .readinessGroups(STREAM, TOPIC, List.of(1L, 2L))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(lookups).hasSize(3).last().isEqualTo(2);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A refusal of the whole fetch does not name the partition that moved. The call fails at once and marks the routes
     * of every target moved, so the next fetch finds the new primary.
     */
    @Test
    void shouldFailAFetchThatItsPrimaryRefusesAndLookUpEveryRouteAgain() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket moved = new ServerSocket(0, 4, loopback);
                ServerSocket primary = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), moved.getLocalPort(), primary.getLocalPort(), leader.getLocalPort());
            CompletableFuture<Void> leaderServed = node(
                    leader,
                    metadata,
                    request -> request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)
                            ? Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, moved.getLocalPort()))
                            : unexpected(request));
            List<Integer> movedLookups = new CopyOnWriteArrayList<>();
            List<Request> refused = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> movedServed = node(moved, metadata, request -> {
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    refused.add(request);
                    return Response.error(OPERATION_NON_REPLICATED, NOT_PRIMARY);
                }
                if (!request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                // The lookup after the refusal asks the last known primary first.
                movedLookups.add(lastInt(request.body()));
                return Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primary.getLocalPort()));
            });
            CompletableFuture<Void> primaryServed = node(
                    primary,
                    metadata,
                    request -> request.is(FETCH_CODE, OPERATION_NON_REPLICATED)
                            ? Response.success(OPERATION_NON_REPLICATED, reply())
                            : unexpected(request));
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                assertServerError(
                        client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, true, FETCH),
                        NOT_PRIMARY);
                assertThat(refused).hasSize(1);

                assertThat(client.messages()
                                .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, true, FETCH)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();
                assertThat(movedLookups).containsExactlyInAnyOrder(1, 2);
                assertThat(refused).hasSize(1);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            CompletableFuture.allOf(leaderServed, movedServed, primaryServed)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * HISTORY_UNAVAILABLE means that the partitions of the fetch were recreated. The call fails, and the next fetch
     * looks up the routes of the topic again and carries their new fences.
     */
    @Test
    void shouldLookUpTheRoutesOfAFetchAgainAfterTheHistoryOfItsPartitionsChanged() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Integer> lookups = new CopyOnWriteArrayList<>();
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = node(socket, () -> singleNodeMetadata(socket.getLocalPort()), request -> {
                if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    lookups.add(lastInt(request.body()));
                    // The partitions have their next incarnation once the server refused the first fetch.
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            pollRoute(request, 1, 1, socket.getLocalPort(), ROUTE_INCARNATION + fetches.size()));
                }
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return fetches.size() == 1
                        ? Response.error(OPERATION_NON_REPLICATED, HISTORY_UNAVAILABLE)
                        : Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertServerError(
                        client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, false, FETCH),
                        HISTORY_UNAVAILABLE);
                client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, false, FETCH)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(lookups).containsExactlyInAnyOrder(1, 2, 1, 2);
                assertThat(fetches).hasSize(2);
                assertThat(targets(fetches.get(0)))
                        .extracting(Target::incarnation)
                        .containsOnly(ROUTE_INCARNATION);
                assertThat(targets(fetches.get(1)))
                        .extracting(Target::incarnation)
                        .containsOnly(ROUTE_INCARNATION + 1);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldRefuseAFetchThatTheServerWouldRefuseBeforeSendingIt() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, AsyncIggyTcpClientFetchMessagesTest::unexpected);
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var messages = client.messages();
                List<Map<Long, PollingStrategy>> refusedTargets = List.of(
                        Map.of(),
                        offsetsOf(LongStream.rangeClosed(0, FetchMessages.MAX_PARTITIONS_PER_REQUEST)
                                .toArray()),
                        Map.of(1L, PollingStrategy.first()),
                        Map.of(1L, PollingStrategy.last()),
                        Map.of(1L, PollingStrategy.timestamp(BigInteger.ONE)),
                        offsetsOf(FetchMessages.MAX_PARTITION_ID + 1),
                        offsetsOf(-1L));
                for (Map<Long, PollingStrategy> targets : refusedTargets) {
                    assertThatThrownBy(() -> messages.fetchMessages(STREAM, TOPIC, targets, GROUP, COUNT, false, FETCH))
                            .isInstanceOf(IggyInvalidArgumentException.class);
                }
                assertThatThrownBy(() -> messages.fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, 0L, false, FETCH))
                        .isInstanceOf(IggyInvalidArgumentException.class);
                assertThatThrownBy(() -> messages.fetchMessages(
                                STREAM, TOPIC, offsetsOf(1L), GROUP, 1L, false, FETCH.withMinCount(2)))
                        .isInstanceOf(IggyInvalidArgumentException.class);
                for (Duration timeout :
                        List.of(DeferredPollOptions.REPLY_MARGIN.minusNanos(1000), DeferredPollOptions.REPLY_MARGIN)) {
                    DeferredPollOptions noStartupBudget =
                            FETCH.withMaxWait(Duration.ZERO).withRequestTimeout(timeout);
                    assertThatThrownBy(() -> messages.fetchMessages(
                                    STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, false, noStartupBudget))
                            .isInstanceOf(IggyInvalidArgumentException.class);
                }
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldSendAZeroWaitFetchWithTimeLeftAfterTheReplyMargin() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertThat(client.messages()
                                .fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, false, IMMEDIATE)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();
                assertThat(fetches).hasSize(1);
                Budgets sent = Budgets.of(fetches.get(0));
                assertThat(sent.waitMicros()).isZero();
                assertThat(sent.timeoutMicros())
                        .isGreaterThanOrEqualTo(
                                TimeUnit.NANOSECONDS.toMicros(DeferredPollOptions.REPLY_MARGIN.toNanos()))
                        .isLessThan(IMMEDIATE.requestTimeoutMicros());
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest(name = "blocked partition={0}")
    @ValueSource(longs = {2, 0xffff_ffffL})
    void shouldNameTheBlockedPartitionAndResumeFetchingAfterRepair(long partition) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> written = new CompletableFuture<>();
            List<Request> stores = new CopyOnWriteArrayList<>();
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.operation() == VsrOperation.STORE_CONSUMER_OFFSET) {
                    stores.add(request);
                    written.complete(request);
                    return stores.size() == 1
                            ? Response.noReply()
                            : Response.committed(
                                    request.operation(), 2, Unpooled.buffer().writeIntLE(0));
                }
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    fetches.add(request);
                    return Response.success(OPERATION_NON_REPLICATED, reply());
                }
                return unexpected(request);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var cancelled = client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(partition), GROUP, BigInteger.ZERO);
                long operation =
                        written.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).requestId();
                cancelled.cancel(false);
                Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
                targets.put(1L, PollingStrategy.next());
                targets.put(partition, PollingStrategy.next());

                assertThatThrownBy(() -> client.messages()
                                .fetchMessages(STREAM, TOPIC, targets, GROUP, COUNT, false, FETCH)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .hasCauseInstanceOf(IggyOperationException.class)
                        .satisfies(error -> {
                            var blocked = (IggyOperationException) error.getCause();
                            assertThat(blocked.getErrorCode()).isEqualTo(IggyErrorCode.PARTITION_LANE_BLOCKED);
                            assertThat(blocked.getOperationId()).isEqualTo(operation);
                            assertThat(blocked.getPartitionId()).hasValue(partition);
                            assertThat(blocked.getMessage()).contains("Partition " + partition);
                        });
                assertThat(fetches).isEmpty();
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(STREAM, TOPIC, Optional.of(partition), GROUP, BigInteger.ONE),
                        IggyErrorCode.LANE_BLOCKED.getCode());

                client.resolveOperation(operation, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(stores).hasSize(2).extracting(Request::requestId).containsOnly(operation);
                assertThat(client.messages()
                                .fetchMessages(STREAM, TOPIC, targets, GROUP, COUNT, false, FETCH)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();
                assertThat(fetches).hasSize(1);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A fetch that commits or starts a partition at its next offset can have moved the partition on the server when
     * its reply is lost, so the client never resends it. A fetch that only reads goes out again until its budget ends.
     */
    @ParameterizedTest
    @CsvSource({"true, Offset, false", "false, Next, false", "false, Offset, true"})
    void shouldResendOnlyAFetchThatCannotHaveMovedItsPartitions(boolean autoCommit, PollingKind kind, boolean resent)
            throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_COMMITTED);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                PollingStrategy strategy =
                        kind == PollingKind.Next ? PollingStrategy.next() : PollingStrategy.offset(BigInteger.ONE);
                var fetch = client.messages()
                        .fetchMessages(STREAM, TOPIC, Map.of(1L, strategy), GROUP, COUNT, autoCommit, IMMEDIATE);

                if (resent) {
                    assertThatThrownBy(() -> fetch.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                            .rootCause()
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    error -> assertThat(error.getRawErrorCode())
                                            .isIn(TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED));
                    assertThat(fetches).hasSizeGreaterThan(1);
                } else {
                    assertServerError(fetch, autoCommit ? 14010 : TRANSIENT_NOT_COMMITTED);
                    assertThat(fetches).hasSize(1);
                    if (autoCommit) {
                        var unknown = fetch.handle(
                                        (sections, error) -> (IggyOperationException) PollRouter.unwrap(error))
                                .join();
                        assertServerError(client.resolveOperation(unknown.getOperationId(), TIMEOUT), 14010);
                        client.abandonOperation(unknown.getOperationId()).join();
                    }
                }
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The server sends these refusals before it reads a partition, so the client sends the fetch again as the same
     * request, also with auto-commit. The new attempt waits only for what is left of the call.
     */
    @ParameterizedTest
    @CsvSource({
        "TRANSIENT_NOT_ACCEPTED, false",
        "TRANSIENT_NOT_ACCEPTED, true",
        "DEFERRED_POLL_CAPACITY_EXCEEDED, false",
        "DEFERRED_POLL_CAPACITY_EXCEEDED, true"
    })
    void shouldSendARefusedFetchAgainWithWhatIsLeftOfItsBudgets(IggyErrorCode refusal, boolean autoCommit)
            throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return fetches.size() == 1
                        ? Response.error(OPERATION_NON_REPLICATED, refusal.getCode())
                        : Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertThat(client.messages()
                                .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, autoCommit, FETCH)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();

                assertThat(fetches).hasSize(2);
                Request first = fetches.get(0);
                Request second = fetches.get(1);
                assertThat(List.of(second.requestId(), second.clientLow(), second.clientHigh()))
                        .containsExactly(first.requestId(), first.clientLow(), first.clientHigh());
                assertThat(targets(second)).isEqualTo(targets(first));
                Budgets sent = Budgets.of(first);
                Budgets resent = Budgets.of(second);
                assertThat(resent.waitMicros()).isLessThan(sent.waitMicros());
                assertThat(resent.timeoutMicros()).isLessThan(sent.timeoutMicros());
                assertThat(List.of(
                                sent.timeoutMicros() - sent.waitMicros(), resent.timeoutMicros() - resent.waitMicros()))
                        .containsOnly(FETCH.requestTimeoutMicros() - FETCH.maxWaitMicros());
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldCancelAParkedFetchThatTheCallerCancels() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> parked = new CompletableFuture<>();
            CompletableFuture<Request> cancelled = new CompletableFuture<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED) && cancelled.complete(request)) {
                    return Response.noReply();
                }
                return request.is(FETCH_CODE, OPERATION_NON_REPLICATED) && parked.complete(request)
                        ? Response.noReply()
                        : unexpected(request);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var fetch = client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, false, FETCH);
                parked.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(fetch.cancel(true)).isTrue();

                assertCancels(cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), parked.join());
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A cancel can drop what a committing fetch committed, so close waits for its reply. Then close cancels the fetch
     * that only reads.
     */
    @Test
    void shouldWaitForACommittingFetchAtCloseAndCancelTheOthers() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket first = new ServerSocket(0, 4, loopback);
                ServerSocket second = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), first.getLocalPort(), second.getLocalPort(), leader.getLocalPort());
            CompletableFuture<Void> leaderServed = node(leader, metadata, routes(first, second));
            CompletableFuture<Request> committing = new CompletableFuture<>();
            CompletableFuture<Void> closing = new CompletableFuture<>();
            List<Request> committingCancels = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> firstServed = node(first, metadata, request -> {
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED)) {
                    committingCancels.add(request);
                    return Response.noReply();
                }
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED) || !committing.complete(request)) {
                    return unexpected(request);
                }
                // The reply comes only after close began.
                after(closing);
                return Response.success(OPERATION_NON_REPLICATED, reply());
            });
            CompletableFuture<Request> reading = new CompletableFuture<>();
            CompletableFuture<Request> readingCancel = new CompletableFuture<>();
            CompletableFuture<Void> secondServed = node(second, metadata, request -> {
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED) && readingCancel.complete(request)) {
                    return Response.noReply();
                }
                return request.is(FETCH_CODE, OPERATION_NON_REPLICATED) && reading.complete(request)
                        ? Response.noReply()
                        : unexpected(request);
            });
            AsyncIggyTcpClient client = signedIn(leader);
            var read = client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(2L), GROUP, COUNT, false, FETCH);
            reading.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            var commit = client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, FETCH);
            committing.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            CompletableFuture<Void> closed = client.close();
            assertThat(closed).isNotDone();
            closing.complete(null);

            closed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            assertThat(commit.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).isEmpty();
            assertCancels(readingCancel.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), reading.join());
            assertThat(read).isCompletedExceptionally();
            assertThat(committingCancels).isEmpty();
            CompletableFuture.allOf(leaderServed, firstServed, secondServed)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The client reads at most the section table and the byte limit of a fetch. A larger reply drops the link, so the
     * outcome of the fetch is unknown and the client does not send it again.
     */
    @Test
    void shouldReadAReplyUpToTheLimitOfTheFetchAndDropALargerOne() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket primary = new ServerSocket(0, 4, loopback);
                ServerSocket idle = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), primary.getLocalPort(), idle.getLocalPort(), leader.getLocalPort());
            // A dropped reply closes only the link of the primary of partition 1.
            CompletableFuture<Void> leaderServed = node(leader, metadata, routes(primary, idle));
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> primaryServed = node(primary, metadata, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                // The second reply holds one byte more than the fetch can get.
                int records = (int) DeferredPollOptions.MIN_MAX_BYTES + fetches.size() - 1;
                ByteBuf reply = Unpooled.buffer().writeIntLE(1);
                section(reply, 1, FetchedMessages.STATUS_OK, records);
                return Response.success(OPERATION_NON_REPLICATED, reply.writeZero(records));
            });
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                DeferredPollOptions smallest = FETCH.withMaxBytes(DeferredPollOptions.MIN_MAX_BYTES);

                assertThat(client.messages()
                                .fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, smallest)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .singleElement()
                        .satisfies(section ->
                                assertThat(section.records()).hasSize((int) DeferredPollOptions.MIN_MAX_BYTES));
                var oversized =
                        client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, smallest);
                assertServerError(oversized, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                var unknown = oversized
                        .handle((sections, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                assertServerError(client.close(), IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                client.abandonOperation(unknown.getOperationId()).join();
                assertThat(fetches).hasSize(2);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            CompletableFuture.allOf(leaderServed, primaryServed).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A link reads replies up to its frame size, so a fetch asks for no more than the link reads. */
    @Test
    void shouldLowerTheByteLimitOfAFetchToWhatTheLinkReads() throws Exception {
        long linkRecords = 2 * DeferredPollOptions.MIN_MAX_BYTES;
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                fetches.add(request);
                return Response.success(OPERATION_NON_REPLICATED, reply());
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("configured-user", "configured-password")
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .maxVsrFrameSize(VsrHeaders.HEADER_SIZE
                            + BytesDeserializer.FETCH_SECTIONS_COUNT_BYTES
                            + BytesDeserializer.FETCH_SECTION_BYTES
                            + linkRecords)
                    .build();
            client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            client.login().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            try {
                client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, false, FETCH)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                ByteBuffer body = ByteBuffer.wrap(fetches.get(0).body()).order(ByteOrder.LITTLE_ENDIAN);
                assertThat(Integer.toUnsignedLong(body.getInt(NUMERIC_LIMITS + FetchMessages.MAX_BYTES_OFFSET)))
                        .isEqualTo(linkRecords);
                assertThat(targets(fetches.get(0)))
                        .singleElement()
                        .extracting(Target::maxBytes)
                        .isEqualTo(linkRecords);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static Map<Long, PollingStrategy> offsetsOf(long... partitions) {
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        for (long partition : partitions) {
            targets.put(partition, PollingStrategy.offset(BigInteger.ZERO));
        }
        return targets;
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 99})
    void shouldRetainEveryTargetWhenTheReplyTableNamesADuplicateOrUnrequestedPartition(int invalid) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, request -> {
                ByteBuf response = Unpooled.buffer().writeIntLE(2);
                section(response, 1, FetchedMessages.STATUS_OK, 0);
                section(response, invalid, FetchedMessages.STATUS_OK, 0);
                return Response.success(OPERATION_NON_REPLICATED, response);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var fetch = client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, true, IMMEDIATE);
                assertServerError(fetch, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                var unknown = fetch.handle((sections, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                try {
                    for (long partition : List.of(1L, 2L)) {
                        assertServerError(
                                client.consumerOffsets()
                                        .storeConsumerOffset(
                                                STREAM, TOPIC, Optional.of(partition), GROUP, BigInteger.ONE),
                                IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                    }
                } finally {
                    client.abandonOperation(unknown.getOperationId()).join();
                }
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {80, 87, 89, 5009})
    void shouldKeepDefinitelyRefusedSectionsFreeForTheNextProgressCall(int status) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    ByteBuf response = Unpooled.buffer().writeIntLE(1);
                    section(response, 1, status, 0);
                    return Response.success(OPERATION_NON_REPLICATED, response);
                }
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var sections = client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, IMMEDIATE)
                        .join();
                assertThat(sections.get(0).status()).isEqualTo(status);
                assertThat(sections.get(0).recoveryError()).isEmpty();
                client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE)
                        .join();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldKeepRecoveryAcrossReconnectAndNeverReuseItsIdentity() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            RequestHandler handler = request -> request.is(FETCH_CODE, OPERATION_NON_REPLICATED)
                    ? Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_COMMITTED)
                    : Response.committed(
                            request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            CompletableFuture<Void> served = singleNode(socket, handler);
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var first =
                        client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, IMMEDIATE);
                assertServerError(first, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                long firstId = first.handle((sections, error) ->
                                ((IggyOperationException) PollRouter.unwrap(error)).getOperationId())
                        .join();
                CompletableFuture<Void> reconnected = singleNode(socket, handler);
                client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.login().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE),
                        IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                assertServerError(
                        client.resolveOperation(firstId, TIMEOUT), IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                client.abandonOperation(firstId).join();
                var next = client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, IMMEDIATE);
                assertServerError(next, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                long nextId = next.handle((sections, error) ->
                                ((IggyOperationException) PollRouter.unwrap(error)).getOperationId())
                        .join();
                assertThat(Long.compareUnsigned(nextId, firstId)).isPositive();
                client.abandonOperation(nextId).join();
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                reconnected.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Test
    void shouldBlockAnotherConnectionWhileAnUnknownCommittingFetchIsStillLive() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket first = new ServerSocket(0, 4, loopback);
                ServerSocket second = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), first.getLocalPort(), second.getLocalPort(), leader.getLocalPort());
            CompletableFuture<Void> leaderServed = node(leader, metadata, routes(first, second));
            CompletableFuture<Request> parked = new CompletableFuture<>();
            CompletableFuture<Void> finishOldRead = new CompletableFuture<>();
            List<Request> fetches = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> firstServed = node(first, metadata, request -> {
                fetches.add(request);
                parked.complete(request);
                after(finishOldRead);
                return Response.noReply();
            });
            List<Request> stores = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> secondServed = node(second, metadata, request -> {
                stores.add(request);
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                var fetch = client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, true, IMMEDIATE);
                parked.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertServerError(fetch, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                var unknown = fetch.handle((sections, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                assertThat(finishOldRead).isNotDone();
                for (long partition : List.of(1L, 2L)) {
                    var store = client.consumerOffsets()
                            .storeConsumerOffset(STREAM, TOPIC, Optional.of(partition), GROUP, BigInteger.ONE);
                    assertThatThrownBy(store::join)
                            .hasCauseInstanceOf(IggyOperationException.class)
                            .satisfies(error -> {
                                var blocked = (IggyOperationException) error.getCause();
                                assertThat(blocked.getErrorCode()).isEqualTo(IggyErrorCode.PARTITION_LANE_BLOCKED);
                                assertThat(blocked.getOperationId()).isEqualTo(unknown.getOperationId());
                                assertThat(blocked.getPartitionId()).hasValue(partition);
                            });
                }
                assertThat(stores).isEmpty();
                assertServerError(
                        client.resolveOperation(unknown.getOperationId(), TIMEOUT),
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                assertThat(fetches).hasSize(1);
                client.abandonOperation(unknown.getOperationId()).join();
                client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(2L), GROUP, BigInteger.ONE)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(stores).hasSize(1);
                assertThat(finishOldRead).isNotDone();
            } finally {
                finishOldRead.complete(null);
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            CompletableFuture.allOf(leaderServed, firstServed, secondServed)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldNameRecoveryAfterTheCallerCancelsASentCommittingFetch() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> parked = new CompletableFuture<>();
            CompletableFuture<Void> releaseReply = new CompletableFuture<>();
            List<Request> requests = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                requests.add(request);
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    parked.complete(request);
                    after(releaseReply);
                    return Response.success(OPERATION_NON_REPLICATED, emptySection(1));
                }
                if (request.is(CommandCode.System.GET_STATS.getValue(), OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var fetch = client.messages().fetchMessages(STREAM, TOPIC, offsetsOf(1L), GROUP, COUNT, true, FETCH);
                parked.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(fetch.cancel(true)).isTrue();
                var store = client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE);
                assertServerError(store, IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                var blocked = store.handle((ignored, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                assertThat(requests).hasSize(1);
                assertServerError(
                        client.resolveOperation(blocked.getOperationId(), TIMEOUT),
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                releaseReply.complete(null);
                client.sendBinaryRequest(CommandCode.System.GET_STATS.getValue(), new byte[0])
                        .join();
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE),
                        IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                assertServerError(
                        client.resolveOperation(blocked.getOperationId(), TIMEOUT),
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                client.abandonOperation(blocked.getOperationId()).join();
                client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(requests).hasSize(3);
            } finally {
                releaseReply.complete(null);
                if (parked.isDone()) {
                    client.abandonOperation(parked.join().requestId())
                            .exceptionally(error -> null)
                            .join();
                }
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 57, 58, 59})
    void shouldKeepHealthySectionsWhileRetainingOnlyUncertainTargets(int status) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> stores = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    ByteBuf response = Unpooled.buffer().writeIntLE(2);
                    section(response, 1, FetchedMessages.STATUS_OK, 1);
                    response.setIntLE(Integer.BYTES * 3 + Long.BYTES, 1);
                    section(response, 2, status, 0);
                    return Response.success(OPERATION_NON_REPLICATED, response.writeByte(42));
                }
                stores.add(request);
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var sections = client.messages()
                        .fetchMessages(STREAM, TOPIC, offsetsOf(1L, 2L), GROUP, COUNT, true, IMMEDIATE)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(sections).hasSize(2);
                assertThat(sections.get(0).records()).containsExactly((byte) 42);
                assertThat(sections.get(0).recoveryError()).isEmpty();
                assertThatThrownBy(sections.get(0)::decode).isNotInstanceOf(IggyOperationException.class);
                var unknown = sections.get(1).recoveryError().orElseThrow();
                assertThat(unknown.getErrorCode()).isEqualTo(IggyErrorCode.FETCH_PROGRESS_UNCERTAIN);
                assertThat(sections.get(1).status()).isEqualTo(status);
                assertThatThrownBy(sections.get(1)::decode).isInstanceOf(IggyOperationException.class);
                client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE)
                        .join();
                assertServerError(
                        client.consumerOffsets()
                                .storeConsumerOffset(STREAM, TOPIC, Optional.of(2L), GROUP, BigInteger.ONE),
                        IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                assertThat(stores).hasSize(1);
                client.abandonOperation(unknown.getOperationId()).join();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldPreserveRawSectionsAndNameTheirRecoveryOnTheNextProgressCall(boolean resolveZero) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            ByteBuf encoded = Unpooled.buffer().writeIntLE(2);
            section(encoded, 1, FetchedMessages.STATUS_OK, 1);
            section(encoded, 2, TRANSIENT_NOT_COMMITTED, 0);
            encoded.writeByte(42);
            byte[] wireReply = new byte[encoded.readableBytes()];
            encoded.readBytes(wireReply);
            encoded.release();
            CompletableFuture<Void> served = singleNode(
                    socket,
                    request -> request.is(FETCH_CODE, OPERATION_NON_REPLICATED)
                            ? Response.success(OPERATION_NON_REPLICATED, Unpooled.wrappedBuffer(wireReply))
                            : unexpected(request));
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                byte[] request = HEX.parseHex(GOLDEN_REQUEST);
                if (resolveZero) {
                    ByteBuffer fields = ByteBuffer.wrap(request).order(ByteOrder.LITTLE_ENDIAN);
                    for (int index = 0; index < 2; index++) {
                        fields.putLong(target(GOLDEN_LIMITS, index) + FetchMessages.INCARNATION_OFFSET, 0);
                    }
                }
                assertThat(client.sendBinaryRequest(FETCH_CODE, request).join()).isEqualTo(wireReply);
                var store = resolveZero
                        ? client.consumerOffsets()
                                .storeConsumerOffset(STREAM, orders(), Optional.of(2L), GROUP, BigInteger.ONE)
                        : client.consumerOffsets()
                                .storeConsumerOffset(
                                        STREAM,
                                        orders(),
                                        2L,
                                        GROUP,
                                        BigInteger.ONE,
                                        new PartitionContext(
                                                BigInteger.valueOf(3), BigInteger.valueOf(9), BigInteger.ZERO));
                assertServerError(store, IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                var blocked = store.handle((ignored, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                assertThat(blocked.getPartitionId()).hasValue(2);
                assertServerError(
                        client.resolveOperation(blocked.getOperationId(), TIMEOUT),
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                client.abandonOperation(blocked.getOperationId()).join();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldResolveZeroIncarnationsBeforeRetainingTypedFetchTargets() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> stores = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.error(OPERATION_NON_REPLICATED, TRANSIENT_NOT_COMMITTED);
                }
                stores.add(request);
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var fetch = client.messages()
                        .fetchMessages(
                                STREAM,
                                TOPIC,
                                Map.of(1L, PollingStrategy.next().withContext(PartitionContext.EMPTY)),
                                GROUP,
                                COUNT,
                                true,
                                IMMEDIATE);
                assertServerError(fetch, IggyErrorCode.FETCH_PROGRESS_UNCERTAIN.getCode());
                var unknown = fetch.handle((sections, error) -> (IggyOperationException) PollRouter.unwrap(error))
                        .join();
                try {
                    assertServerError(
                            client.consumerOffsets()
                                    .storeConsumerOffset(STREAM, TOPIC, Optional.of(1L), GROUP, BigInteger.ONE),
                            IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                    assertServerError(
                            client.consumerOffsets()
                                    .storeConsumerOffset(
                                            STREAM, TOPIC, 1L, GROUP, BigInteger.ONE, PartitionContext.EMPTY),
                            IggyErrorCode.PARTITION_LANE_BLOCKED.getCode());
                    assertThat(stores).isEmpty();
                } finally {
                    client.abandonOperation(unknown.getOperationId()).join();
                }
                client.consumerOffsets()
                        .storeConsumerOffset(STREAM, TOPIC, 1L, GROUP, BigInteger.ONE, PartitionContext.EMPTY)
                        .join();
                assertThat(stores)
                        .singleElement()
                        .satisfies(store -> assertThat(store.incarnation()).isZero());
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A reply without sections, as a fetch whose wait found nothing gets. */
    private static ByteBuf reply() {
        return Unpooled.buffer().writeIntLE(0);
    }

    /** A reply with one section of {@code partition}, which holds no records. */
    private static ByteBuf emptySection(int partition) {
        ByteBuf reply = Unpooled.buffer().writeIntLE(1);
        section(reply, partition, FetchedMessages.STATUS_OK, 0);
        return reply;
    }

    /** Appends one section table entry, all of whose other fields are zero. */
    private static void section(ByteBuf reply, int partition, long status, int recordsLength) {
        reply.writeIntLE(partition)
                .writeIntLE((int) status)
                .writeLongLE(0)
                .writeIntLE(0)
                .writeZero(PartitionContext.ENCODED_SIZE)
                .writeLongLE(0)
                .writeIntLE(recordsLength);
    }

    private static int target(int limits, int index) {
        return limits + FetchMessages.LIMITS_BYTES + index * FetchMessages.TARGET_BYTES;
    }

    private static List<Target> targets(Request fetch) {
        ByteBuffer body = ByteBuffer.wrap(fetch.body()).order(ByteOrder.LITTLE_ENDIAN);
        int count = body.getInt(NUMERIC_LIMITS + FetchMessages.TARGETS_COUNT_OFFSET);
        List<Target> targets = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            int target = target(NUMERIC_LIMITS, index);
            targets.add(new Target(
                    Integer.toUnsignedLong(body.getInt(target)),
                    Byte.toUnsignedInt(body.get(target + FetchMessages.STRATEGY_KIND_OFFSET)),
                    body.getLong(target + FetchMessages.STRATEGY_KIND_OFFSET + Byte.BYTES),
                    Integer.toUnsignedLong(body.getInt(target + FetchMessages.TARGET_MAX_BYTES_OFFSET)),
                    body.getLong(target + FetchMessages.INCARNATION_OFFSET),
                    body.getLong(target + FetchMessages.INCARNATION_OFFSET + Long.BYTES)));
        }
        return targets;
    }

    private record Target(
            long partition, int kind, long value, long maxBytes, long incarnation, long ownerGeneration) {}

    private record Budgets(long waitMicros, long timeoutMicros) {
        static Budgets of(Request fetch) {
            ByteBuffer body = ByteBuffer.wrap(fetch.body()).order(ByteOrder.LITTLE_ENDIAN);
            return new Budgets(body.getLong(NUMERIC_LIMITS), body.getLong(NUMERIC_LIMITS + Long.BYTES));
        }
    }

    @Test
    void shouldSendTheGoldenDeferredPollBodyWithBudgetsCountedDownFromTheCall() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> sent = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                sent.add(request);
                return Response.success(OPERATION_NON_REPLICATED, emptyPoll(3));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                client.messages()
                        .pollMessagesDeferred(
                                STREAM,
                                orders(),
                                Optional.of(3L),
                                GROUP,
                                PollingStrategy.next(),
                                100L,
                                true,
                                DeferredPollOptions.defaults())
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                assertThat(sent).extracting(Request::commandCode).containsExactly(POLL_DEFERRED_CODE);
                int limits = HEX.parseHex(DEFERRED_POLL_BODY).length - DEFERRED_LIMITS_BYTES;
                assertGoldenBody(
                        sent.get(0).body(), DEFERRED_POLL_BODY, limits, limits + DEFERRED_LIMITS_BYTES - Long.BYTES);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldGroupThePartitionsOfEachPrimaryInTheOrderOfTheCall() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket first = new ServerSocket(0, 4, loopback);
                ServerSocket second = new ServerSocket(0, 4, loopback)) {
            Supplier<ByteBuf> metadata = () -> threeNodeMetadata(
                    leader.getLocalPort(), first.getLocalPort(), second.getLocalPort(), leader.getLocalPort());
            // The leader fails on any request but a route lookup, and nothing serves the primaries.
            CompletableFuture<Void> served = node(leader, metadata, routes(first, second));
            AsyncIggyTcpClient client = signedIn(leader);
            try {
                for (long partition : new long[] {FetchMessages.MAX_PARTITION_ID + 1, -1}) {
                    assertThatThrownBy(() -> client.messages().readinessGroups(STREAM, TOPIC, List.of(partition)))
                            .isInstanceOf(IggyInvalidArgumentException.class);
                }
                assertThat(client.messages()
                                .readinessGroups(STREAM, TOPIC, List.of(2L, 1L, 3L))
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .containsExactly(List.of(2L, 3L), List.of(1L));
                assertThat(client.messages()
                                .readinessGroups(STREAM, TOPIC, List.of())
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEmpty();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void shouldSplitThePartitionsOfOnePrimaryIntoFetchSizedGroups() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, AsyncIggyTcpClientFetchMessagesTest::unexpected);
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(socket.getInetAddress().getHostAddress())
                    .port(socket.getLocalPort())
                    .credentials("configured-user", "configured-password")
                    .requestTimeout(FETCH.requestTimeout())
                    .heartbeatInterval(Duration.ofMinutes(1))
                    .build();
            client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            client.login().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            try {
                int limit = FetchMessages.MAX_PARTITIONS_PER_REQUEST;
                List<Long> partitions = LongStream.rangeClosed(0, limit).boxed().toList();
                assertThat(client.messages()
                                .readinessGroups(STREAM, TOPIC, partitions)
                                .get(FETCH.requestTimeout().toMillis(), TimeUnit.MILLISECONDS))
                        .containsExactly(partitions.subList(0, limit), partitions.subList(limit, partitions.size()));
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** A leader whose route lookups name {@code first} the primary of partition 1 and {@code second} of the others. */
    static RequestHandler routes(ServerSocket first, ServerSocket second) {
        return request -> request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)
                ? Response.success(
                        OPERATION_NON_REPLICATED,
                        pollRoute(
                                request,
                                1,
                                1,
                                lastInt(request.body()) == 1 ? first.getLocalPort() : second.getLocalPort()))
                : unexpected(request);
    }

    /** Holds a reply of a mock node until {@code earlier} ended and {@link #LATE_REPLY} passed, so it comes second. */
    static void after(CompletableFuture<Void> earlier) {
        earlier.thenRunAsync(() -> {}, CompletableFuture.delayedExecutor(LATE_REPLY.toMillis(), TimeUnit.MILLISECONDS))
                .orTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .join();
    }

    /** A node of one replica that is the primary of every partition, so data requests share the sign-in link. */
    static CompletableFuture<Void> singleNode(ServerSocket socket, RequestHandler data) {
        return node(
                socket,
                () -> singleNodeMetadata(socket.getLocalPort()),
                request -> request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)
                        ? Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, socket.getLocalPort()))
                        : data.handle(request));
    }

    /** A node that signs in, binds data links and names the cluster, and gives every other request to {@code data}. */
    static CompletableFuture<Void> node(ServerSocket socket, Supplier<ByteBuf> metadata, RequestHandler data) {
        return serve(socket, request -> {
            if (request.operation() == OPERATION_REGISTER) {
                return Response.success(OPERATION_REGISTER, registerBody(1));
            }
            if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, metadata.get());
            }
            if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
            }
            return data.handle(request);
        });
    }

    static AsyncIggyTcpClient signedIn(ServerSocket socket) throws Exception {
        AsyncIggyTcpClient client = client(socket);
        client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        client.login().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return client;
    }

    static Response unexpected(Request request) {
        throw new IllegalStateException("Unexpected request: " + request);
    }

    static void assertGoldenBody(byte[] sent, String golden, int waitOffset, int timeoutOffset) {
        byte[] expected = HEX.parseHex(golden);
        ByteBuffer sentFields = ByteBuffer.wrap(sent).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer goldenFields = ByteBuffer.wrap(expected).order(ByteOrder.LITTLE_ENDIAN);
        for (int budget : new int[] {waitOffset, timeoutOffset}) {
            // Each attempt carries what is left of both budgets of the call.
            assertThat(sentFields.getLong(budget))
                    .isLessThanOrEqualTo(goldenFields.getLong(budget))
                    .isGreaterThan(
                            goldenFields.getLong(budget) - TimeUnit.NANOSECONDS.toMicros(ROUTING_ALLOWANCE.toNanos()));
        }
        byte[] masked = sent.clone();
        System.arraycopy(expected, waitOffset, masked, waitOffset, Long.BYTES);
        System.arraycopy(expected, timeoutOffset, masked, timeoutOffset, Long.BYTES);
        assertThat(HEX.formatHex(masked)).isEqualTo(golden);
    }

    /** The cancel of a parked request is a header with no body that names the same client and request. */
    static void assertCancels(Request cancel, Request parked) {
        assertThat(cancel.body()).isEmpty();
        assertThat(List.of(cancel.requestId(), cancel.clientLow(), cancel.clientHigh()))
                .containsExactly(parked.requestId(), parked.clientLow(), parked.clientHigh());
    }

    static TopicId orders() {
        return TopicId.of("orders");
    }

    static int lastInt(byte[] body) {
        return ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN).getInt(body.length - Integer.BYTES);
    }
}
