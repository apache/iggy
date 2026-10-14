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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.TRANSIENT_NOT_ACCEPTED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.clusterMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.apache.iggy.serde.BytesSerializer.encodeMessagesBatchInto;
import static org.apache.iggy.serde.BytesSerializer.toBytes;
import static org.apache.iggy.serde.BytesSerializer.toBytesAsU64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientRawRequestTest {
    private static final int SEND_CODE = 101;
    private static final int GET_PARTITION_ROUTE_CODE = 105;
    private static final int STORE_OFFSET_CODE = 121;
    private static final int DELETE_OFFSET_CODE = 122;
    private static final int GET_OFFSET_ROUTING_CODE = 105;
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int OPERATION_STORE_CONSUMER_OFFSET = 161;
    private static final int OPERATION_DELETE_CONSUMER_OFFSET = 162;
    private static final int INVALID_COMMAND = 3;
    private static final int FEATURE_UNAVAILABLE = 5;
    private static final int HISTORY_UNAVAILABLE = 87;
    private static final int GET_OFFSET_CODE = 120;
    private static final int OPTIONAL_PARTITION_BYTES = 5;
    private static final long INCARNATION = 5;
    private static final long OWNER_GENERATION = 0;
    private static final long METADATA_OP = 7;
    private static final StreamId STREAM = StreamId.of(1L);
    private static final TopicId TOPIC = TopicId.of(2L);
    // Replicated commands travel under their operation and carry no command code.
    private static final Map<Integer, Integer> REPLICATED_CODES = Map.of(
            OPERATION_SEND_MESSAGES, SEND_CODE,
            OPERATION_STORE_CONSUMER_OFFSET, STORE_OFFSET_CODE,
            OPERATION_DELETE_CONSUMER_OFFSET, DELETE_OFFSET_CODE);

    @Test
    void shouldStampARawSendToAnExplicitPartitionWithACachedSendContext() throws Exception {
        AtomicInteger sendAttempts = new AtomicInteger();
        assertRawRequests(
                request -> {
                    if (code(request) == SEND_CODE && sendAttempts.getAndIncrement() == 0) {
                        return Response.error(OPERATION_SEND_MESSAGES, TRANSIENT_NOT_ACCEPTED);
                    }
                    return null;
                },
                (client, requests) -> {
                    client.sendBinaryRequest(SEND_CODE, rawSend(Partitioning.partitionId(3L)))
                            .get(5, TimeUnit.SECONDS);
                    client.sendBinaryRequest(SEND_CODE, rawSend(Partitioning.partitionId(3L)))
                            .get(5, TimeUnit.SECONDS);

                    assertThat(requests)
                            .extracting(AsyncIggyTcpClientRawRequestTest::code)
                            .containsExactly(
                                    GET_PARTITION_ROUTE_CODE,
                                    SEND_CODE,
                                    GET_PARTITION_ROUTE_CODE,
                                    SEND_CODE,
                                    SEND_CODE);
                    assertThat(requests.get(0).body())
                            .isEqualTo(bytes(
                                    toBytes(STREAM),
                                    toBytes(TOPIC),
                                    Unpooled.buffer().writeIntLE(3)));
                    assertCapturedContext(requests.stream()
                            .filter(request -> code(request) == SEND_CODE)
                            .toList());
                });
    }

    @Test
    void shouldKeepTheRequestIdAndContextWhenARawSendIsRefusedByThePrimary() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket coordinatorSocket = new ServerSocket(0, 4, loopback);
                ServerSocket primarySocket = new ServerSocket(0, 4, loopback)) {
            List<Request> sends = new CopyOnWriteArrayList<>();
            List<Request> controls = new CopyOnWriteArrayList<>();
            AtomicInteger routes = new AtomicInteger();
            CompletableFuture<Void> coordinator = serve(coordinatorSocket, request -> {
                controls.add(request);
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
                if (code(request) == GET_PARTITION_ROUTE_CODE) {
                    routes.incrementAndGet();
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            Unpooled.buffer()
                                    .writeLongLE(INCARNATION + routes.get() - 1)
                                    .writeLongLE(METADATA_OP)
                                    .writeIntLE(routes.get())
                                    .writeIntLE(1)
                                    .writeByte(1));
                }
                throw new IllegalStateException("Unexpected coordinator request: " + request);
            });
            CompletableFuture<Void> primary = serve(primarySocket, 2, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    controls.add(request);
                    return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
                }
                if (code(request) == SEND_CODE) {
                    sends.add(request);
                    return sends.size() == 1
                            ? Response.error(OPERATION_SEND_MESSAGES, AsyncTcpConnection.NOT_PRIMARY)
                            : respond(request);
                }
                throw new IllegalStateException("Unexpected primary request: " + request);
            });
            AsyncIggyTcpClient client = client(coordinatorSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                client.sendBinaryRequest(SEND_CODE, rawSend(Partitioning.partitionId(3L)))
                        .get(5, TimeUnit.SECONDS);
                assertThat(client.getConnectionInfo().port()).isEqualTo(coordinatorSocket.getLocalPort());
                assertThat(routes).hasValue(2);
                assertThat(sends).hasSize(2);
                assertThat(sends)
                        .extracting(Request::requestId)
                        .containsOnly(sends.get(0).requestId());
                assertCapturedContext(sends);
                assertThat(controls.stream()
                                .filter(request -> request.requestId() != 0)
                                .map(Request::requestId)
                                .toList())
                        .doesNotHaveDuplicates()
                        .doesNotContain(sends.get(0).requestId());
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            coordinator.get(5, TimeUnit.SECONDS);
            primary.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldRouteRawPollsLikeTypedPollsAndRouteAgainAfterARefusal() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        assertRawRequests(
                request -> request.is(POLL_CODE, OPERATION_NON_REPLICATED) && polls.incrementAndGet() == 2
                        ? Response.error(OPERATION_NON_REPLICATED, HISTORY_UNAVAILABLE)
                        : null,
                (client, requests) -> {
                    client.sendBinaryRequest(POLL_CODE, rawPoll()).get(5, TimeUnit.SECONDS);
                    assertServerError(client.sendBinaryRequest(POLL_CODE, rawPoll()), HISTORY_UNAVAILABLE);
                    client.sendBinaryRequest(POLL_CODE, rawPoll()).get(5, TimeUnit.SECONDS);

                    assertThat(requests)
                            .extracting(AsyncIggyTcpClientRawRequestTest::code)
                            .containsExactly(
                                    GET_PARTITION_ROUTE_CODE,
                                    POLL_CODE,
                                    POLL_CODE,
                                    GET_PARTITION_ROUTE_CODE,
                                    POLL_CODE);
                    assertThat(requests.get(0).body())
                            .isEqualTo(bytes(
                                    toBytes(STREAM),
                                    toBytes(TOPIC),
                                    Unpooled.buffer().writeIntLE(0)));
                    assertCapturedContext(List.of(requests.get(1), requests.get(2), requests.get(4)));
                });
    }

    @Test
    void shouldRouteRawOffsetWritesLikeTypedOffsetWrites() throws Exception {
        assertRawRequests(request -> null, (client, requests) -> {
            client.sendBinaryRequest(STORE_OFFSET_CODE, rawStoreOffset()).get(5, TimeUnit.SECONDS);
            client.sendBinaryRequest(DELETE_OFFSET_CODE, rawDeleteOffset()).get(5, TimeUnit.SECONDS);

            assertThat(requests)
                    .extracting(AsyncIggyTcpClientRawRequestTest::code)
                    .containsExactly(GET_OFFSET_ROUTING_CODE, STORE_OFFSET_CODE, DELETE_OFFSET_CODE);
            assertThat(requests.get(0).body())
                    .isEqualTo(bytes(
                            toBytes(STREAM), toBytes(TOPIC), Unpooled.buffer().writeIntLE(0)));
            assertCapturedContext(List.of(requests.get(1), requests.get(2)));
        });
    }

    @Test
    void shouldShareOneRouteAcrossEveryPartitionCommand() throws Exception {
        assertRawRequests(request -> null, (client, requests) -> {
            client.sendBinaryRequest(SEND_CODE, rawSend(Partitioning.partitionId(0L)))
                    .get(5, TimeUnit.SECONDS);
            client.sendBinaryRequest(POLL_CODE, rawPoll()).get(5, TimeUnit.SECONDS);
            client.sendBinaryRequest(GET_OFFSET_CODE, bytes(offsetTarget())).get(5, TimeUnit.SECONDS);
            client.sendBinaryRequest(STORE_OFFSET_CODE, rawStoreOffset()).get(5, TimeUnit.SECONDS);
            client.sendBinaryRequest(DELETE_OFFSET_CODE, rawDeleteOffset()).get(5, TimeUnit.SECONDS);
            assertThat(requests)
                    .extracting(AsyncIggyTcpClientRawRequestTest::code)
                    .containsExactly(
                            GET_PARTITION_ROUTE_CODE,
                            SEND_CODE,
                            POLL_CODE,
                            GET_OFFSET_CODE,
                            STORE_OFFSET_CODE,
                            DELETE_OFFSET_CODE);
            assertCapturedContext(requests.subList(1, requests.size()));
        });
    }

    @Test
    void shouldRejectMissingMutationAndGroupPartitionsLocally() throws Exception {
        assertRawRequests(request -> null, (client, requests) -> {
            byte[] groupPoll = rawPoll();
            groupPoll[0] = 2;
            groupPoll[bytes(offsetTarget()).length - OPTIONAL_PARTITION_BYTES] = 0;
            byte[] store = rawStoreOffset();
            store[bytes(offsetTarget()).length - OPTIONAL_PARTITION_BYTES] = 0;
            byte[] delete = rawDeleteOffset();
            delete[bytes(offsetTarget()).length - OPTIONAL_PARTITION_BYTES] = 0;
            assertMalformed(client.sendBinaryRequest(POLL_CODE, groupPoll));
            assertMalformed(client.sendBinaryRequest(STORE_OFFSET_CODE, store));
            assertMalformed(client.sendBinaryRequest(DELETE_OFFSET_CODE, delete));
            assertThat(requests).isEmpty();
        });
    }

    @Test
    void shouldRefuseRawBalancedAndMessagesKeySendsWithoutSendingThem() throws Exception {
        assertRawRequests(request -> null, (client, requests) -> {
            for (Partitioning partitioning : List.of(Partitioning.balanced(), Partitioning.messagesKey("key"))) {
                CompletableFuture<byte[]> sent = client.sendBinaryRequest(SEND_CODE, rawSend(partitioning));
                assertServerError(sent, FEATURE_UNAVAILABLE);
                assertThatThrownBy(sent::join).rootCause().hasMessageContaining("explicit partition id");
            }
            assertThat(requests).isEmpty();
        });
    }

    @Test
    void shouldRejectMalformedRawPartitionPayloadsWithoutSendingThem() throws Exception {
        byte[] send = rawSend(Partitioning.partitionId(3L));
        byte[] poll = rawPoll();
        byte[] store = rawStoreOffset();
        byte[] delete = rawDeleteOffset();
        assertRawRequests(request -> null, (client, requests) -> {
            assertMalformed(client.sendBinaryRequest(SEND_CODE, Arrays.copyOf(send, 3)));
            // A numeric stream id claiming five value bytes.
            byte[] badIdentifier = send.clone();
            badIdentifier[Integer.BYTES + 1] = 5;
            assertMalformed(client.sendBinaryRequest(SEND_CODE, badIdentifier));
            assertMalformed(client.sendBinaryRequest(POLL_CODE, Arrays.copyOf(poll, poll.length - 1)));
            assertMalformed(client.sendBinaryRequest(STORE_OFFSET_CODE, bytes(offsetTarget())));
            assertMalformed(client.sendBinaryRequest(DELETE_OFFSET_CODE, Arrays.copyOf(delete, delete.length + 1)));
            assertMalformed(client.sendBinaryRequest(STORE_OFFSET_CODE, Arrays.copyOf(store, 2)));
            assertThat(requests).isEmpty();
        });
    }

    private static void assertRawRequests(RawResponder override, RawScenario scenario) throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            List<Request> requests = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(serverSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                requests.add(request);
                Response response = override.respond(request);
                return response != null ? response : respond(request);
            });
            AsyncIggyTcpClient client = client(serverSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                scenario.run(client, requests);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    private static Response respond(Request request) {
        return switch (code(request)) {
            case GET_PARTITION_ROUTE_CODE ->
                Response.success(
                        OPERATION_NON_REPLICATED,
                        Unpooled.buffer()
                                .writeLongLE(INCARNATION)
                                .writeLongLE(METADATA_OP)
                                .writeIntLE(0)
                                .writeIntLE(1)
                                .writeByte(0));
            case GET_OFFSET_CODE -> Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
            case SEND_CODE -> Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
            case POLL_CODE -> Response.success(OPERATION_NON_REPLICATED, emptyPoll(0));
            case STORE_OFFSET_CODE, DELETE_OFFSET_CODE ->
                Response.success(request.operation(), Unpooled.buffer().writeIntLE(0));
            default -> Response.error(request.operation(), INVALID_COMMAND);
        };
    }

    private static int code(Request request) {
        return REPLICATED_CODES.getOrDefault(request.operation(), request.commandCode());
    }

    private static ByteBuf context(ByteBuf body) {
        return body.writeLongLE(INCARNATION).writeLongLE(OWNER_GENERATION).writeLongLE(METADATA_OP);
    }

    private static void assertCapturedContext(List<Request> requests) {
        assertThat(requests)
                .isNotEmpty()
                .allSatisfy(request -> assertThat(
                                List.of(request.incarnation(), request.ownerGeneration(), request.metadataOp()))
                        .containsExactly(INCARNATION, OWNER_GENERATION, METADATA_OP));
    }

    private static void assertMalformed(CompletableFuture<byte[]> sent) {
        assertThatThrownBy(() -> sent.get(5, TimeUnit.SECONDS))
                .hasRootCauseInstanceOf(IggyInvalidArgumentException.class);
    }

    private static byte[] rawSend(Partitioning partitioning) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeIntLE(STREAM.getSize() + TOPIC.getSize() + partitioning.getSize() + Integer.BYTES);
        payload.writeBytes(toBytes(STREAM));
        payload.writeBytes(toBytes(TOPIC));
        payload.writeBytes(toBytes(partitioning));
        payload.writeIntLE(1);
        encodeMessagesBatchInto(payload, List.of(Message.of("m")));
        return bytes(payload);
    }

    private static byte[] rawPoll() {
        ByteBuf payload = offsetTarget();
        payload.writeBytes(toBytes(PollingStrategy.next()));
        payload.writeIntLE(1);
        payload.writeByte(0);
        return bytes(payload);
    }

    static byte[] rawStoreOffset() {
        return bytes(
                offsetTarget(), toBytesAsU64(BigInteger.TEN), Unpooled.buffer().writeByte(1));
    }

    static byte[] rawDeleteOffset() {
        return bytes(offsetTarget(), Unpooled.buffer().writeByte(1));
    }

    static ByteBuf offsetTarget() {
        ByteBuf target = toBytes(Consumer.of(7L));
        target.writeBytes(toBytes(STREAM));
        target.writeBytes(toBytes(TOPIC));
        target.writeBytes(toBytes(Optional.of(0L)));
        return target;
    }

    private static byte[] bytes(ByteBuf... parts) {
        ByteBuf payload = Unpooled.wrappedBuffer(parts);
        try {
            return ByteBufUtil.getBytes(payload);
        } finally {
            payload.release();
        }
    }

    @FunctionalInterface
    private interface RawResponder {
        Response respond(Request request);
    }

    @FunctionalInterface
    private interface RawScenario {
        void run(AsyncIggyTcpClient client, List<Request> requests) throws Exception;
    }
}
