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
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.SINGLE_NODE_INCARNATION;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.assertCancels;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.emptySection;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.signedIn;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.singleNode;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.unexpected;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A partition reader on the TCP client against a mock node. */
class AsyncIggyTcpClientReaderTest {
    private static final int FETCH_CODE = CommandCode.Messages.FETCH_MESSAGES.getValue();
    private static final int CANCEL_CODE = CommandCode.Messages.CANCEL_DEFERRED_REQUEST.getValue();
    private static final int GET_OFFSET_CODE = CommandCode.ConsumerOffset.GET.getValue();
    private static final int POLL_DEFERRED_CODE = CommandCode.Messages.POLL_DEFERRED.getValue();
    private static final int SYNC_CODE = CommandCode.ConsumerGroup.SYNC.getValue();
    // The metadata commit of the replica that answers the first sync, above the commit of the sign-in.
    private static final long ASSIGNED_AT = 7;
    private static final long CURRENT_OFFSET = 9;
    // The owner that the server installed for the member, as its poll reply names it.
    private static final PartitionContext INSTALLED = new PartitionContext(
            BigInteger.valueOf(SINGLE_NODE_INCARNATION), BigInteger.valueOf(5), BigInteger.valueOf(6));
    private static final long STORED_OFFSET = 3;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /**
     * The close of a reader under OnRead cancels its parked committing fetch on the server and ends with the one reply
     * to that cancel, instead of waiting out the wait of the fetch.
     */
    @ParameterizedTest(name = "server answers the cancel with status {0}")
    @ValueSource(ints = {0, 58})
    void shouldCloseAnOnReadReaderWithTheReplyToTheCancelOfItsParkedFetch(int status) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> parked = new CompletableFuture<>();
            CompletableFuture<Request> cancelled = new CompletableFuture<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(GET_OFFSET_CODE, OPERATION_NON_REPLICATED)) {
                    return noStoredOffset();
                }
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED) && parked.complete(request)) {
                    return Response.noReply();
                }
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED) && cancelled.complete(request)) {
                    return status == 0
                            ? Response.success(OPERATION_NON_REPLICATED, emptySection(1))
                            : Response.error(OPERATION_NON_REPLICATED, status);
                }
                return unexpected(request);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var reader = client.reader(StreamId.of(1L), TopicId.of(1L), Consumer.of(7L))
                        .partitions(List.of(1L))
                        .start(PollingStrategy.offset(BigInteger.ZERO))
                        .options(PollOptions.waitFor(Duration.ofSeconds(30)).withCommit(CommitPolicy.onRead()))
                        .build()
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                reader.next();
                parked.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertCancels(cancelled.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), parked.join());
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A parked committing fetch whose cancel loses the link ends with an unknown outcome. It blocks its partition on
     * the client, so the close of its reader abandons it. The rewind of the close then looks the offset up on a new
     * link. A later store of the partition on the same client goes through.
     */
    @Test
    void shouldAbandonAParkedFetchWhoseCancelLosesTheLinkWhenTheReaderCloses() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> parked = new CompletableFuture<>();
            RequestHandler handler = request -> {
                if (request.is(GET_OFFSET_CODE, OPERATION_NON_REPLICATED)) {
                    return noStoredOffset();
                }
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED) && parked.complete(request)) {
                    return Response.noReply();
                }
                if (request.is(CANCEL_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.disconnect();
                }
                return Response.committed(
                        request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            };
            CompletableFuture<Void> served = singleNode(socket, handler);
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var reader = client.reader(StreamId.of(1L), TopicId.of(1L), Consumer.of(7L))
                        .partitions(List.of(1L))
                        .start(PollingStrategy.offset(BigInteger.ZERO))
                        .options(PollOptions.waitFor(Duration.ofSeconds(30)).withCommit(CommitPolicy.onRead()))
                        .build()
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                reader.next();
                parked.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                CompletableFuture<Void> reconnected = singleNode(socket, handler);

                reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.consumerOffsets()
                        .storeConsumerOffset(
                                StreamId.of(1L), TopicId.of(1L), Optional.of(1L), Consumer.of(7L), BigInteger.ONE)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                reconnected.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * The transport keeps each committing fetch whose outcome is not known, so the close can abandon it. A fetch that
     * the client refuses before the send, or that the server refuses, has a known outcome, so the transport drops it.
     */
    @Test
    void shouldDropACommittingFetchThatTheClientOrTheServerRefused() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(
                    socket,
                    request -> request.is(FETCH_CODE, OPERATION_NON_REPLICATED)
                            ? Response.error(OPERATION_NON_REPLICATED, IggyErrorCode.INVALID_COMMAND.getCode())
                            : unexpected(request));
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var transport = new TcpReaderTransport(client, StreamId.of(1L), TopicId.of(1L), Consumer.of(7L));
                Set<?> committing = committing(transport);
                DeferredPollOptions options = PollOptions.waitFor(Duration.ZERO).deferredOptions();

                assertThatThrownBy(() ->
                                transport.read(Map.of(1L, PollingStrategy.timestamp(BigInteger.ONE)), 1, true, options))
                        .isInstanceOf(IggyInvalidArgumentException.class);
                assertThatThrownBy(() -> transport
                                .read(Map.of(1L, PollingStrategy.offset(BigInteger.ZERO)), 1, true, options)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .hasCauseInstanceOf(IggyServerException.class);

                AsyncIggyTcpClientEventLoopGroupTest.await(
                        committing::isEmpty, "The transport drops the refused fetches", TIMEOUT);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The server refuses an offset lookup or write that names incarnation 0 with HistoryUnavailable. A seek before the
     * first read has no read context, so its lookup and its write name the incarnation of the partition route.
     */
    @ParameterizedTest(name = "seek to offset {0}")
    @ValueSource(longs = {0, 5})
    void shouldNameTheRouteIncarnationInTheWriteOfASeekBeforeTheFirstRead(long target) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> offsetRequests = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                boolean lookup = request.is(GET_OFFSET_CODE, OPERATION_NON_REPLICATED);
                if (!lookup
                        && request.operation() != VsrOperation.STORE_CONSUMER_OFFSET
                        && request.operation() != VsrOperation.DELETE_CONSUMER_OFFSET) {
                    return unexpected(request);
                }
                offsetRequests.add(request);
                if (request.incarnation() == 0) {
                    return Response.error(request.operation(), IggyErrorCode.HISTORY_UNAVAILABLE.getCode());
                }
                return lookup
                        ? Response.success(OPERATION_NON_REPLICATED, storedOffset())
                        : Response.committed(
                                request.operation(), 2, Unpooled.buffer().writeIntLE(0));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var reader = client.reader(StreamId.of(1L), TopicId.of(1L), Consumer.of(7L))
                        .partitions(List.of(1L))
                        .options(PollOptions.waitFor(Duration.ofSeconds(30)).withCommit(CommitPolicy.onRead()))
                        .build()
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                AsyncIggyTcpClientEventLoopGroupTest.await(
                        () -> reader.partitions().contains(1L), "The reader reads partition 1", TIMEOUT);

                reader.seek(1L, PollingStrategy.offset(BigInteger.valueOf(target)))
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                reader.closeAsync().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(offsetRequests)
                        .isNotEmpty()
                        .allSatisfy(request -> assertThat(request.incarnation()).isEqualTo(SINGLE_NODE_INCARNATION));
                assertThat(offsetRequests)
                        .filteredOn(request -> request.operation() != OPERATION_NON_REPLICATED)
                        .extracting(Request::operation)
                        .containsExactly(
                                target == 0 ? VsrOperation.DELETE_CONSUMER_OFFSET : VsrOperation.STORE_CONSUMER_OFFSET);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * After a refusal as not the owner, the reader reads the owner of the partition from the context of a poll reply
     * only. So a reply whose messages do not decode fails a poll, but still gives its context.
     */
    @Test
    void shouldReadTheContextOfAPollReplyWhoseMessagesDoNotDecode() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(
                    socket,
                    request -> request.is(POLL_DEFERRED_CODE, OPERATION_NON_REPLICATED)
                            ? Response.success(OPERATION_NON_REPLICATED, undecodablePoll())
                            : unexpected(request));
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var transport = new TcpReaderTransport(client, StreamId.of(1L), TopicId.of(1L), Consumer.of(7L));
                DeferredPollOptions immediate = DeferredPollOptions.defaults().withMaxWait(Duration.ZERO);

                assertThatThrownBy(() -> transport
                                .poll(1L, PollingStrategy.next(), 1, immediate)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .hasCauseInstanceOf(IggyMalformedResponseException.class);
                assertThat(transport
                                .pollContext(1L, PollingStrategy.next(), 1, immediate)
                                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                        .isEqualTo(INSTALLED);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A handover installs each owner in a later metadata op than the join. So a sync reply with a new assignment raises
     * the watermark to its commit, and the next fetch names it. A sync reply with the same assignment at a later commit
     * raises nothing.
     */
    @Test
    void shouldNameTheCommitOfAChangedAssignmentInTheNextFetch() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicLong syncCommit = new AtomicLong(ASSIGNED_AT);
            List<Long> fetchOps = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.is(SYNC_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.committed(OPERATION_NON_REPLICATED, syncCommit.get(), assignment());
                }
                if (request.is(FETCH_CODE, OPERATION_NON_REPLICATED)) {
                    fetchOps.add(request.metadataOp());
                    return Response.success(OPERATION_NON_REPLICATED, emptySection(1));
                }
                return unexpected(request);
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                var transport = new TcpReaderTransport(client, StreamId.of(1L), TopicId.of(1L), Consumer.group(7L));
                Map<Long, PollingStrategy> targets = Map.of(1L, PollingStrategy.offset(BigInteger.ZERO));
                DeferredPollOptions options = PollOptions.waitFor(Duration.ZERO).deferredOptions();
                assertThat(transport.metadataWatermark()).isLessThan(BigInteger.valueOf(ASSIGNED_AT));

                transport.assignment().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                transport.read(targets, 1, false, options).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                syncCommit.set(ASSIGNED_AT + 1);
                transport.assignment().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                transport.read(targets, 1, false, options).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThat(fetchOps).containsExactly(ASSIGNED_AT, ASSIGNED_AT);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static Set<?> committing(TcpReaderTransport transport) throws ReflectiveOperationException {
        Field field = TcpReaderTransport.class.getDeclaredField("committing");
        field.setAccessible(true);
        return (Set<?>) field.get(transport);
    }

    /** Under OnRead an offset start applies only without a stored offset, so the first read looks it up. */
    private static Response noStoredOffset() {
        return Response.success(OPERATION_NON_REPLICATED, Unpooled.EMPTY_BUFFER);
    }

    private static ByteBuf storedOffset() {
        return Unpooled.buffer().writeIntLE(1).writeLongLE(CURRENT_OFFSET).writeLongLE(STORED_OFFSET);
    }

    /** A poll reply of partition 1 under {@link #INSTALLED} whose count names one message that it does not hold. */
    private static ByteBuf undecodablePoll() {
        return Unpooled.buffer()
                .writeIntLE(1)
                .writeLongLE(CURRENT_OFFSET)
                .writeIntLE(1)
                .writeLongLE(INSTALLED.incarnation().longValueExact())
                .writeLongLE(INSTALLED.ownerGeneration().longValueExact())
                .writeLongLE(INSTALLED.metadataOp().longValueExact());
    }

    /** A sync reply that assigns partition 1 in generation 1. */
    private static ByteBuf assignment() {
        return Unpooled.buffer().writeLongLE(1).writeIntLE(1).writeIntLE(1);
    }
}
