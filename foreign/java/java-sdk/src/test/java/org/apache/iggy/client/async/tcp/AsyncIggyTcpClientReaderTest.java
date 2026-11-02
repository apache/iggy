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

import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.RequestHandler;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.assertCancels;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.emptySection;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.signedIn;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.singleNode;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.unexpected;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;

/** A partition reader on the TCP client against a mock node. */
class AsyncIggyTcpClientReaderTest {
    private static final int FETCH_CODE = CommandCode.Messages.FETCH_MESSAGES.getValue();
    private static final int CANCEL_CODE = CommandCode.Messages.CANCEL_DEFERRED_REQUEST.getValue();
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
     * A parked committing fetch whose cancel loses the link ends with an unknown outcome and blocks its partition on
     * the client, so the close of its reader abandons it, and a store of the partition on the same client goes through
     * after it.
     */
    @Test
    void shouldAbandonAParkedFetchWhoseCancelLosesTheLinkWhenTheReaderCloses() throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Request> parked = new CompletableFuture<>();
            RequestHandler handler = request -> {
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
                client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.login().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
}
