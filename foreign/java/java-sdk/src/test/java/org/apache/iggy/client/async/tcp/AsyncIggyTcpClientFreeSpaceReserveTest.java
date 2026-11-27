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
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.SendMessagesResponse;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.partitionRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A primary below its free-space reserve stored nothing and is still the primary, so a send goes to it again on the
 * same route until its deadline, which ends the send with that refusal, as in Rust.
 */
class AsyncIggyTcpClientFreeSpaceReserveTest {
    private static final int GET_PARTITION_ROUTE_CODE = 105;
    private static final int OPERATION_SEND_MESSAGES = 160;
    private static final int FREE_SPACE_RESERVE_REACHED = IggyErrorCode.FREE_SPACE_RESERVE_REACHED.getCode();

    @Test
    void shouldResendToTheSamePrimaryUntilItHasRoom() throws Exception {
        assertSends(2, (client, sends, routes) -> {
            send(client).get(5, TimeUnit.SECONDS);

            assertThat(sends).hasSize(3);
            assertThat(sends)
                    .extracting(Request::requestId)
                    .containsOnly(sends.get(0).requestId());
            assertThat(routes).hasValue(1);
        });
    }

    @Test
    void shouldEndWithTheRefusalAtTheDeadline() throws Exception {
        assertSends(Integer.MAX_VALUE, (client, sends, routes) -> {
            assertServerError(send(client), FREE_SPACE_RESERVE_REACHED);

            assertThat(sends).hasSizeGreaterThan(1);
            assertThat(sends)
                    .extracting(Request::requestId)
                    .containsOnly(sends.get(0).requestId());
            assertThat(routes).hasValue(1);
        });
    }

    /** Runs {@code scenario} against a node whose partition primary refuses the first {@code refusals} sends. */
    private static void assertSends(int refusals, Scenario scenario) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            List<Request> sends = new CopyOnWriteArrayList<>();
            AtomicInteger routes = new AtomicInteger();
            CompletableFuture<Void> server = serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    routes.incrementAndGet();
                    return Response.success(OPERATION_NON_REPLICATED, partitionRoute(Unpooled.buffer(), 1));
                }
                if (request.operation() == OPERATION_SEND_MESSAGES) {
                    sends.add(request);
                    return sends.size() <= refusals
                            ? Response.error(OPERATION_SEND_MESSAGES, FREE_SPACE_RESERVE_REACHED)
                            : Response.success(OPERATION_SEND_MESSAGES, Unpooled.EMPTY_BUFFER);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                scenario.run(client, sends, routes);
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<SendMessagesResponse> send(AsyncIggyTcpClient client) {
        return client.messages()
                .sendMessages(StreamId.of(1L), TopicId.of(1L), Partitioning.partitionId(0L), List.of(Message.of("m")));
    }

    @FunctionalInterface
    private interface Scenario {
        void run(AsyncIggyTcpClient client, List<Request> sends, AtomicInteger routes) throws Exception;
    }
}
