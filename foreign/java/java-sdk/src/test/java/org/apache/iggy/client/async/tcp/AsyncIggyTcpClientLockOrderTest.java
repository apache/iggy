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

import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEventLoopGroupTest.await;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.BIND_SESSION_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_PARTITION_ROUTE_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.POLL_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.bindBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.clusterMetadata;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.emptyPoll;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.pollRoute;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * With one IO thread, a client runs all of its links on one event loop, so a lock that the loop waits for stops them
 * all.
 */
class AsyncIggyTcpClientLockOrderTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    // Shorter than the dial timeout, so the deadline of a poll finds the dial of its link still running.
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(500);
    private static final int QUEUE_PROBE_MILLIS = 200;
    private static final int MAX_QUEUED_DIALS = 64;
    // Twice the pause before a poll that lost its link dials again.
    private static final int REDIAL_SETTLE_MILLIS = 100;

    /**
     * The partition primary goes down, so a poll dials it again until its deadline. The deadline ends the poll under
     * the poll monitor and cancels the dial, which reports a link failure on the event loop. A sign-in holds the lock
     * of the client while it ends the pending polls, which takes that poll monitor, so the event loop must not wait
     * for that lock.
     */
    @Test
    void shouldNotWaitForTheClientLockWhenThePollDeadlineEndsTheDialOfItsLink() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        IoEventLoopGroup group = new MultiThreadIoEventLoopGroup(
                1, new DefaultThreadFactory("lock-order", true), NioIoHandler.newFactory());
        List<Socket> queued = new ArrayList<>();
        try (ServerSocket leader = new ServerSocket(0, 4, loopback);
                ServerSocket primary = new ServerSocket(0, 1, loopback)) {
            var leaderServed = serve(leader, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(1));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(
                            OPERATION_NON_REPLICATED,
                            clusterMetadata(leader.getLocalPort(), primary.getLocalPort(), leader.getLocalPort()));
                }
                if (request.is(GET_PARTITION_ROUTE_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, pollRoute(request, 1, 1, primary.getLocalPort()));
                }
                throw new IllegalStateException("Unexpected leader request: " + request);
            });
            AtomicInteger polls = new AtomicInteger();
            var primaryServed = serve(primary, request -> {
                if (request.is(BIND_SESSION_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, bindBody(request));
                }
                if (!request.is(POLL_CODE, OPERATION_NON_REPLICATED)) {
                    throw new IllegalStateException("Unexpected primary request: " + request);
                }
                // The node goes down at the second poll and accepts no other link.
                return polls.incrementAndGet() == 1
                        ? Response.success(OPERATION_NON_REPLICATED, emptyPoll(0))
                        : Response.disconnect();
            });
            AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                    .host(loopback.getHostAddress())
                    .port(leader.getLocalPort())
                    .credentials("iggy", "iggy")
                    .requestTimeout(REQUEST_TIMEOUT)
                    .heartbeatInterval(Duration.ofHours(1))
                    .eventLoopGroup(group)
                    .build();
            Thread loop = group.next().submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                poll(client).get(5, TimeUnit.SECONDS);
                fillAcceptQueue(primary, queued);
                CompletableFuture<PolledMessages> stalled = poll(client);
                primaryServed.get(5, TimeUnit.SECONDS);
                // The link failure of the drop runs before the lock is taken, so the loop meets the lock at the
                // deadline of the poll. If it met the lock earlier, the probe below would still fail.
                TimeUnit.MILLISECONDS.sleep(REDIAL_SETTLE_MILLIS);

                synchronized (client) {
                    await(
                            () -> loop.getState() == Thread.State.BLOCKED || stalled.isDone(),
                            "the deadline of the poll",
                            TIMEOUT);
                    assertThat(group.next().submit(() -> {})).succeedsWithin(TIMEOUT);
                }
                assertThat(stalled).failsWithin(TIMEOUT);
            } finally {
                for (Socket socket : queued) {
                    socket.close();
                }
                client.close().get(5, TimeUnit.SECONDS);
                group.shutdownGracefully(0, 1, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
            }
            leaderServed.get(5, TimeUnit.SECONDS);
        }
    }

    private static CompletableFuture<PolledMessages> poll(AsyncIggyTcpClient client) {
        return client.messages()
                .pollMessages(
                        StreamId.of(1L),
                        TopicId.of(1L),
                        Optional.of(0L),
                        Consumer.of(ConsumerId.of(7L)),
                        PollingStrategy.offset(BigInteger.ZERO),
                        1L,
                        false);
    }

    /** Fills the accept queue of a node that accepts no more links, so that the next dial waits for its timeout. */
    private static void fillAcceptQueue(ServerSocket node, List<Socket> queued) throws IOException {
        while (queued.size() < MAX_QUEUED_DIALS) {
            Socket socket = new Socket();
            try {
                socket.connect(node.getLocalSocketAddress(), QUEUE_PROBE_MILLIS);
                queued.add(socket);
            } catch (SocketTimeoutException full) {
                socket.close();
                return;
            }
        }
        throw new IllegalStateException("The accept queue of the node did not fill");
    }
}
