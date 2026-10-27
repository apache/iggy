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

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientNamespaceMissTest.Cluster;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientNamespaceMissTest.Node;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientNamespaceMissTest.ANSWERED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A partition primary answers 57 when it could not commit a write yet, also for a write it relayed that expired. The
 * client replays the same request until the deadline, and then the outcome is uncertain.
 */
class AsyncIggyTcpClientPartitionWriteTest {
    private static final int TEST_TIMEOUT_SECONDS = 5;

    @ParameterizedTest
    @EnumSource(Write.class)
    void shouldSucceedWhenTheReplayOfAnUncommittedWriteCommits(Write write) throws Exception {
        try (Cluster cluster = new Cluster()) {
            List<Request> writes = write.script(
                    cluster.backup, attempt -> attempt == 1 ? AsyncTcpConnection.TRANSIENT_NOT_COMMITTED : ANSWERED);
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                write.send(client).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(writes)
                    .hasSize(2)
                    .extracting(Request::requestId)
                    .containsOnly(writes.get(0).requestId());
        }
    }

    @ParameterizedTest
    @EnumSource(Write.class)
    void shouldEndAsUncertainWhenTheWriteStaysUncommittedUntilTheDeadline(Write write) throws Exception {
        try (Cluster cluster = new Cluster()) {
            List<Request> writes = write.script(cluster.backup, attempt -> AsyncTcpConnection.TRANSIENT_NOT_COMMITTED);
            AsyncIggyTcpClient client = cluster.signIn();
            try {
                assertServerError(write.send(client), IggyErrorCode.OPERATION_UNCERTAIN.getCode());
            } finally {
                client.close().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            cluster.awaitServed();
            assertThat(writes)
                    .hasSizeGreaterThan(1)
                    .extracting(Request::requestId)
                    .containsOnly(writes.get(0).requestId());
        }
    }

    enum Write {
        SEND_MESSAGES,
        STORE_CONSUMER_OFFSET;

        /** Scripts how the partition primary answers this write, and returns where it records each attempt. */
        List<Request> script(Node primary, IntUnaryOperator status) {
            if (this == SEND_MESSAGES) {
                primary.sendStatus = status;
                return primary.sends;
            }
            primary.offsetStoreStatus = status;
            return primary.offsetStores;
        }

        CompletableFuture<?> send(AsyncIggyTcpClient client) {
            return this == SEND_MESSAGES
                    ? client.messages()
                            .sendMessages(
                                    StreamId.of(1L),
                                    TopicId.of(1L),
                                    Partitioning.partitionId(0L),
                                    List.of(Message.of("m")))
                    : client.consumerOffsets()
                            .storeConsumerOffset(
                                    StreamId.of(1L), TopicId.of(1L), Optional.of(0L), Consumer.of(7L), BigInteger.ONE);
        }
    }
}
