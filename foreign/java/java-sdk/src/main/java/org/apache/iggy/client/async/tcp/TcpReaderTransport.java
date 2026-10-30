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

import org.apache.iggy.client.async.reader.ReaderTransport;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The reads of a reader over TCP: one fetch per primary, sent to the primary of its partitions. */
final class TcpReaderTransport implements ReaderTransport {
    private final AsyncIggyTcpClient client;
    private final StreamId stream;
    private final TopicId topic;
    private final Consumer consumer;

    TcpReaderTransport(AsyncIggyTcpClient client, StreamId stream, TopicId topic, Consumer consumer) {
        this.client = client;
        this.stream = stream;
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public boolean group() {
        return consumer.kind() == Consumer.Kind.ConsumerGroup;
    }

    @Override
    public CompletableFuture<List<List<Long>>> groups(List<Long> partitions) {
        return messages().readinessGroups(stream, topic, partitions);
    }

    @Override
    public CompletableFuture<List<FetchedMessages>> read(
            Map<Long, PollingStrategy> targets, long count, boolean commit, DeferredPollOptions options) {
        // A fetch needs the reply margin also without a wait.
        DeferredPollOptions fetch =
                options.requestTimeout().compareTo(options.maxWait().plus(DeferredPollOptions.REPLY_MARGIN)) < 0
                        ? options.withRequestTimeout(options.maxWait().plus(DeferredPollOptions.REPLY_MARGIN))
                        : options;
        return messages().fetchMessages(stream, topic, targets, consumer, count, commit, fetch);
    }

    @Override
    public CompletableFuture<Void> store(long partition, BigInteger offset, PartitionContext context) {
        return client.consumerOffsets().storeConsumerOffset(stream, topic, partition, consumer, offset, context);
    }

    @Override
    public CompletableFuture<Void> delete(long partition, PartitionContext context) {
        return ((ConsumerOffsetsTcpClient) client.consumerOffsets())
                .deleteConsumerOffset(stream, topic, partition, consumer, context);
    }

    @Override
    public CompletableFuture<Optional<ConsumerOffsetInfo>> storedOffset(long partition) {
        return client.consumerOffsets().getConsumerOffset(stream, topic, Optional.of(partition), consumer);
    }

    @Override
    public CompletableFuture<Void> join() {
        return client.consumerGroups().joinConsumerGroup(stream, topic, consumer.id());
    }

    @Override
    public CompletableFuture<Optional<ConsumerGroupAssignment>> assignment() {
        return client.consumerGroups().syncConsumerGroup(stream, topic, consumer.id());
    }

    @Override
    public CompletableFuture<Void> leave() {
        return client.consumerGroups().leaveConsumerGroup(stream, topic, consumer.id());
    }

    @Override
    public CompletableFuture<Void> abandon(long operationId) {
        return client.abandonOperation(operationId);
    }

    private MessagesTcpClient messages() {
        return (MessagesTcpClient) client.messages();
    }
}
