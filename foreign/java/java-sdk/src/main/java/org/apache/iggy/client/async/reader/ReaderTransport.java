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

package org.apache.iggy.client.async.reader;

import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The requests that a {@link PartitionReader} sends for one stream, topic and consumer. The clients of the SDK
 * implement it, an application does not call it: {@code AsyncIggyTcpClient#reader} reads the partitions of one
 * primary with one request.
 */
public interface ReaderTransport {

    /** Whether the consumer is a consumer group, whose member reads its assignment. */
    boolean group();

    /**
     * Groups {@code partitions} so that each group can be read with one {@link #read} request, in the order of
     * {@code partitions}.
     */
    CompletableFuture<List<List<Long>>> groups(List<Long> partitions);

    /**
     * Reads the partitions of one group, each from its strategy, an {@link org.apache.iggy.message.PollingKind#Offset}
     * or a {@link org.apache.iggy.message.PollingKind#Next}. A partition without a section was not read.
     */
    CompletableFuture<List<FetchedMessages>> read(
            Map<Long, PollingStrategy> targets, long count, boolean commit, DeferredPollOptions options);

    /**
     * Polls {@code partition} from {@code strategy} of any kind, without a store. A fetch takes only an offset or the
     * next offset, so the reader finds the offset of a timestamp or of the last messages with this poll first.
     */
    CompletableFuture<PolledMessages> poll(
            long partition, PollingStrategy strategy, long count, DeferredPollOptions options);

    /**
     * Polls {@code partition} as {@link #poll} does, but reads only the context of the reply. It decodes no message, so
     * a message that does not decode cannot fail it.
     */
    CompletableFuture<PartitionContext> pollContext(
            long partition, PollingStrategy strategy, long count, DeferredPollOptions options);

    /**
     * Stores {@code offset} of {@code partition} under the history of {@code context}. An empty context, also of a
     * delete, takes the context of the partition route: its incarnation, owner 0 and metadata op. Another context with
     * incarnation 0 takes only the incarnation of the route.
     */
    CompletableFuture<Void> store(long partition, BigInteger offset, PartitionContext context);

    CompletableFuture<Void> delete(long partition, PartitionContext context);

    /**
     * The highest metadata op that the replies of the client reported as committed: the reply that bound its session
     * and every reply of a replicated metadata command. A write without a context names it as its minimum metadata op.
     */
    BigInteger metadataWatermark();

    /** The offset that the server holds for {@code partition}. */
    CompletableFuture<Optional<ConsumerOffsetInfo>> storedOffset(long partition);

    /** Joins the consumer group. */
    CompletableFuture<Void> join();

    /** The current assignment of this member, or empty when it is not a member. */
    CompletableFuture<Optional<ConsumerGroupAssignment>> assignment();

    /** Leaves the consumer group. */
    CompletableFuture<Void> leave();

    /** Lets the client forget a read whose outcome the reader repairs by itself. */
    CompletableFuture<Void> abandon(long operationId);

    /** Completes once no committing read of the client that was cancelled after its send still waits for its reply. */
    CompletableFuture<Void> cancelledReadsSettled();

    /** Abandons the committing reads of this transport whose outcome stayed unknown, so their partitions take stores. */
    CompletableFuture<Void> abandonLost();
}
