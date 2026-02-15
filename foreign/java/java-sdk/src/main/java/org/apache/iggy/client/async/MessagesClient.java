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

package org.apache.iggy.client.async;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async client interface for message operations (producing and consuming).
 *
 * <p>This is the core interface for interacting with Iggy's message streaming
 * capabilities. It supports both sending (producing) and polling (consuming)
 * messages asynchronously via {@link CompletableFuture}.
 *
 * <h2>Producing Messages</h2>
 * <pre>{@code
 * MessagesClient messages = client.messages();
 *
 * // Send messages with balanced partitioning (round-robin)
 * var msgs = List.of(Message.of("order-created"), Message.of("order-updated"));
 * messages.sendMessages(streamId, topicId, Partitioning.balanced(), msgs)
 *     .thenRun(() -> System.out.println("Messages sent"));
 *
 * // Send with message key partitioning (ensures ordering per key)
 * messages.sendMessages(streamId, topicId, Partitioning.messagesKey("user-123"), msgs);
 * }</pre>
 *
 * <h2>Consuming Messages</h2>
 * <pre>{@code
 * // Poll from the beginning
 * messages.pollMessages(streamId, topicId, Optional.empty(),
 *         Consumer.of(1L), PollingStrategy.first(), 100L, true)
 *     .thenAccept(polled -> {
 *         for (var msg : polled.messages()) {
 *             System.out.println(new String(msg.payload()));
 *         }
 *     });
 * }</pre>
 *
 * @see Partitioning
 * @see PollingStrategy
 * @see Consumer
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#messages()
 */
public interface MessagesClient {

    /**
     * Polls messages from a topic partition asynchronously.
     *
     * <p>Messages are retrieved according to the specified {@link PollingStrategy}, which
     * controls where in the partition to start reading (e.g., from the beginning, end,
     * a specific offset, or a timestamp).
     *
     * <p>When {@code autoCommit} is {@code true}, the server automatically stores the
     * consumer's offset after returning the messages. This simplifies offset management
     * but provides at-least-once delivery semantics.
     *
     * @param streamId    the stream identifier (numeric or string-based)
     * @param topicId     the topic identifier (numeric or string-based)
     * @param partitionId optional partition ID to poll from; if empty, the server selects
     *                    the partition (required when using consumer groups)
     * @param consumer    the consumer identity, either individual ({@link Consumer#of(Long)})
     *                    or group ({@link Consumer#group(Long)})
     * @param strategy    the polling strategy controlling where to start reading
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether the server should automatically commit the consumer offset
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     *         containing the retrieved messages and their metadata
     */
    CompletableFuture<PolledMessages> pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit);

    /**
     * Polls messages from a topic partition asynchronously using numeric identifiers.
     *
     * <p>This is a convenience overload that accepts raw numeric IDs instead of typed
     * identifier objects. See {@link #pollMessages(StreamId, TopicId, Optional, Consumer,
     * PollingStrategy, Long, boolean)} for full documentation.
     *
     * @param streamId    the numeric stream ID
     * @param topicId     the numeric topic ID
     * @param partitionId optional partition ID
     * @param consumerId  the numeric consumer ID
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether to auto-commit offsets
     * @return a {@link CompletableFuture} that completes with the {@link PolledMessages}
     */
    default CompletableFuture<PolledMessages> pollMessages(
            Long streamId,
            Long topicId,
            Optional<Long> partitionId,
            Long consumerId,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {
        return pollMessages(
                StreamId.of(streamId),
                TopicId.of(topicId),
                partitionId,
                Consumer.of(consumerId),
                strategy,
                count,
                autoCommit);
    }

    /**
     * Sends messages to a topic asynchronously.
     *
     * <p>Messages are routed to partitions according to the specified {@link Partitioning}
     * strategy:
     * <ul>
     *   <li>{@link Partitioning#balanced()} — round-robin distribution across partitions</li>
     *   <li>{@link Partitioning#partitionId(Long)} — send to a specific partition</li>
     *   <li>{@link Partitioning#messagesKey(String)} — hash-based routing that guarantees
     *       messages with the same key always go to the same partition, preserving order</li>
     * </ul>
     *
     * <p>Messages are batched into a single network request for efficiency. For high
     * throughput, accumulate messages and send them in larger batches rather than one
     * at a time.
     *
     * @param streamId     the stream identifier (numeric or string-based)
     * @param topicId      the topic identifier (numeric or string-based)
     * @param partitioning the partitioning strategy for routing messages
     * @param messages     the list of messages to send
     * @return a {@link CompletableFuture} that completes when all messages have been
     *         acknowledged by the server
     * @throws org.apache.iggy.exception.IggyException if the stream or topic does not exist
     */
    CompletableFuture<Void> sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);

    /**
     * Sends messages to a topic asynchronously using numeric identifiers.
     *
     * <p>This is a convenience overload that accepts raw numeric IDs. See
     * {@link #sendMessages(StreamId, TopicId, Partitioning, List)} for full documentation.
     *
     * @param streamId     the numeric stream ID
     * @param topicId      the numeric topic ID
     * @param partitioning the partitioning strategy
     * @param messages     the list of messages to send
     * @return a {@link CompletableFuture} that completes when messages are acknowledged
     */
    default CompletableFuture<Void> sendMessages(
            Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        return sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }
}
