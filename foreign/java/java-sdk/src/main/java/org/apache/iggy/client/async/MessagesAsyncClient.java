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
 * Asynchronous client interface for message operations.
 */
public interface MessagesAsyncClient {

    /**
     * Asynchronously polls messages from a topic.
     *
     * @param streamId    the stream ID
     * @param topicId     the topic ID
     * @param partitionId the partition ID (optional)
     * @param consumerId  the consumer ID
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to poll
     * @param autoCommit  whether to automatically commit the offset
     * @return a CompletableFuture that completes with the polled messages
     */
    default CompletableFuture<PolledMessages> pollMessages(Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId, PollingStrategy strategy, Long count, boolean autoCommit) {
        return pollMessages(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId),
                strategy, count, autoCommit);
    }

    /**
     * Asynchronously polls messages from a topic.
     *
     * @param streamId    the stream ID
     * @param topicId     the topic ID
     * @param partitionId the partition ID (optional)
     * @param consumer    the consumer
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to poll
     * @param autoCommit  whether to automatically commit the offset
     * @return a CompletableFuture that completes with the polled messages
     */
    CompletableFuture<PolledMessages> pollMessages(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, PollingStrategy strategy, Long count, boolean autoCommit);

    /**
     * Asynchronously sends messages to a topic.
     *
     * @param streamId     the stream ID
     * @param topicId      the topic ID
     * @param partitioning the partitioning strategy
     * @param messages     the messages to send
     * @return a CompletableFuture that completes when the messages are sent
     */
    default CompletableFuture<Void> sendMessages(Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        return sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }

    /**
     * Asynchronously sends messages to a topic.
     *
     * @param streamId     the stream ID
     * @param topicId      the topic ID
     * @param partitioning the partitioning strategy
     * @param messages     the messages to send
     * @return a CompletableFuture that completes when the messages are sent
     */
    CompletableFuture<Void> sendMessages(StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);
}
