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

import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.concurrent.CompletableFuture;

/**
 * Async client interface for consumer group operations.
 *
 * <p>Consumer groups enable coordinated message consumption across multiple clients.
 * When a client joins a consumer group, the server assigns topic partitions to that
 * client. The partition assignment is rebalanced automatically when members join or
 * leave.
 *
 * <p><strong>Important:</strong> Consumer group membership is tied to the TCP connection.
 * If a client disconnects, it is automatically removed from the group and partitions
 * are reassigned to remaining members.
 *
 * <p>Usage example:
 * <pre>{@code
 * ConsumerGroupsClient groups = client.consumerGroups();
 *
 * // Join a group before polling with Consumer.group()
 * groups.joinConsumerGroup(streamId, topicId, ConsumerId.of(1L))
 *     .thenRun(() -> System.out.println("Joined consumer group"))
 *     .thenCompose(v -> client.messages().pollMessages(
 *         streamId, topicId, Optional.empty(),
 *         Consumer.group(1L), PollingStrategy.next(), 100L, true));
 *
 * // Leave the group when done
 * groups.leaveConsumerGroup(streamId, topicId, ConsumerId.of(1L));
 * }</pre>
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient#consumerGroups()
 */
public interface ConsumerGroupsClient {

    /**
     * Joins a consumer group asynchronously.
     *
     * <p>The client becomes a member of the specified consumer group and will be assigned
     * one or more partitions to consume from. The membership is tied to this TCP connection
     * — disconnecting will automatically remove the client from the group.
     *
     * <p>A client must join the consumer group before polling messages with a
     * {@link org.apache.iggy.consumergroup.Consumer#group(Long)} consumer type.
     *
     * @param streamId the stream identifier containing the topic
     * @param topicId  the topic identifier
     * @param groupId  the consumer group identifier to join
     * @return a {@link CompletableFuture} that completes when the client has joined
     * @throws org.apache.iggy.exception.IggyException if the consumer group does not exist
     */
    CompletableFuture<Void> joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    /**
     * Leaves a consumer group asynchronously.
     *
     * <p>The client is removed from the group and its assigned partitions are redistributed
     * among the remaining members. After leaving, the client can no longer poll messages
     * using a group consumer for this group until it joins again.
     *
     * @param streamId the stream identifier containing the topic
     * @param topicId  the topic identifier
     * @param groupId  the consumer group identifier to leave
     * @return a {@link CompletableFuture} that completes when the client has left
     * @throws org.apache.iggy.exception.IggyException if the client is not a member of the
     *         group
     */
    CompletableFuture<Void> leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);
}
