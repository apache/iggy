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

package org.apache.iggy.client.blocking;

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;

import java.util.List;
import java.util.Optional;

public interface MessagesClient {

    default PolledMessages pollMessages(
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
     * Polls messages from a topic partition.
     *
     * <p>Over TCP, a poll without a caller context takes the partition context from the
     * client's route to the partition, and the client caches that route. After another
     * client deletes and recreates the partition, such a poll can fail once with error 87
     * (history unavailable), or 5009 (partition not owned) for a group consumer. The
     * client returns that error and does not retry it. It drops the failed route, so the
     * next call routes again. A group poll without a partition ID is the exception for
     * 5009: it syncs the assignment again and retries once, unless its strategy has a
     * context.
     *
     * <p>To continue at an offset from an earlier poll, pass the
     * {@link PolledMessages#context()} of that poll with {@link PollingStrategy#withContext}.
     * Then a poll of a recreated partition is refused instead of reading it at an offset
     * from the old incarnation.
     */
    PolledMessages pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit);

    default SendMessagesResponse sendMessages(
            Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        return sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }

    SendMessagesResponse sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);
}
