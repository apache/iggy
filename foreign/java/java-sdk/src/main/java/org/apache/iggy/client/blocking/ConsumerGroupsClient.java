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

import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.List;
import java.util.Optional;

public interface ConsumerGroupsClient {

    default Optional<ConsumerGroupDetails> getConsumerGroup(Long streamId, Long topicId, Long groupId) {
        return getConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default List<ConsumerGroup> getConsumerGroups(Long streamId, Long topicId) {
        return getConsumerGroups(StreamId.of(streamId), TopicId.of(topicId));
    }

    List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId);

    default ConsumerGroupDetails createConsumerGroup(Long streamId, Long topicId, String name) {
        return createConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), name);
    }

    ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, String name);

    default void deleteConsumerGroup(Long streamId, Long topicId, Long groupId) {
        deleteConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default void joinConsumerGroup(Long streamId, Long topicId, Long groupId) {
        joinConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default void leaveConsumerGroup(Long streamId, Long topicId, Long groupId) {
        leaveConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);
}
