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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.List;
import java.util.Optional;

final class ConsumerGroupsTcpClient implements ConsumerGroupsClient {

    private final org.apache.iggy.client.async.ConsumerGroupsClient delegate;

    ConsumerGroupsTcpClient(org.apache.iggy.client.async.ConsumerGroupsClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        return FutureUtil.resolve(delegate.getConsumerGroup(streamId, topicId, groupId));
    }

    @Override
    public List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId) {
        return FutureUtil.resolve(delegate.getConsumerGroups(streamId, topicId));
    }

    @Override
    public ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, String name) {
        return FutureUtil.resolve(delegate.createConsumerGroup(streamId, topicId, name));
    }

    @Override
    public void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        FutureUtil.resolve(delegate.deleteConsumerGroup(streamId, topicId, groupId));
    }

    @Override
    public void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        FutureUtil.resolve(delegate.joinConsumerGroup(streamId, topicId, groupId));
    }

    @Override
    public void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        FutureUtil.resolve(delegate.leaveConsumerGroup(streamId, topicId, groupId));
    }
}
