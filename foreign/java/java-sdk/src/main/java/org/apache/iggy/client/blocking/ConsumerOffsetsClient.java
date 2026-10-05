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
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.Optional;

public interface ConsumerOffsetsClient {

    default void storeConsumerOffset(
            Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId, BigInteger offset) {
        storeConsumerOffset(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId), offset);
    }

    void storeConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset);

    /**
     * Stores a consumer offset fenced by the context its messages were polled with.
     * The server refuses the store once the partition incarnation or its owner has
     * changed, so an offset from an older incarnation never lands in a newer one.
     */
    void storeConsumerOffset(
            StreamId streamId,
            TopicId topicId,
            Long partitionId,
            Consumer consumer,
            BigInteger offset,
            PartitionContext context);

    default Optional<ConsumerOffsetInfo> getConsumerOffset(
            Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId) {
        return getConsumerOffset(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId));
    }

    Optional<ConsumerOffsetInfo> getConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer);
}
