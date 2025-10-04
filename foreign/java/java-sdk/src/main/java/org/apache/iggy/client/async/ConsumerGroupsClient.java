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
 * Async interface for consumer group operations.
 */
public interface ConsumerGroupsClient {

    /**
     * Joins a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    /**
     * Leaves a consumer group asynchronously.
     *
     * @param streamId The stream identifier
     * @param topicId The topic identifier
     * @param groupId The consumer group identifier
     * @return A CompletableFuture that completes when the operation is done
     */
    CompletableFuture<Void> leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);
}
