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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.PartitionContext;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The partition contexts that fence sends to an explicit partition, read from the topic details. A
 * topic's contexts are dropped when a send to it is refused for a changed incarnation, and all of
 * them when this client deletes a stream or a topic, or creates or deletes partitions.
 */
final class SendContexts {

    private final Map<TopicKey, TopicContexts> topics = new ConcurrentHashMap<>();

    /**
     * Returns the context of the partition. The topic is read again when its last read did not list
     * the partition, so a partition created since then is fenced too. An empty result means the send
     * goes without one, so the server captures it.
     */
    Optional<PartitionContext> get(StreamId streamId, TopicId topicId, long partitionId, Supplier<TopicContexts> read) {
        TopicKey key = TopicKey.of(streamId, topicId);
        // Not computeIfAbsent: the read is an HTTP request and must not block the map.
        TopicContexts topic = topics.get(key);
        if (topic == null || topic.lacks(partitionId)) {
            topic = read.get();
            topics.put(key, topic);
        }
        return Optional.ofNullable(topic.partitions().get(partitionId));
    }

    void forget(StreamId streamId, TopicId topicId) {
        topics.remove(TopicKey.of(streamId, topicId));
    }

    void clear() {
        topics.clear();
    }

    /**
     * What one read of the topic reported. A caller that may send to the topic but not read it gets
     * no contexts, and the topic is not read for it again until its contexts are dropped.
     */
    record TopicContexts(Map<Long, PartitionContext> partitions, boolean refused) {
        static final TopicContexts REFUSED = new TopicContexts(Map.of(), true);

        static TopicContexts of(Map<Long, PartitionContext> partitions) {
            return new TopicContexts(partitions, false);
        }

        private boolean lacks(long partitionId) {
            return !refused && !partitions.containsKey(partitionId);
        }
    }

    /** Keyed by the identifiers as the REST path carries them, which is how the server resolves them. */
    private record TopicKey(String streamId, String topicId) {
        static TopicKey of(StreamId streamId, TopicId topicId) {
            return new TopicKey(streamId.toString(), topicId.toString());
        }
    }
}
