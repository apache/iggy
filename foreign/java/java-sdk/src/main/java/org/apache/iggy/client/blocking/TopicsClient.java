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

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TopicsClient {

    default Optional<TopicDetails> getTopic(Long streamId, Long topicId) {
        return getTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId);

    default List<Topic> getTopics(Long streamId) {
        return getTopics(StreamId.of(streamId));
    }

    /**
     * Gets the topics the caller may read within a stream. Over TCP, reads the list in pages of up to 1000 topics, so
     * the list is not one consistent read. All pages share the sign-in and the request timeout of the call, so a new
     * sign-in during the read fails it. If the stream goes missing after the first page, the call fails as
     * {@link #getTopicsPage} does for a caller with a global read grant, and ends there for any other caller. Over
     * TCP, the call fails with {@link org.apache.iggy.exception.IggyMalformedResponseException} when the server
     * answers the page cursor it was sent. Over HTTP, reads the whole list in one reply, which the server builds from
     * pages of 1000, so it is not one consistent read either.
     */
    List<Topic> getTopics(StreamId streamId);

    /**
     * Gets one page of the topics the caller may read within a stream. For a stream that does not exist, or one the
     * caller may read no topic of, the first page is empty and has no next cursor. For a caller with a global read
     * grant, a page read with a cursor fails with a stream not found error instead, because the stream was deleted
     * during the walk. A query that breaks the page rules fails with an invalid command error.
     */
    Page<Topic> getTopicsPage(StreamId streamId, NamedPageQuery query);

    default TopicDetails createTopic(Long streamId, String name, long partitionsCount) {
        return createTopic(StreamId.of(streamId), name, partitionsCount);
    }

    default TopicDetails createTopic(Long streamId, String name, long partitionsCount, TopicOptions options) {
        return createTopic(StreamId.of(streamId), name, partitionsCount, options);
    }

    /** Creates a topic with the server's default settings. */
    default TopicDetails createTopic(StreamId streamId, String name, long partitionsCount) {
        return createTopic(
                streamId, name, partitionsCount, TopicOptions.builder().build());
    }

    /** Creates a topic with typed settings. Unset keys resolve to the server defaults. */
    default TopicDetails createTopic(StreamId streamId, String name, long partitionsCount, TopicOptions options) {
        return createTopic(streamId, name, partitionsCount, options.entries());
    }

    /**
     * Creates a topic with settings keyed by option name, for keys newer than this build.
     *
     * <p>{@code options} reuses {@link HeaderValue} because options ride the user-headers codec. A key
     * outside the catalog is refused by name.
     */
    TopicDetails createTopic(StreamId streamId, String name, long partitionsCount, Map<String, HeaderValue> options);

    default void updateTopic(Long streamId, Long topicId, TopicUpdate update) {
        updateTopic(StreamId.of(streamId), TopicId.of(topicId), update);
    }

    /**
     * Updates a topic's name, message expiry or size cap, or resets the last two to their defaults. Every
     * key left out keeps its value.
     */
    void updateTopic(StreamId streamId, TopicId topicId, TopicUpdate update);

    default void deleteTopic(Long streamId, Long topicId) {
        deleteTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    void deleteTopic(StreamId streamId, TopicId topicId);
}
