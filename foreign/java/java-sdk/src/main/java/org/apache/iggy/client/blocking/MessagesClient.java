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
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
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

    PolledMessages pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit);

    /**
     * Polls messages with a deferred wait using numeric identifiers.
     *
     * <p>See {@link #pollMessagesDeferred(StreamId, TopicId, Optional, Consumer, PollingStrategy,
     * Long, boolean, DeferredPollOptions)} for full documentation.
     *
     * @param streamId    the numeric stream ID
     * @param topicId     the numeric topic ID
     * @param partitionId optional partition ID
     * @param consumerId  the numeric consumer ID
     * @param strategy    the polling strategy
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether to auto-commit offsets
     * @param options     the readiness, byte and request-time limits
     * @return the polled messages
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    default PolledMessages pollMessagesDeferred(
            Long streamId,
            Long topicId,
            Optional<Long> partitionId,
            Long consumerId,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        return pollMessagesDeferred(
                StreamId.of(streamId),
                TopicId.of(topicId),
                partitionId,
                Consumer.of(consumerId),
                strategy,
                count,
                autoCommit,
                options);
    }

    /**
     * Polls messages, letting the server hold the request until data is ready.
     *
     * <p>Unlike {@link #pollMessages}, which reads whatever is resident and returns, this
     * waits up to {@link DeferredPollOptions#maxWait()} for {@link DeferredPollOptions#minCount()}
     * messages, caps the encoded response at {@link DeferredPollOptions#maxBytes()}, and bounds
     * the whole exchange by {@link DeferredPollOptions#requestTimeout()}. An expired readiness
     * wait can still return partial or empty data; an expired request timeout is an error.
     *
     * <p>The HTTP client accepts only a plain consumer, because its JSON poll query cannot
     * carry a consumer kind. A group consumer is rejected before any request is sent.
     *
     * @param streamId    the stream identifier (numeric or string-based)
     * @param topicId     the topic identifier (numeric or string-based)
     * @param partitionId optional partition ID to poll from
     * @param consumer    the consumer identity
     * @param strategy    the polling strategy controlling where to start reading
     * @param count       the maximum number of messages to return
     * @param autoCommit  whether the server should automatically commit the consumer offset
     * @param options     the readiness, byte and request-time limits
     * @return the polled messages
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    default PolledMessages pollMessagesDeferred(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        throw new IggyOperationNotSupportedException(
                "pollMessagesDeferred", getClass().getSimpleName());
    }

    default SendMessagesResponse sendMessages(
            Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        return sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }

    SendMessagesResponse sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);
}
