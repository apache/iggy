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

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.http.SendContexts.TopicContexts;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyAuthorizationException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PartitioningKind;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.partition.PartitionContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class MessagesHttpClient implements MessagesClient {

    private final InternalHttpClient httpClient;
    private final TopicsClient topicsClient;
    private final SendContexts sendContexts;

    public MessagesHttpClient(InternalHttpClient httpClient, TopicsClient topicsClient, SendContexts sendContexts) {
        this.httpClient = httpClient;
        this.topicsClient = topicsClient;
        this.sendContexts = sendContexts;
    }

    @Override
    public PolledMessages pollMessages(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit) {
        var request = httpClient.prepareGetRequest(
                path(streamId, topicId),
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                partitionId
                        .map(id -> new BasicNameValuePair("partition_id", id.toString()))
                        .orElse(null),
                new BasicNameValuePair("kind", strategy.kind().name().toLowerCase()),
                new BasicNameValuePair("value", strategy.value().toString()),
                strategy.context()
                        .map(context -> new BasicNameValuePair(
                                "context", ObjectMapperFactory.getInstance().writeValueAsString(context)))
                        .orElse(null),
                new BasicNameValuePair("count", count.toString()),
                new BasicNameValuePair("auto_commit", Boolean.toString(autoCommit)));
        return httpClient.execute(request, PolledMessages.class);
    }

    @Override
    public SendMessagesResponse sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        try {
            return post(streamId, topicId, partitioning, messages);
        } catch (IggyServerException error) {
            if (error.getErrorCode() != IggyErrorCode.HISTORY_UNAVAILABLE) {
                throw error;
            }
            // A refused batch did not commit, so one resend under the contexts read again cannot
            // duplicate it. The binary transports resend once the same way.
            return post(streamId, topicId, partitioning, messages);
        }
    }

    private SendMessagesResponse post(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        var command = new SendMessages(partitioning, messages, sendContext(streamId, topicId, partitioning));
        var request = httpClient.preparePostRequest(path(streamId, topicId), command);
        String body;
        try {
            body = httpClient.executeWithStringResponse(request);
        } catch (IggyServerException error) {
            // The partition was recreated after its topic was read.
            if (error.getErrorCode() == IggyErrorCode.HISTORY_UNAVAILABLE) {
                sendContexts.forget(streamId, topicId);
            }
            throw error;
        }
        if (body.isBlank()) {
            return SendMessagesResponse.empty();
        }
        return ObjectMapperFactory.getInstance().readValue(body, SendMessagesResponse.class);
    }

    // Balanced and keyed sends carry no context, so the server captures the one of the partition it picks.
    private Optional<PartitionContext> sendContext(StreamId streamId, TopicId topicId, Partitioning partitioning) {
        if (partitioning.kind() != PartitioningKind.PartitionId) {
            return Optional.empty();
        }
        long partitionId = Integer.toUnsignedLong(ByteBuffer.wrap(partitioning.value())
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt());
        return sendContexts.get(streamId, topicId, partitionId, () -> readContexts(streamId, topicId));
    }

    private TopicContexts readContexts(StreamId streamId, TopicId topicId) {
        try {
            return TopicContexts.of(topicsClient
                    .getTopic(streamId, topicId)
                    .map(topic -> topic.partitions().stream()
                            .collect(Collectors.toUnmodifiableMap(Partition::id, Partition::context)))
                    .orElse(Map.of()));
        } catch (IggyAuthorizationException error) {
            // Sending needs no read access to the topic. Without it the sends carry no context, and
            // the server captures one for each.
            return TopicContexts.REFUSED;
        }
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/messages";
    }

    private record SendMessages(
            Partitioning partitioning,
            List<Message> messages,
            @JsonInclude(JsonInclude.Include.NON_ABSENT) Optional<PartitionContext> context) {}
}
