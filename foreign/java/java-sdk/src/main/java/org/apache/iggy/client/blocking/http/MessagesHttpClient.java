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
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.http.SendContexts.TopicContexts;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyAuthorizationException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class MessagesHttpClient implements MessagesClient {

    /**
     * The server applies the byte limit to the equivalent binary selection and
     * then encodes it as JSON, which is larger. Rust bounds the JSON body at the
     * same multiple.
     */
    private static final long JSON_BODY_EXPANSION = 16;

    private static final long INITIAL_RETRY_INTERVAL_MILLIS = 50;
    private static final long MAX_RETRY_INTERVAL_MILLIS = 1000;

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
                contextParameter(strategy),
                new BasicNameValuePair("count", count.toString()),
                new BasicNameValuePair("auto_commit", Boolean.toString(autoCommit)));
        return httpClient.execute(request, PolledMessages.class);
    }

    @Override
    @SuppressWarnings("checkstyle:ParameterNumber")
    public PolledMessages pollMessagesDeferred(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        options.validate(count);
        if (consumer.kind() == Consumer.Kind.ConsumerGroup) {
            // The server refuses consumer_group on HTTP: without a persistent
            // connection the client cannot follow the group's partition assignment.
            throw new IggyOperationNotSupportedException(
                    "A consumer group cannot poll over HTTP, which cannot follow the group's partition assignment");
        }
        long startNanos = System.nanoTime();
        long requestDeadlineNanos = startNanos + options.requestTimeout().toNanos();
        long maxBodyBytes = Math.multiplyExact(options.maxBytes(), JSON_BODY_EXPANSION);
        long retryIntervalMillis = INITIAL_RETRY_INTERVAL_MILLIS;
        while (true) {
            // Both budgets carry across retries rather than restarting.
            DeferredPollOptions remaining = options.remaining(Duration.ofNanos(System.nanoTime() - startNanos))
                    .orElseThrow(() -> InternalHttpClient.deferredFailure(
                            false, new IggyTimeoutException("Deferred poll exceeded its request timeout")));
            ClassicHttpRequest request =
                    deferredRequest(streamId, topicId, partitionId, consumer, strategy, count, autoCommit, remaining);
            try {
                return httpClient.executeDeferred(request, PolledMessages.class, requestDeadlineNanos, maxBodyBytes);
            } catch (IggyServerException refused) {
                // Only an explicit non-admission proves nothing was served. An
                // opaque 503 can be an ambiguous not-committed or a proxy fault.
                if (refused.getRawErrorCode() != IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()
                        && refused.getRawErrorCode() != IggyErrorCode.DEFERRED_POLL_CAPACITY_EXCEEDED.getCode()) {
                    throw refused;
                }
                sleepBeforeRetry(retryIntervalMillis, requestDeadlineNanos, refused);
                retryIntervalMillis = Math.min(retryIntervalMillis * 2, MAX_RETRY_INTERVAL_MILLIS);
            }
        }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private ClassicHttpRequest deferredRequest(
            StreamId streamId,
            TopicId topicId,
            Optional<Long> partitionId,
            Consumer consumer,
            PollingStrategy strategy,
            Long count,
            boolean autoCommit,
            DeferredPollOptions options) {
        return httpClient.prepareGetRequest(
                path(streamId, topicId) + "/deferred",
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                partitionId
                        .map(id -> new BasicNameValuePair("partition_id", id.toString()))
                        .orElse(null),
                new BasicNameValuePair("kind", strategy.kind().name().toLowerCase()),
                new BasicNameValuePair("value", strategy.value().toString()),
                contextParameter(strategy),
                new BasicNameValuePair("count", count.toString()),
                new BasicNameValuePair("auto_commit", Boolean.toString(autoCommit)),
                new BasicNameValuePair("wait_us", Long.toString(options.maxWaitMicros())),
                new BasicNameValuePair("min_count", Long.toString(options.minCount())),
                new BasicNameValuePair("max_bytes", Long.toString(options.maxBytes())),
                new BasicNameValuePair("request_timeout_us", Long.toString(options.requestTimeoutMicros())));
    }

    // Without the context the server would read a recreated partition instead of refusing the poll.
    private static NameValuePair contextParameter(PollingStrategy strategy) {
        return strategy.context()
                .map(context -> new BasicNameValuePair(
                        "context", ObjectMapperFactory.getInstance().writeValueAsString(context)))
                .orElse(null);
    }

    private static void sleepBeforeRetry(long intervalMillis, long requestDeadlineNanos, IggyServerException refused) {
        if (requestDeadlineNanos - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(intervalMillis)) {
            throw refused;
        }
        try {
            Thread.sleep(intervalMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw InternalHttpClient.deferredFailure(false, interrupted);
        }
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
            // No attempt of this batch committed: InternalHttpClient reports the refusal of an
            // automatic resend after an attempt that may have committed as 57 instead. So one
            // resend under the contexts read again cannot duplicate the batch. The binary
            // transports resend once the same way.
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
