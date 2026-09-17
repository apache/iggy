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

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
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
import java.util.concurrent.TimeUnit;

class MessagesHttpClient implements MessagesClient {

    /**
     * The server applies the byte limit to the equivalent binary selection and
     * then encodes it as JSON, which is larger. Rust bounds the JSON body at the
     * same multiple.
     */
    private static final long JSON_BODY_EXPANSION = 16;

    private static final long INITIAL_RETRY_INTERVAL_MILLIS = 50;
    private static final long MAX_RETRY_INTERVAL_MILLIS = 1000;
    private static final long NANOS_PER_MICRO = 1000;

    private final InternalHttpClient httpClient;

    public MessagesHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
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
            // The poll query has no consumer kind field, so the server would read
            // a group id as a plain consumer and poll the wrong thing.
            throw new IggyOperationNotSupportedException(
                    "A consumer group cannot poll over HTTP; the poll query carries no consumer kind");
        }
        long startNanos = System.nanoTime();
        long waitDeadlineNanos = startNanos + options.maxWait().toNanos();
        long requestDeadlineNanos = startNanos + options.requestTimeout().toNanos();
        long maxBodyBytes = Math.multiplyExact(options.maxBytes(), JSON_BODY_EXPANSION);
        long retryIntervalMillis = INITIAL_RETRY_INTERVAL_MILLIS;
        while (true) {
            ClassicHttpRequest request = deferredRequest(
                    streamId,
                    topicId,
                    partitionId,
                    consumer,
                    strategy,
                    count,
                    autoCommit,
                    options,
                    waitDeadlineNanos,
                    requestDeadlineNanos);
            try {
                return httpClient.executeDeferred(request, PolledMessages.class, requestDeadlineNanos, maxBodyBytes);
            } catch (IggyServerException refused) {
                // Only an explicit non-admission proves nothing was served. An
                // opaque 503 can be an ambiguous not-committed or a proxy fault.
                if (refused.getRawErrorCode() != IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode()) {
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
            DeferredPollOptions options,
            long waitDeadlineNanos,
            long requestDeadlineNanos) {
        // Both budgets carry across retries rather than restarting.
        long now = System.nanoTime();
        long requestTimeoutMicros = Math.max(0, requestDeadlineNanos - now) / NANOS_PER_MICRO;
        if (requestTimeoutMicros == 0) {
            throw new IggyTimeoutException("Deferred poll exceeded its request timeout");
        }
        long waitMicros = Math.max(0, waitDeadlineNanos - now) / NANOS_PER_MICRO;
        return httpClient.prepareGetRequest(
                path(streamId, topicId) + "/deferred",
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                partitionId
                        .map(id -> new BasicNameValuePair("partition_id", id.toString()))
                        .orElse(null),
                new BasicNameValuePair("kind", strategy.kind().name().toLowerCase()),
                new BasicNameValuePair("value", strategy.value().toString()),
                new BasicNameValuePair("count", count.toString()),
                new BasicNameValuePair("auto_commit", Boolean.toString(autoCommit)),
                new BasicNameValuePair("wait_us", Long.toString(waitMicros)),
                new BasicNameValuePair("min_count", Long.toString(options.minCount())),
                new BasicNameValuePair("max_bytes", Long.toString(options.maxBytes())),
                new BasicNameValuePair("request_timeout_us", Long.toString(requestTimeoutMicros)));
    }

    private static void sleepBeforeRetry(long intervalMillis, long requestDeadlineNanos, IggyServerException refused) {
        if (requestDeadlineNanos - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(intervalMillis)) {
            throw refused;
        }
        try {
            Thread.sleep(intervalMillis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IggyClientException("Interrupted while retrying a deferred poll", interrupted);
        }
    }

    @Override
    public SendMessagesResponse sendMessages(
            StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        var request = httpClient.preparePostRequest(path(streamId, topicId), new SendMessages(partitioning, messages));
        var body = httpClient.executeWithStringResponse(request);
        if (body.isBlank()) {
            return SendMessagesResponse.empty();
        }
        return ObjectMapperFactory.getInstance().readValue(body, SendMessagesResponse.class);
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/messages";
    }

    private record SendMessages(Partitioning partitioning, List<Message> messages) {}
}
