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

package org.apache.iggy.async;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Optional.empty;

/**
 * Example demonstrating the true async Netty-based client.
 */
public final class AsyncConsumerExample {

    private static final String STREAM_NAME = "async-test";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);
    private static final String TOPIC_NAME = "events";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);
    private static final String GROUP_NAME = "async-consumer";
    private static final ConsumerId GROUP_ID = ConsumerId.of(GROUP_NAME);
    private static final Logger log = LoggerFactory.getLogger(AsyncConsumerExample.class);

    private AsyncConsumerExample() {}

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        // First, setup the stream/topic/group using blocking client
        setupWithBlockingClient();

        // Now test the async client
        testAsyncClient();
    }

    private static void setupWithBlockingClient() {
        log.info("Setting up stream, topic, and consumer group...");

        var blockingClient = new IggyTcpClient("localhost", 8090);
        blockingClient.users().login("iggy", "iggy");

        // Create stream if needed
        Optional<StreamDetails> stream = blockingClient.streams().getStream(STREAM_ID);
        if (!stream.isPresent()) {
            blockingClient.streams().createStream(STREAM_NAME);
            log.info("Created stream: {}", STREAM_NAME);
        }

        // Create topic if needed
        Optional<TopicDetails> topic = blockingClient.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (!topic.isPresent()) {
            blockingClient
                    .topics()
                    .createTopic(
                            STREAM_ID,
                            1L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            empty(),
                            TOPIC_NAME);
            log.info("Created topic: {}", TOPIC_NAME);
        }

        // Create consumer group if needed
        Optional<ConsumerGroupDetails> group =
                blockingClient.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);
        if (!group.isPresent()) {
            blockingClient.consumerGroups().createConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_NAME);
            log.info("Created consumer group: {}", GROUP_NAME);
        }

        // Join the consumer group
        blockingClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);
        log.info("Joined consumer group");
    }

    private static void testAsyncClient() throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Testing async client with Netty...");

        // Create async client
        AsyncIggyTcpClient asyncClient = new AsyncIggyTcpClient("localhost", 8090);

        // Connect asynchronously
        log.info("Connecting to server...");
        asyncClient
                .connect()
                .thenCompose(v -> {
                    log.info("Connected! Logging in...");
                    return asyncClient.users().loginAsync("iggy", "iggy");
                })
                .thenCompose(v -> {
                    log.info("Logged in! Joining consumer group...");
                    // Join the consumer group first
                    return asyncClient.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);
                })
                .thenCompose(v -> {
                    log.info("Joined consumer group! Now polling messages...");
                    return asyncClient
                            .messages()
                            .pollMessagesAsync(
                                    STREAM_ID,
                                    TOPIC_ID,
                                    Optional.empty(),
                                    Consumer.group(GROUP_ID),
                                    PollingStrategy.next(),
                                    10L,
                                    true);
                })
                .thenAccept(messages -> {
                    log.info("Received {} messages", messages.messages().size());
                    messages.messages().forEach(msg -> log.info("Message: {}", new String(msg.payload())));
                })
                .exceptionally(error -> {
                    log.error("Error in async operation", error);
                    return null;
                })
                .thenCompose(v -> {
                    log.info("Closing connection...");
                    return asyncClient.close();
                })
                .get(10, TimeUnit.SECONDS);

        log.info("Async test completed!");
    }
}
