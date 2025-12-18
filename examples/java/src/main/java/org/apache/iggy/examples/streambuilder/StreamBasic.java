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

package org.apache.iggy.examples.streambuilder;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public final class StreamBasic {

    private static final Logger log = LoggerFactory.getLogger(StreamBasic.class);

    private static final String STREAM_NAME = "test_stream";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);

    private static final String TOPIC_NAME = "test_topic";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);

    private static final long PARTITION_ID = 0L;
    private static final long POLL_BATCH_SIZE = 50L;
    private static final int EXPECTED_MESSAGES = 3;

    private StreamBasic() {}

    public static void main(String[] args) {
        log.info("Build iggy client and connect it.");
        var client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .build();

        try {
            ensureStreamAndTopic(client);

            log.info("Build iggy producer & consumer");
            log.info("Send 3 test messages...");
            sendMessage(client, "Hello World");
            sendMessage(client, "Hola Iggy");
            sendMessage(client, "Hi Apache");

            log.info("Consume the messages");
            consumeMessages(client);

            log.info("Stop the message stream and shutdown iggy client");
        } finally {
            deleteStreamIfExists(client);
        }
    }

    private static void consumeMessages(IggyTcpClient client) {
        BigInteger offset = BigInteger.ZERO;
        Consumer consumer = Consumer.of(0L);
        int consumedMessages = 0;

        while (consumedMessages < EXPECTED_MESSAGES) {
            PolledMessages polledMessages = client.messages()
                    .pollMessages(
                            STREAM_ID,
                            TOPIC_ID,
                            Optional.of(PARTITION_ID),
                            consumer,
                            PollingStrategy.offset(offset),
                            POLL_BATCH_SIZE,
                            false);

            for (Message message : polledMessages.messages()) {
                String payload = new String(message.payload(), StandardCharsets.UTF_8);
                log.info(
                        "Message received: {} at offset: {} in partition ID: {}",
                        payload,
                        message.header().offset(),
                        polledMessages.partitionId());

                consumedMessages++;
                if (consumedMessages == EXPECTED_MESSAGES) {
                    break;
                }
            }

            offset = offset.add(BigInteger.valueOf(polledMessages.messages().size()));
        }
    }

    private static void ensureStreamAndTopic(IggyTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            log.info("Stream {} already exists.", STREAM_NAME);
        } else {
            client.streams().createStream(STREAM_NAME);
            log.info("Stream {} was created.", STREAM_NAME);
        }

        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            log.info("Topic {} already exists.", TOPIC_NAME);
        } else {
            client.topics()
                    .createTopic(
                            STREAM_ID,
                            1L,
                            CompressionAlgorithm.None,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            Optional.empty(),
                            TOPIC_NAME);
            log.info("Topic {} was created.", TOPIC_NAME);
        }
    }

    private static void sendMessage(IggyTcpClient client, String payload) {
        client.messages()
                .sendMessages(
                        STREAM_ID, TOPIC_ID, Partitioning.partitionId(PARTITION_ID), List.of(Message.of(payload)));
    }

    private static void deleteStreamIfExists(IggyTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isEmpty()) {
            log.info("Stream {} already removed.", STREAM_NAME);
            return;
        }

        try {
            client.streams().deleteStream(STREAM_ID);
            log.info("Stream {} deleted.", STREAM_NAME);
        } catch (Exception e) {
            log.warn("Failed to delete stream {}: {}", STREAM_NAME, e.getMessage());
        }
    }
}
