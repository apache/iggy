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

package org.apache.iggy.examples.gettingstarted.producer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;

public final class GettingStartedProducer {

    private static final String STREAM_NAME = "sample-stream";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);

    private static final String TOPIC_NAME = "sample-topic";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);

    private static final long PARTITION_ID = 0L;

    private static final int BATCHES_LIMIT = 5;

    private static final int MESSAGES_PER_BATCH = 10;
    private static final long INTERVAL_MS = 500;

    private static final Logger log = LoggerFactory.getLogger(GettingStartedProducer.class);

    private GettingStartedProducer() {}

    public static void main(String[] args) {
        var client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .buildAndLogin();

        createStream(client);
        createTopic(client);
        produceMessages(client);
    }

    private static void produceMessages(IggyTcpClient client) {
        log.info(
                "Messages will be sent to stream: {}, topic: {}, partition: {} with interval {}ms.",
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID,
                INTERVAL_MS);

        int currentId = 0;
        int sentBatches = 0;

        Partitioning partitioning = Partitioning.partitionId(PARTITION_ID);

        while (sentBatches < BATCHES_LIMIT) {
            try {
                Thread.sleep(INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            List<Message> messages = new ArrayList<>();
            for (int i = 0; i < MESSAGES_PER_BATCH; i++) {
                currentId++;
                String payload = "message-" + currentId;
                messages.add(Message.of(payload));
            }

            client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, messages);
            sentBatches++;
            log.info("Sent {} message(s).", MESSAGES_PER_BATCH);
        }

        log.info("Sent {} batches of messages, exiting.", sentBatches);
    }

    private static void createStream(IggyTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            return;
        }
        client.streams().createStream(STREAM_NAME);
        log.info("Stream {} was created.", STREAM_NAME);
    }

    private static void createTopic(IggyTcpClient client) {
        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            log.warn("Topic already exists and will not be created again.");
            return;
        }
        client.topics()
                .createTopic(
                        STREAM_ID,
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        TOPIC_NAME);
        log.info("Topic {} was created.", TOPIC_NAME);
    }
}
