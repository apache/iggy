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

package org.apache.iggy.examples.gettingstarted.consumer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class GettingStartedConsumer {

    private static final StreamId STREAM_ID = StreamId.of("sample-stream");
    private static final TopicId TOPIC_ID = TopicId.of("sample-topic");

    private static final long PARTITION_ID = 0L;

    private static final int BATCHES_LIMIT = 5;

    private static final long MESSAGES_PER_BATCH = 10L;
    private static final long INTERVAL_MS = 500;

    private static final Logger log = LoggerFactory.getLogger(GettingStartedConsumer.class);

    private GettingStartedConsumer() {}

    public static void main(String[] args) {
        var client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .buildAndLogin();

        consumeMessages(client);
    }

    private static void consumeMessages(IggyTcpClient client) {
        log.info(
                "Messages will be consumed from stream: {}, topic: {}, partition: {} with interval {}ms.",
                STREAM_ID,
                TOPIC_ID,
                PARTITION_ID,
                INTERVAL_MS);

        BigInteger offset = BigInteger.ZERO;
        int consumedBatches = 0;

        Consumer consumer = Consumer.of(0L);

        while (true) {
            if (consumedBatches == BATCHES_LIMIT) {
                log.info("Consumed {} batches of messages, exiting.", consumedBatches);
                return;
            }

            try {
                PolledMessages polledMessages = client.messages()
                        .pollMessages(
                                STREAM_ID,
                                TOPIC_ID,
                                Optional.of(PARTITION_ID),
                                consumer,
                                PollingStrategy.offset(offset),
                                MESSAGES_PER_BATCH,
                                false);

                if (polledMessages.messages().isEmpty()) {
                    log.info("No messages found.");
                    Thread.sleep(INTERVAL_MS);
                    continue;
                }

                for (Message message : polledMessages.messages()) {
                    handleMessage(message, offset);
                }

                consumedBatches++;

                offset = offset.add(BigInteger.valueOf(polledMessages.messages().size()));

                Thread.sleep(INTERVAL_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error polling messages", e);
                break;
            }
        }
    }

    private static void handleMessage(Message message, BigInteger offset) {
        String payload = new String(message.payload(), StandardCharsets.UTF_8);
        log.info("Handling message at offset {}, payload: {}...", offset, payload);
    }
}
