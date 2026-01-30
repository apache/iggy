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

package org.apache.iggy.examples.messageheaders.consumer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.examples.shared.Messages;
import org.apache.iggy.examples.shared.Messages.OrderConfirmed;
import org.apache.iggy.examples.shared.Messages.OrderCreated;
import org.apache.iggy.examples.shared.Messages.OrderRejected;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public final class MessageHeadersConsumer {

    private static final String STREAM_NAME = "headers-stream";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);

    private static final String TOPIC_NAME = "orders";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);

    private static final long PARTITION_ID = 0L;
    private static final int BATCHES_LIMIT = 10;
    private static final long MESSAGES_PER_BATCH = 1L;

    private static final String MESSAGE_TYPE_HEADER = "message_type";

    private static final Logger log = LoggerFactory.getLogger(MessageHeadersConsumer.class);
    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    private MessageHeadersConsumer() {}

    public static void main(final String[] args) {
        var client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .buildAndLogin();

        consumeMessages(client);
    }

    private static void consumeMessages(IggyTcpClient client) {
        log.info(
                "Messages will be consumed from stream: {}, topic: {}, partition: {}.",
                STREAM_ID,
                TOPIC_ID,
                PARTITION_ID);

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
                    continue;
                }

                for (Message message : polledMessages.messages()) {
                    handleMessage(message);
                }

                consumedBatches++;

                offset = offset.add(BigInteger.valueOf(polledMessages.messages().size()));
            } catch (Exception e) {
                log.error("Error polling messages", e);
                break;
            }
        }
    }

    private static void handleMessage(Message message) {
        String payload = new String(message.payload(), StandardCharsets.UTF_8);
        String messageType = "unknown";

        try {
            Map<HeaderKey, HeaderValue> userHeaders = message.userHeaders();
            if (userHeaders.isEmpty()) {
                log.warn("Missing headers at offset {}.", message.header().offset());
                return;
            }

            HeaderValue headerValue = userHeaders.get(HeaderKey.fromString(MESSAGE_TYPE_HEADER));
            if (headerValue == null) {
                log.warn(
                        "Missing message type header at offset {}.",
                        message.header().offset());
                return;
            }

            messageType = headerValue.asString();
            log.info(
                    "Handling message type: {} at offset: {}...",
                    messageType,
                    message.header().offset());

            switch (messageType) {
                case Messages.ORDER_CREATED_TYPE -> {
                    OrderCreated order = MAPPER.readValue(payload, OrderCreated.class);
                    log.info("{}", order);
                }
                case Messages.ORDER_CONFIRMED_TYPE -> {
                    OrderConfirmed order = MAPPER.readValue(payload, OrderConfirmed.class);
                    log.info("{}", order);
                }
                case Messages.ORDER_REJECTED_TYPE -> {
                    OrderRejected order = MAPPER.readValue(payload, OrderRejected.class);
                    log.info("{}", order);
                }
                default -> log.warn("Received unknown message type: {}", messageType);
            }
        } catch (Exception e) {
            log.error(
                    "Failed to handle message type {} at offset {}",
                    messageType,
                    message.header().offset(),
                    e);
        }
    }
}
