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

package org.apache.iggy.examples.messageheaders.producer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.examples.shared.Messages.SerializableMessage;
import org.apache.iggy.examples.shared.MessagesGenerator;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class MessageHeadersProducer {

    private static final String STREAM_NAME = "headers-stream";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);

    private static final String TOPIC_NAME = "orders";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);

    private static final long PARTITION_ID = 0L;
    private static final int BATCHES_LIMIT = 10;
    private static final int MESSAGES_PER_BATCH = 1;

    private static final String MESSAGE_TYPE_HEADER = "message_type";

    private static final Logger log = LoggerFactory.getLogger(MessageHeadersProducer.class);

    private MessageHeadersProducer() {}

    public static void main(String[] args) {
        var client = IggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .buildAndLogin();

        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            log.warn("Stream {} already exists and will not be created again.", STREAM_NAME);
        } else {
            client.streams().createStream(STREAM_NAME);
            log.info("Stream {} was created.", STREAM_NAME);
        }

        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            log.warn("Topic already exists and will not be created again.");
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

        produceMessages(client);
    }

    public static void produceMessages(IggyTcpClient client) {
        log.info(
                "Messages will be sent to stream: {}, topic: {}, partition: {}.",
                STREAM_NAME,
                TOPIC_NAME,
                PARTITION_ID);

        int sentBatches = 0;
        Partitioning partitioning = Partitioning.partitionId(PARTITION_ID);
        MessagesGenerator generator = new MessagesGenerator();

        while (sentBatches < BATCHES_LIMIT) {
            List<Message> messages = new ArrayList<>();
            List<SerializableMessage> serializableMessages = new ArrayList<>();

            for (int i = 0; i < MESSAGES_PER_BATCH; i++) {
                SerializableMessage serializableMessage = generator.generate();
                String messageType = serializableMessage.getMessageType();
                String json = serializableMessage.toJson();
                Map<HeaderKey, HeaderValue> userHeaders = new HashMap<>();
                userHeaders.put(HeaderKey.fromString(MESSAGE_TYPE_HEADER), HeaderValue.fromString(messageType));
                messages.add(Message.of(json, userHeaders));
                serializableMessages.add(serializableMessage);
            }

            client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, messages);
            sentBatches++;
            log.info("Sent messages: {}", serializableMessages);
        }
    }
}
