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

package org.apache.iggy.consumer;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Optional;

import static java.util.Optional.empty;

public final class SimpleConsumer {

    private static final String STREAM_NAME = "dev01";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);
    private static final String TOPIC_NAME = "events";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);
    private static final String GROUP_NAME = "simple-consumer";
    private static final ConsumerId GROUP_ID = ConsumerId.of(GROUP_NAME);
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    private SimpleConsumer() {}

    public static void main(String[] args) {
        var client = new IggyTcpClient("localhost", 8090);
        client.users().login("iggy", "iggy");

        createStream(client);
        createTopic(client);
        createConsumerGroup(client);
        client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);

        var messages = new ArrayList<Message>();
        while (messages.size() < 1000) {
            var polledMessages = client.messages()
                    .pollMessages(
                            STREAM_ID, TOPIC_ID, empty(), Consumer.group(GROUP_ID), PollingStrategy.next(), 10L, true);
            messages.addAll(polledMessages.messages());
            log.debug(
                    "Fetched {} messages from partition {}, current offset {}",
                    polledMessages.messages().size(),
                    polledMessages.partitionId(),
                    polledMessages.currentOffset());
        }
    }

    private static void createStream(IggyBaseClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            return;
        }
        client.streams().createStream(STREAM_NAME);
    }

    private static void createTopic(IggyBaseClient client) {
        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
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
    }

    private static void createConsumerGroup(IggyBaseClient client) {
        Optional<ConsumerGroupDetails> consumerGroup =
                client.consumerGroups().getConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);
        if (consumerGroup.isPresent()) {
            return;
        }
        client.consumerGroups().createConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_NAME);
    }
}
