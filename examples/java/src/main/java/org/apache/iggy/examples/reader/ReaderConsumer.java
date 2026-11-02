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

package org.apache.iggy.examples.reader;

import org.apache.iggy.client.async.reader.IggyConsumer;
import org.apache.iggy.client.async.reader.ReceivedMessage;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.PollOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Reads the messages of the getting started producer as a member of a consumer group. The consumer waits on the server
 * for new messages, and stores the offset of each message after its handler processed it.
 */
public final class ReaderConsumer {
    private static final Logger log = LoggerFactory.getLogger(ReaderConsumer.class);

    private static final StreamId STREAM_ID = StreamId.of("sample-stream");
    private static final TopicId TOPIC_ID = TopicId.of("sample-topic");
    private static final String GROUP_NAME = "sample-readers";
    private static final int MESSAGES_LIMIT = 100;

    private ReaderConsumer() {}

    public static void main(String[] args) throws Exception {
        AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                .host("localhost")
                .port(8090)
                .credentials("iggy", "iggy")
                .build();
        client.connect().join();
        client.login().join();
        ConsumerId group = ConsumerId.of(GROUP_NAME);
        if (client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, group)
                .join()
                .isEmpty()) {
            client.consumerGroups()
                    .createConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_NAME)
                    .join();
        }

        // Every 10,000 messages or 5 s, a message counts once its handler succeeded.
        PollOptions options = PollOptions.waitFor(Duration.ofSeconds(5)).withCommit(CommitPolicy.auto());
        IggyConsumer consumer = IggyConsumer.build(client.reader(STREAM_ID, TOPIC_ID, Consumer.group(group))
                        .options(options))
                .join();
        try {
            for (int handled = 0; handled < MESSAGES_LIMIT; handled++) {
                ReceivedMessage received = consumer.next().join();
                log.info(
                        "Partition {} offset {}: {}",
                        received.partitionId(),
                        received.checkpoint().offset(),
                        new String(received.message().payload(), StandardCharsets.UTF_8));
            }
        } finally {
            // Counts the last message, stores the counted offsets and leaves the group.
            consumer.shutdown().join();
            client.close().join();
        }
    }
}
