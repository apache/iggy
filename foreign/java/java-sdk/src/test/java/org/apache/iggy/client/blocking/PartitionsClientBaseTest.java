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

package org.apache.iggy.client.blocking;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class PartitionsClientBaseTest extends IntegrationTest {

    // 5 messages of ~220KB exceed the 1MB segment, so they seal one segment and leave the
    // second one active; 4 messages (~880KB) would fit in a single segment.
    private static final int SEGMENT_SIZE_BYTES = 1024 * 1024;
    private static final int MESSAGE_SIZE_BYTES = 220_000;
    private static final int MESSAGES_PER_SEGMENT = 5;

    TopicsClient topicsClient;
    PartitionsClient partitionsClient;

    @BeforeEach
    void beforeEachBase() {
        topicsClient = client.topics();
        partitionsClient = client.partitions();

        login();
        setUpStreamAndTopic();
    }

    @Test
    void shouldCreateAndDeletePartitions() {
        // given
        assert topicsClient.getTopic(STREAM_NAME, TOPIC_NAME).get().partitionsCount() == 1L;

        // when
        partitionsClient.createPartitions(STREAM_NAME, TOPIC_NAME, 10L);

        // then
        TopicDetails topic = topicsClient.getTopic(STREAM_NAME, TOPIC_NAME).get();
        assertThat(topic.partitionsCount()).isEqualTo(11L);

        // when
        partitionsClient.deletePartitions(STREAM_NAME, TOPIC_NAME, 10L);

        // then
        topic = topicsClient.getTopic(STREAM_NAME, TOPIC_NAME).get();
        assertThat(topic.partitionsCount()).isEqualTo(1L);
    }

    @Test
    void shouldDeleteSegments() throws InterruptedException {
        // given
        var topicId = TopicId.of("segments-topic");
        topicsClient.createTopic(
                STREAM_NAME,
                1L,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                topicId.getName(),
                TopicOptions.builder()
                        .segmentSize(BigInteger.valueOf(SEGMENT_SIZE_BYTES))
                        .messagesRequiredToSave(1)
                        .build());
        var message = Message.of("a".repeat(MESSAGE_SIZE_BYTES));
        for (int count = 0; count < MESSAGES_PER_SEGMENT; count++) {
            client.messages().sendMessages(STREAM_NAME, topicId, Partitioning.partitionId(0L), List.of(message));
        }

        var topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.partitions().get(0).segmentsCount()).isEqualTo(2L);

        // when
        partitionsClient.deleteSegments(STREAM_NAME, topicId, 0L, 1L);

        // then: the server acks the metadata commit before the reconciler removes the segment,
        // so poll until the count drops
        topic = awaitSegmentsCount(STREAM_NAME, topicId, 1L);
        assertThat(topic.partitions().get(0).segmentsCount()).isEqualTo(1L);
    }

    private TopicDetails awaitSegmentsCount(StreamId streamId, TopicId topicId, long expected)
            throws InterruptedException {
        for (int attempt = 0; attempt < 50; attempt++) {
            var details = topicsClient.getTopic(streamId, topicId).orElseThrow();
            if (details.partitions().get(0).segmentsCount() == expected) {
                return details;
            }
            Thread.sleep(200);
        }
        return topicsClient.getTopic(streamId, topicId).orElseThrow();
    }
}
