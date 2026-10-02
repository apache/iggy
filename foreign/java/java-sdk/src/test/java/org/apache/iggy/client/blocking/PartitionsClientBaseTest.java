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

import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
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
    void shouldDeleteSegments() {
        // given
        var topicId = TopicId.of("segments-topic");
        topicsClient.createTopic(
                STREAM_NAME,
                1L,
                org.apache.iggy.topic.CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                "segments-topic",
                TopicOptions.builder()
                        .segmentSize(BigInteger.valueOf(1024 * 1024))
                        .messagesRequiredToSave(1)
                        .build());
        var message = Message.of("a".repeat(220_000));
        for (int count = 0; count < 5; count++) {
            client.messages().sendMessages(STREAM_NAME, topicId, Partitioning.partitionId(0L), List.of(message));
        }

        var topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.partitions().get(0).segmentsCount()).isEqualTo(2L);

        // when
        partitionsClient.deleteSegments(STREAM_NAME, topicId, 0L, 1L);

        // then
        topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.partitions().get(0).segmentsCount()).isEqualTo(1L);
    }
}
