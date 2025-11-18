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

import org.apache.iggy.topic.TopicDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
}
