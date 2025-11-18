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
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static java.util.Optional.empty;
import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TopicsClientBaseTest extends IntegrationTest {

    protected TopicsClient topicsClient;

    @BeforeEach
    void beforeEachBase() {
        topicsClient = client.topics();

        login();
        setUpStream();
    }

    @Test
    void shouldCreateAndDeleteTopic() {
        // when
        var topicDetails = topicsClient.createTopic(
                STREAM_NAME, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, empty(), "test-topic");
        var topicId = TopicId.of("test-topic");
        var topicOptional = topicsClient.getTopic(STREAM_NAME, topicId);

        // then
        assertThat(topicOptional).isPresent();
        var topic = topicOptional.get();
        assertThat(topic.name()).isEqualTo("test-topic");

        // when
        topicsClient.deleteTopic(STREAM_NAME, topicId);

        // then
        assertThat(topicsClient.getTopics(STREAM_NAME)).isEmpty();
    }

    @Test
    void shouldUpdateTopic() {
        // given
        var topic = topicsClient.createTopic(
                STREAM_NAME, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, empty(), "test-topic");

        // when
        topicsClient.updateTopic(
                STREAM_NAME,
                TopicId.of(topic.id()),
                CompressionAlgorithm.None,
                BigInteger.valueOf(5000),
                BigInteger.ZERO,
                empty(),
                "new-name");

        // then
        var updatedTopic =
                topicsClient.getTopic(STREAM_NAME, TopicId.of(topic.id())).get();
        assertThat(updatedTopic.name()).isEqualTo("new-name");
        assertThat(updatedTopic.messageExpiry()).isEqualTo(BigInteger.valueOf(5000));
    }

    @Test
    void shouldReturnEmptyForNonExistingTopic() {
        // when
        var topic = topicsClient.getTopic(STREAM_NAME, TopicId.of(404L));

        // then
        assertThat(topic).isEmpty();
    }
}
