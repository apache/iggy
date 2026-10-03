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

import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class StreamClientBaseTest extends IntegrationTest {

    StreamsClient streamsClient;

    @BeforeEach
    void beforeEachBase() {
        streamsClient = client.streams();

        login();
    }

    @Test
    void shouldCreateAndDeleteStream() {
        // when
        var streamDetails = streamsClient.createStream("test-stream");
        trackStream(streamDetails.id());
        var streamOptional = streamsClient.getStream(streamDetails.id());

        // then
        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.name()).isEqualTo("test-stream");

        // when
        var streams = streamsClient.getStreams();

        // then
        assertThat(streams).hasSize(1);

        // when
        streamsClient.deleteStream(streamDetails.id());
        createdStreamIds.remove(streamDetails.id()); // Remove from tracking since we deleted it
        streams = streamsClient.getStreams();

        // then
        assertThat(streams).isEmpty();
    }

    @Test
    void shouldUpdateStream() {
        // given
        var streamDetails = streamsClient.createStream("test-stream");
        trackStream(streamDetails.id());

        // when
        streamsClient.updateStream(streamDetails.id(), "test-stream-new");

        // then
        var streamOptional = streamsClient.getStream(streamDetails.id());

        assertThat(streamOptional).isPresent();
        var stream = streamOptional.get();
        assertThat(stream.name()).isEqualTo("test-stream-new");
    }

    @Test
    void shouldReturnEmptyForNonExistingStream() {
        // when
        var stream = streamsClient.getStream(333L);

        // then
        assertThat(stream).isEmpty();
    }

    @Test
    void shouldPurgeStream() throws InterruptedException {
        // given
        var streamDetails = streamsClient.createStream("test-stream");
        trackStream(streamDetails.id());
        var streamId = StreamId.of(streamDetails.id());
        var topicsClient = client.topics();
        var topicDetails = topicsClient.createTopic(
                streamId, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, "test-topic");
        var topicId = TopicId.of(topicDetails.id());
        var secondTopicDetails = topicsClient.createTopic(
                streamId, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, "test-topic-2");
        var secondTopicId = TopicId.of(secondTopicDetails.id());
        var messagesClient = client.messages();
        messagesClient.sendMessages(
                streamId, topicId, Partitioning.partitionId(0L), List.of(Message.of("message to purge")));
        messagesClient.sendMessages(
                streamId, secondTopicId, Partitioning.partitionId(0L), List.of(Message.of("message to purge")));

        // The sends are acknowledged before this runs, so the messages must already be visible.
        assertThat(messagesClient
                        .pollMessages(
                                streamId,
                                topicId,
                                Optional.empty(),
                                Consumer.of(0L),
                                new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                                10L,
                                false)
                        .messages())
                .hasSize(1);
        assertThat(messagesClient
                        .pollMessages(
                                streamId,
                                secondTopicId,
                                Optional.empty(),
                                Consumer.of(0L),
                                new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                                10L,
                                false)
                        .messages())
                .hasSize(1);

        // when
        streamsClient.purgeStream(streamDetails.id());

        // then — the stream and both topics remain, and every message is gone
        var streamOptional = streamsClient.getStream(streamDetails.id());
        assertThat(streamOptional).isPresent();
        assertThat(topicsClient.getTopic(streamId, topicId)).isPresent();
        assertThat(topicsClient.getTopic(streamId, secondTopicId)).isPresent();
        // The purge is acknowledged once the server accepts it, but the messages can still
        // be visible to a poll issued right after, so poll until both topics read empty.
        var deadline = System.currentTimeMillis() + 10_000;
        var purged = false;
        while (System.currentTimeMillis() < deadline) {
            if (messagesClient
                            .pollMessages(
                                    streamId,
                                    topicId,
                                    Optional.empty(),
                                    Consumer.of(0L),
                                    new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                                    10L,
                                    false)
                            .messages()
                            .isEmpty()
                    && messagesClient
                            .pollMessages(
                                    streamId,
                                    secondTopicId,
                                    Optional.empty(),
                                    Consumer.of(0L),
                                    new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                                    10L,
                                    false)
                            .messages()
                            .isEmpty()) {
                purged = true;
                break;
            }
            Thread.sleep(200);
        }
        assertThat(purged).as("messages are purged from the stream").isTrue();
    }

    @Test
    void shouldFailPurgeForNonExistingStream() {
        assertThatThrownBy(() -> streamsClient.purgeStream(999L)).isInstanceOf(RuntimeException.class);
    }
}
