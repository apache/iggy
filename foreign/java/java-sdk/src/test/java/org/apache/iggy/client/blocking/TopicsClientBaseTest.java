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

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class TopicsClientBaseTest extends IntegrationTest {

    private static final BigInteger HOUR_MICROS = BigInteger.valueOf(3_600_000_000L);
    private static final BigInteger GIB = BigInteger.valueOf(1_073_741_824L);
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

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
        var topicDetails = topicsClient.createTopic(STREAM_NAME, "test-topic", 1L);
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
        var topic = topicsClient.createTopic(STREAM_NAME, "test-topic", 1L);

        // when
        topicsClient.updateTopic(
                STREAM_NAME,
                TopicId.of(topic.id()),
                TopicUpdate.builder()
                        .name("new-name")
                        .messageExpiry(BigInteger.valueOf(5000))
                        .build());

        // then
        var updatedTopic =
                topicsClient.getTopic(STREAM_NAME, TopicId.of(topic.id())).get();
        assertThat(updatedTopic.name()).isEqualTo("new-name");
        assertThat(updatedTopic.messageExpiry()).contains(BigInteger.valueOf(5000));
    }

    @Test
    void shouldCarryOptionKeysWithNoParameterOfTheirOwn() {
        // when
        var created = topicsClient.createTopic(
                STREAM_NAME, "options-topic", 1L, Map.of("durability", HeaderValue.fromString("persisted")));

        // then
        var topic = topicsClient.getTopic(STREAM_NAME, TopicId.of(created.id())).orElseThrow();
        // Both transports preserve the canonical durability token.
        assertThat(topic.options()).containsKey("durability");
        assertThat(topic.options().get("durability").toStringValue()).isEqualTo("persisted");
        // Keys the client left alone are resolved by admission and reported apart.
        assertThat(topic.derivedOptions()).containsKey("max_topic_size");
        assertThat(topic.derivedOptions()).doesNotContainKey("durability");
    }

    @Test
    void shouldRejectAnOptionKeyOutsideTheCatalog() {
        assertThatThrownBy(() -> topicsClient.createTopic(
                        STREAM_NAME, "bad-options-topic", 1L, Map.of("not_a_real_option", HeaderValue.fromString("1"))))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void givenTopicResetWhenUpdatingShouldPreserveUnmentionedValues() {
        // given
        var created = topicsClient.createTopic(
                STREAM_NAME,
                "patched-topic",
                1L,
                TopicOptions.builder()
                        .compressionAlgorithm(CompressionAlgorithm.Gzip)
                        .messageExpiry(HOUR_MICROS)
                        .maxTopicSize(GIB)
                        .build());
        var topicId = TopicId.of(created.id());

        // when
        topicsClient.updateTopic(
                STREAM_NAME,
                topicId,
                TopicUpdate.builder()
                        .messageExpiry(HOUR_MICROS.shiftLeft(1))
                        .reset(TopicOptionKey.MAX_TOPIC_SIZE)
                        .build());

        // then
        var topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.name()).isEqualTo("patched-topic");
        assertThat(topic.options().get("message_expiry")).isEqualTo(HeaderValue.fromUint64(HOUR_MICROS.shiftLeft(1)));
        assertThat(topic.options()).doesNotContainKey("max_topic_size");
        assertThat(topic.derivedOptions().get("max_topic_size")).isEqualTo(HeaderValue.fromUint64(U64_MAX));
        assertThat(topic.options().get("compression_algorithm")).isEqualTo(HeaderValue.fromString("gzip"));
        assertThat(topic.compressionAlgorithm()).contains(CompressionAlgorithm.Gzip);
        assertThat(topic.maxTopicSize()).contains(U64_MAX);
    }

    @Test
    void givenTopicRenameWhenUpdatingShouldKeepSettings() {
        // given
        var created = topicsClient.createTopic(
                STREAM_NAME,
                "rename-topic",
                1L,
                TopicOptions.builder()
                        .messageExpiry(HOUR_MICROS)
                        .maxTopicSize(GIB)
                        .build());
        var topicId = TopicId.of(created.id());

        // when
        topicsClient.updateTopic(
                STREAM_NAME,
                topicId,
                TopicUpdate.builder().name("renamed-topic").build());

        // then
        var topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.name()).isEqualTo("renamed-topic");
        assertThat(topic.options().get("message_expiry")).isEqualTo(HeaderValue.fromUint64(HOUR_MICROS));
        assertThat(topic.options().get("max_topic_size")).isEqualTo(HeaderValue.fromUint64(GIB));
    }

    @Test
    void givenCreateOnlyTopicKeyWhenUpdatingShouldRefuseOnClientAndServer() {
        // given
        var created = topicsClient.createTopic(STREAM_NAME, "fixed-topic", 1L);
        var topicId = TopicId.of(created.id());

        // when
        var reset = TopicUpdate.builder().reset(TopicOptionKey.COMPRESSION_ALGORITHM);
        var rawSet = TopicUpdate.builder()
                .raw("compression_algorithm", HeaderValue.fromString("gzip"))
                .build();

        // then
        assertThatThrownBy(reset::build)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.OPTION_NOT_UPDATABLE));
        assertThatThrownBy(() -> topicsClient.updateTopic(STREAM_NAME, topicId, rawSet))
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.OPTION_NOT_UPDATABLE));
        var topic = topicsClient.getTopic(STREAM_NAME, topicId).orElseThrow();
        assertThat(topic.compressionAlgorithm()).contains(CompressionAlgorithm.None);
    }

    @Test
    void shouldReturnEmptyForNonExistingTopic() {
        // when
        var topic = topicsClient.getTopic(STREAM_NAME, TopicId.of(404L));

        // then
        assertThat(topic).isEmpty();
    }
}
