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
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import static java.util.Optional.empty;
import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class MessagesClientBaseTest extends IntegrationTest {

    protected MessagesClient messagesClient;

    @BeforeEach
    void beforeEachBase() {
        messagesClient = client.messages();

        login();
    }

    @Test
    void shouldSendAndGetMessages() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of(text)));

        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME,
                TOPIC_NAME,
                empty(),
                Consumer.of(0L),
                new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                10L,
                false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }

    @Test
    void shouldPollMessagesWithFirstStrategy() {
        // given
        setUpStreamAndTopic();
        messagesClient.sendMessages(
                STREAM_NAME,
                TOPIC_NAME,
                Partitioning.partitionId(0L),
                List.of(Message.of("first"), Message.of("second"), Message.of("third")));

        // when
        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME, TOPIC_NAME, Optional.of(0L), Consumer.of(0L), PollingStrategy.first(), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(3);
        assertThat(new String(polledMessages.messages().get(0).payload())).isEqualTo("first");
    }

    @Test
    void shouldPollMessagesWithOffsetStrategy() {
        // given
        setUpStreamAndTopic();
        messagesClient.sendMessages(
                STREAM_NAME,
                TOPIC_NAME,
                Partitioning.partitionId(0L),
                List.of(Message.of("msg-0"), Message.of("msg-1"), Message.of("msg-2")));

        // when — poll starting from offset 1 (skip first message)
        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME,
                TOPIC_NAME,
                Optional.of(0L),
                Consumer.of(0L),
                PollingStrategy.offset(BigInteger.ONE),
                10L,
                false);

        // then
        assertThat(polledMessages.messages()).hasSize(2);
        assertThat(new String(polledMessages.messages().get(0).payload())).isEqualTo("msg-1");
        assertThat(new String(polledMessages.messages().get(1).payload())).isEqualTo("msg-2");
    }

    @Test
    void shouldPollMessagesWithLastStrategy() {
        // given
        setUpStreamAndTopic();
        messagesClient.sendMessages(
                STREAM_NAME,
                TOPIC_NAME,
                Partitioning.partitionId(0L),
                List.of(Message.of("msg-0"), Message.of("msg-1"), Message.of("msg-2")));

        // when
        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME, TOPIC_NAME, Optional.of(0L), Consumer.of(0L), PollingStrategy.last(), 1L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
        assertThat(new String(polledMessages.messages().get(0).payload())).isEqualTo("msg-2");
    }

    @Test
    void shouldVerifyMessageContentRoundTrip() {
        // given
        setUpStreamAndTopic();
        String content = "hello from java sdk – special chars: łóżko, 日本語, emoji 🎉";
        messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of(content)));

        // when
        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME, TOPIC_NAME, Optional.of(0L), Consumer.of(0L), PollingStrategy.first(), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
        assertThat(new String(polledMessages.messages().get(0).payload())).isEqualTo(content);
    }

    @Test
    void shouldSendMessageWithBalancedPartitioning() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(STREAM_NAME, TOPIC_NAME, Partitioning.balanced(), List.of(Message.of(text)));

        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME,
                TOPIC_NAME,
                empty(),
                Consumer.of(0L),
                new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                10L,
                false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }

    @Test
    void shouldSendMessageWithMessageKeyPartitioning() {
        // given
        setUpStreamAndTopic();

        // when
        String text = "message from java sdk";
        messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of(text)));
        var polledMessages = messagesClient.pollMessages(
                STREAM_NAME,
                TOPIC_NAME,
                empty(),
                Consumer.of(0L),
                new PollingStrategy(PollingKind.Last, BigInteger.TEN),
                10L,
                false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
    }
}
