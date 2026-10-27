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
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Optional.empty;
import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class MessagesClientBaseTest extends IntegrationTest {

    private static final Duration READINESS_WAIT = Duration.ofSeconds(4);
    private static final Duration SHORT_READINESS_WAIT = Duration.ofMillis(400);
    private static final Duration SEND_DELAY = Duration.ofMillis(200);
    private static final int BYTE_BOUND_PAYLOAD_SIZE = 400;

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

    @Test
    void shouldReturnSendConfirmations() {
        // given
        setUpStreamAndTopic();

        // when
        var firstResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("first")));
        var secondResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("second")));

        // then
        assertThat(firstResponse.confirmations()).hasSize(1);
        var firstConfirmation = firstResponse.confirmations().get(0);
        assertThat(firstConfirmation.partitionId()).isEqualTo(0L);
        assertThat(firstConfirmation.baseOffset()).isEqualTo(BigInteger.ZERO);
        assertThat(secondResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations().get(0).baseOffset()).isEqualTo(BigInteger.ONE);
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
    void shouldWakeADeferredPollWhenAMessageArrives() {
        // given
        setUpStreamAndTopic();
        var options = DeferredPollOptions.defaults().withMaxWait(READINESS_WAIT);
        CompletableFuture<Void> sender = CompletableFuture.runAsync(() -> {
            sleep(SEND_DELAY);
            messagesClient.sendMessages(
                    STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("late arrival")));
        });

        // when
        long startedNanos = System.nanoTime();
        PolledMessages polled = messagesClient.pollMessagesDeferred(
                STREAM_NAME,
                TOPIC_NAME,
                Optional.of(0L),
                Consumer.of(0L),
                PollingStrategy.first(),
                10L,
                false,
                options);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedNanos);
        sender.join();

        // then
        assertThat(polled.messages()).hasSize(1);
        assertThat(new String(polled.messages().get(0).payload())).isEqualTo("late arrival");
        assertThat(elapsed)
                .as("the poll woke on the message, not on the readiness deadline")
                .isLessThan(READINESS_WAIT);
    }

    @Test
    void shouldReturnEmptyWhenTheReadinessWaitExpires() {
        // given
        setUpStreamAndTopic();
        var options = DeferredPollOptions.defaults().withMaxWait(SHORT_READINESS_WAIT);

        // when
        long startedNanos = System.nanoTime();
        PolledMessages polled = messagesClient.pollMessagesDeferred(
                STREAM_NAME,
                TOPIC_NAME,
                Optional.of(0L),
                Consumer.of(0L),
                PollingStrategy.first(),
                10L,
                false,
                options);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedNanos);

        // then
        assertThat(polled.messages()).isEmpty();
        assertThat(elapsed).isGreaterThanOrEqualTo(SHORT_READINESS_WAIT.dividedBy(2));
    }

    @Test
    void shouldReturnAPartialBatchWhenTheReadinessTargetIsNotMet() {
        // given
        setUpStreamAndTopic();
        messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("only one")));
        var options =
                DeferredPollOptions.defaults().withMaxWait(SHORT_READINESS_WAIT).withMinCount(3);

        // when
        PolledMessages polled = messagesClient.pollMessagesDeferred(
                STREAM_NAME,
                TOPIC_NAME,
                Optional.of(0L),
                Consumer.of(0L),
                PollingStrategy.first(),
                10L,
                false,
                options);

        // then
        assertThat(polled.messages()).hasSize(1);
    }

    @Test
    void shouldReturnADeferredBatchBoundedByBytes() {
        // given
        setUpStreamAndTopic();
        String payload = "x".repeat(BYTE_BOUND_PAYLOAD_SIZE);
        messagesClient.sendMessages(
                STREAM_NAME,
                TOPIC_NAME,
                Partitioning.partitionId(0L),
                List.of(Message.of(payload), Message.of(payload), Message.of(payload)));
        // Room for a message and its framing, far short of all three.
        var options = DeferredPollOptions.defaults()
                .withMaxWait(SHORT_READINESS_WAIT)
                .withMaxBytes(2L * BYTE_BOUND_PAYLOAD_SIZE);

        // when
        PolledMessages polled = messagesClient.pollMessagesDeferred(
                STREAM_NAME,
                TOPIC_NAME,
                Optional.of(0L),
                Consumer.of(0L),
                PollingStrategy.first(),
                10L,
                false,
                options);

        // then
        assertThat(polled.messages()).hasSizeBetween(1, 2);
        assertThat(new String(polled.messages().get(0).payload())).isEqualTo(payload);
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
