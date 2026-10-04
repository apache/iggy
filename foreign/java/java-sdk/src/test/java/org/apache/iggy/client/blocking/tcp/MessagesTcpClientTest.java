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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClientBaseTest;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class MessagesTcpClientTest extends MessagesClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    /*
     * The TCP client resolves balanced and key-based partitioning to an
     * explicit partition id before encoding the frame (the VSR broker routes
     * explicit partitions only), so messages with the same key must land on
     * the same partition.
     */

    @Test
    void shouldRouteSameMessageKeyToSamePartition() {
        setUpStreamAndTopic();

        var firstResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("first")));
        var secondResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("second")));

        assertThat(firstResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations().get(0).partitionId())
                .isEqualTo(firstResponse.confirmations().get(0).partitionId());
    }

    /*
     * TCP only: the HTTP client does not percent-encode path segments yet, so
     * non-ASCII stream and topic names cannot be addressed over HTTP.
     */
    @Test
    void shouldRoundTripNonAsciiNamesKeyAndPayload() {
        // given
        var streamId = StreamId.of("strumień-世界");
        var topicId = TopicId.of("тема-日本語");
        var stream = client.streams().createStream(streamId.getName());
        trackStream(stream.id());
        client.topics()
                .createTopic(
                        streamId, 1L, CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO, topicId.getName());
        String text = "wiadomość 世界 😀";

        // when
        messagesClient.sendMessages(streamId, topicId, Partitioning.messagesKey("klucz-键"), List.of(Message.of(text)));
        var polledMessages = messagesClient.pollMessages(
                streamId, topicId, Optional.of(0L), Consumer.of(0L), PollingStrategy.first(), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
        assertThat(new String(polledMessages.messages().get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo(text);
    }
}
