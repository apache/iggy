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

package org.apache.iggy.bdd;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BasicMessagingSteps {

    private final TestContext context = new TestContext();

    @Given("I have a running Iggy server")
    public void runningServer() {
        context.serverAddr = getenvOrDefault("IGGY_TCP_ADDRESS", "127.0.0.1:8090");
        HostPort hostPort = HostPort.parse(context.serverAddr);

        IggyTcpClient client = IggyTcpClient.builder()
                .host(hostPort.host)
                .port(hostPort.port)
                .build();

        client.connect();
        client.system().ping();
        context.client = client;
    }

    @Given("I am authenticated as the root user")
    public void authenticatedRootUser() {
        String username = getenvOrDefault("IGGY_ROOT_USERNAME", "iggy");
        String password = getenvOrDefault("IGGY_ROOT_PASSWORD", "iggy");
        getClient().users().login(username, password);
    }

    @Given("I have no streams in the system")
    public void noStreamsInSystem() {
        List<StreamBase> streams = getClient().streams().getStreams();
        assertTrue(streams.isEmpty(), "System should have no streams initially");
    }

    @When("I create a stream with name {string}")
    public void createStream(String streamName) {
        StreamDetails stream = getClient().streams().createStream(streamName);
        context.lastStreamId = stream.id();
        context.lastStreamName = stream.name();
    }

    @Then("the stream should be created successfully")
    public void streamCreatedSuccessfully() {
        assertNotNull(context.lastStreamId, "Stream should have been created");
    }

    @Then("the stream should have name {string}")
    public void streamHasName(String streamName) {
        assertEquals(streamName, context.lastStreamName, "Stream should have expected name");
        Optional<StreamDetails> stream = getClient().streams().getStream(context.lastStreamId);
        assertTrue(stream.isPresent(), "Stream should exist");
        assertEquals(streamName, stream.get().name(), "Stream should have expected name");
    }

    @When("I create a topic with name {string} in stream {int} with {int} partitions")
    public void createTopic(String topicName, int streamId, int partitions) {
        TopicDetails topic = getClient().topics().createTopic(
                (long) streamId,
                (long) partitions,
                CompressionAlgorithm.None,
                BigInteger.ZERO,
                BigInteger.ZERO,
                Optional.empty(),
                topicName);

        context.lastTopicId = topic.id();
        context.lastTopicName = topic.name();
        context.lastTopicPartitions = topic.partitionsCount();
    }

    @Then("the topic should be created successfully")
    public void topicCreatedSuccessfully() {
        assertNotNull(context.lastTopicId, "Topic should have been created");
    }

    @Then("the topic should have name {string}")
    public void topicHasName(String topicName) {
        assertEquals(topicName, context.lastTopicName, "Topic should have expected name");
    }

    @Then("the topic should have {int} partitions")
    public void topicHasPartitions(int partitions) {
        assertEquals((long) partitions, context.lastTopicPartitions, "Topic should have expected partitions");
    }

    @When("I send {int} messages to stream {int}, topic {int}, partition {int}")
    public void sendMessages(int messageCount, int streamId, int topicId, int partitionId) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String content = "test message " + i;
            messages.add(Message.of(content));
        }

        getClient().messages().sendMessages(
                (long) streamId,
                (long) topicId,
                Partitioning.partitionId((long) partitionId),
                messages);

        if (!messages.isEmpty()) {
            context.lastSentMessage = "test message " + (messageCount - 1);
        }
    }

    @Then("all messages should be sent successfully")
    public void messagesSentSuccessfully() {
        assertNotNull(context.lastSentMessage, "Last sent message should be captured");
    }

    @When("I poll messages from stream {int}, topic {int}, partition {int} starting from offset {int}")
    public void pollMessages(int streamId, int topicId, int partitionId, int startOffset) {
        context.lastPolledMessages = getClient().messages().pollMessages(
                (long) streamId,
                (long) topicId,
                Optional.of((long) partitionId),
                0L,
                PollingStrategy.offset(BigInteger.valueOf(startOffset)),
                100L,
                true);
    }

    @Then("I should receive {int} messages")
    public void shouldReceiveMessages(int expectedCount) {
        assertNotNull(context.lastPolledMessages, "Should have polled messages");
        assertEquals(expectedCount, context.lastPolledMessages.messages().size(), "Message count should match");
    }

    @Then("the messages should have sequential offsets from {int} to {int}")
    public void messagesHaveSequentialOffsets(int startOffset, int endOffset) {
        assertNotNull(context.lastPolledMessages, "Should have polled messages");

        var messages = context.lastPolledMessages.messages();
        for (int i = 0; i < messages.size(); i++) {
            BigInteger expectedOffset = BigInteger.valueOf(startOffset + i);
            assertEquals(expectedOffset, messages.get(i).header().offset(), "Offsets should be sequential");
        }

        assertFalse(messages.isEmpty(), "Should have at least one message");
        BigInteger lastOffset = messages.get(messages.size() - 1).header().offset();
        assertEquals(BigInteger.valueOf(endOffset), lastOffset, "Last offset should match");
    }

    @Then("each message should have the expected payload content")
    public void messagesHaveExpectedPayload() {
        assertNotNull(context.lastPolledMessages, "Should have polled messages");

        var messages = context.lastPolledMessages.messages();
        for (int i = 0; i < messages.size(); i++) {
            String expectedPayload = "test message " + i;
            String actualPayload = new String(messages.get(i).payload(), StandardCharsets.UTF_8);
            assertEquals(expectedPayload, actualPayload, "Payload should match expected content");
        }
    }

    @Then("the last polled message should match the last sent message")
    public void lastPolledMessageMatchesSent() {
        assertNotNull(context.lastSentMessage, "Should have a sent message to compare");
        assertNotNull(context.lastPolledMessages, "Should have polled messages");

        var messages = context.lastPolledMessages.messages();
        assertFalse(messages.isEmpty(), "Should have at least one polled message");

        String lastPayload = new String(messages.get(messages.size() - 1).payload(), StandardCharsets.UTF_8);
        assertEquals(context.lastSentMessage, lastPayload, "Last message should match sent message");
    }

    private IggyBaseClient getClient() {
        if (context.client == null) {
            throw new IllegalStateException("Iggy client not initialized");
        }
        return context.client;
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static final class HostPort {
        private final String host;
        private final int port;

        private HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }

        private static HostPort parse(String address) {
            String[] parts = address.split(":", 2);
            String host = parts.length > 0 ? parts[0] : "127.0.0.1";
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8090;

            try {
                String resolved = InetAddress.getByName(host).getHostAddress();
                host = resolved;
            } catch (UnknownHostException ignored) {
                // Fall back to provided host if resolution fails.
            }

            return new HostPort(host, port);
        }
    }
}
