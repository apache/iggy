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

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamOptionKey;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ResourceOptionsSteps {

    private IggyTcpClient client;
    private Long streamId;
    private Long topicId;
    private Long userId;
    private String streamName;
    private byte[] streamKey;
    private boolean updateRejected;

    @After
    public void close() {
        if (client == null) {
            return;
        }
        if (topicId != null && streamId != null) {
            client.topics().deleteTopic(streamId, topicId);
        }
        if (streamId != null) {
            client.streams().deleteStream(streamId);
        }
        if (userId != null) {
            client.users().deleteUser(userId);
        }
        client.close();
    }

    @When("I create a stream named {string} with an encryption key of 32 bytes valued {int}")
    public void createKeyedStream(String name, int value) {
        streamKey = new byte[32];
        Arrays.fill(streamKey, (byte) value);
        var created = client().streams()
                .createStream(
                        name, StreamOptions.builder().encryptionKey(streamKey).build());
        streamId = created.id();
        streamName = name;
    }

    @Given("a stream named {string} with an encryption key of 32 bytes valued {int} exists")
    public void keyedStreamExists(String name, int value) {
        createKeyedStream(name, value);
    }

    @When("I create a stream named {string} without an encryption key")
    public void createKeylessStream(String name) {
        var created = client().streams().createStream(name);
        streamId = created.id();
        streamName = name;
    }

    @Given("a stream named {string} exists")
    public void streamExists(String name) {
        createKeylessStream(name);
    }

    @Then("the stream should report encryption key source {string}")
    public void streamSource(String source) {
        assertEquals(
                EncryptionKeySource.valueOf(source.toUpperCase()),
                stream().encryptionKeySource().orElseThrow());
    }

    @Then("the stream encryption key fingerprint should match the key")
    public void streamFingerprintMatches() {
        assertNotNull(streamKey);
        assertEquals(
                EncryptionKeys.fingerprint(streamKey),
                stream().encryptionKeyFingerprint().orElseThrow());
    }

    @Then("the stream should report an encryption key fingerprint")
    public void streamFingerprintExists() {
        assertTrue(stream().encryptionKeyFingerprint().orElseThrow().matches("[0-9a-f]{16}"));
    }

    @Then("the stream should report at least one KEK fingerprint")
    public void kekFingerprintExists() {
        List<String> fingerprints = stream().encryptionKekFingerprints();
        assertFalse(fingerprints.isEmpty());
        assertTrue(fingerprints.stream().allMatch(value -> value.matches("[0-9a-f]{16}")));
    }

    @Then("the stream should expose no encryption key material")
    public void streamDoesNotExposeKey() {
        var details = stream();
        for (Map<String, HeaderValue> options : List.of(details.options(), details.derivedOptions())) {
            for (String key : List.of("encryption_key", "encryption_key_wrapped", "critical_keys")) {
                assertFalse(options.containsKey(key));
            }
        }
    }

    @When("I create a topic named {string} with {int} partition in the stream")
    public void createTopic(String name, int partitions) {
        var created = client().topics().createTopic(StreamId.of(requireStreamId()), name, partitions);
        topicId = created.id();
    }

    @When("I send a message with payload {string} to the topic")
    public void sendPayload(String payload) {
        client().messages()
                .sendMessages(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        Partitioning.partitionId(0L),
                        List.of(Message.of(payload)));
    }

    @When("I send a message with payload {string} and header {string} set to {string} to the topic")
    public void sendPayloadWithHeader(String payload, String key, String value) {
        client().messages()
                .sendMessages(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        Partitioning.partitionId(0L),
                        List.of(Message.of(payload, Map.of(HeaderKey.fromString(key), HeaderValue.fromString(value)))));
    }

    @Then("polling the topic should return the payload {string}")
    public void pollPayload(String expected) {
        var messages = poll();
        assertEquals(1, messages.size());
        assertEquals(expected, new String(messages.get(0).payload(), StandardCharsets.UTF_8));
    }

    @Then("polling the topic should return the payload {string} and header {string} set to {string}")
    public void pollPayloadWithHeader(String expected, String key, String value) {
        var messages = poll();
        assertEquals(1, messages.size());
        assertEquals(expected, new String(messages.get(0).payload(), StandardCharsets.UTF_8));
        assertEquals(
                HeaderValue.fromString(value), messages.get(0).userHeaders().get(HeaderKey.fromString(key)));
    }

    @When("I rename the stream to {string}")
    public void renameStream(String name) {
        client().streams()
                .updateStream(
                        StreamId.of(requireStreamId()),
                        StreamUpdate.builder().name(name).build());
        streamName = name;
    }

    @Then("the stream name should be {string}")
    public void streamName(String name) {
        assertEquals(name, stream().name());
    }

    @When("I request a reset of the stream option {string}")
    public void resetStreamOption(String key) {
        try {
            var update = StreamUpdate.builder()
                    .reset(StreamOptionKey.valueOf(key.toUpperCase()))
                    .build();
            client().streams().updateStream(StreamId.of(requireStreamId()), update);
        } catch (RuntimeException rejected) {
            updateRejected = true;
        }
    }

    @Then("the stream update should be rejected")
    public void streamUpdateRejected() {
        assertTrue(updateRejected);
    }

    @When("I describe the stream options")
    public void describeStreamOptions() {
        client().system().describeOptions(OptionsScope.Stream);
    }

    @Then("the stream option {string} should be updatable and not secret")
    public void updatableStreamOption(String key) {
        OptionSpec option = streamOption(key);
        assertTrue(option.updatable());
        assertFalse(option.secret());
    }

    @Then("the stream option {string} should be create only and secret")
    public void secretStreamOption(String key) {
        OptionSpec option = streamOption(key);
        assertFalse(option.updatable());
        assertTrue(option.secret());
    }

    @Then("the stream option {string} should be read only")
    public void readOnlyStreamOption(String key) {
        assertTrue(streamOption(key).readOnly());
    }

    @Given(
            "a topic named {string} exists in the stream with message expiry {long} microseconds and max topic size {long} bytes")
    public void topicWithOptions(String name, long expiry, long maxSize) {
        var options = TopicOptions.builder()
                .messageExpiry(BigInteger.valueOf(expiry))
                .maxTopicSize(BigInteger.valueOf(maxSize))
                .build();
        var created = client().topics().createTopic(StreamId.of(requireStreamId()), name, 1L, options);
        topicId = created.id();
    }

    @When("I update the topic message expiry to {long} microseconds without a name")
    public void updateTopicExpiry(long expiry) {
        client().topics()
                .updateTopic(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        TopicUpdate.builder()
                                .messageExpiry(BigInteger.valueOf(expiry))
                                .build());
    }

    @When("I reset the topic option {string}")
    public void resetTopicOption(String key) {
        client().topics()
                .updateTopic(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        TopicUpdate.builder()
                                .reset(TopicOptionKey.valueOf(key.toUpperCase()))
                                .build());
    }

    @When("I rename the topic to {string}")
    public void renameTopic(String name) {
        client().topics()
                .updateTopic(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        TopicUpdate.builder().name(name).build());
    }

    @Then("the topic name should be {string}")
    public void topicName(String name) {
        assertEquals(name, topic().name());
    }

    @Then("the topic message expiry should be {long} microseconds set explicitly")
    public void topicExpiry(long expiry) {
        assertEquals(
                HeaderValue.fromUint64(BigInteger.valueOf(expiry)),
                topic().options().get("message_expiry"));
    }

    @Then("the topic max topic size should be {long} bytes set explicitly")
    public void topicMaxSize(long bytes) {
        assertEquals(
                HeaderValue.fromUint64(BigInteger.valueOf(bytes)),
                topic().options().get("max_topic_size"));
    }

    @Then("the topic max topic size should be derived from the server default")
    public void defaultTopicMaxSize() {
        assertEquals(
                BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE),
                topic().derivedOptions().get("max_topic_size").asUint64());
    }

    @Given("a user named {string} with password {string} exists")
    public void createUser(String name, String password) {
        var created =
                client().users().createUser(name, password, UserStatus.Active, Optional.empty(), UserOptions.empty());
        userId = created.id();
    }

    @When("I update the user username to {string} and status to {string}")
    public void updateUser(String name, String status) {
        client().users()
                .updateUser(
                        UserId.of(requireUserId()),
                        UserUpdate.builder()
                                .username(name)
                                .status(UserStatus.valueOf(
                                        status.substring(0, 1).toUpperCase() + status.substring(1)))
                                .build());
    }

    @Then("the user should have username {string}")
    public void userName(String name) {
        assertEquals(
                name, client().users().getUser(requireUserId()).orElseThrow().username());
    }

    @Then("the user should have status {string}")
    public void userStatus(String status) {
        assertEquals(
                status,
                client().users()
                        .getUser(requireUserId())
                        .orElseThrow()
                        .status()
                        .name()
                        .toLowerCase());
    }

    private IggyTcpClient client() {
        if (client == null) {
            String[] address = TestEnvironment.serverAddress().split(":", 2);
            client = IggyTcpClient.builder()
                    .host(address[0])
                    .port(Integer.parseInt(address[1]))
                    .build();
            client.connect();
            client.users().login(TestEnvironment.rootUsername(), TestEnvironment.rootPassword());
        }
        return client;
    }

    private StreamDetails stream() {
        return client().streams().getStream(requireStreamId()).orElseThrow();
    }

    private TopicDetails topic() {
        return client().topics()
                .getTopic(StreamId.of(requireStreamId()), TopicId.of(requireTopicId()))
                .orElseThrow();
    }

    private OptionSpec streamOption(String key) {
        return client().system().describeOptions(OptionsScope.Stream).stream()
                .filter(option -> option.key().equals(key))
                .findFirst()
                .orElseThrow();
    }

    private List<Message> poll() {
        return client().messages()
                .pollMessages(
                        StreamId.of(requireStreamId()),
                        TopicId.of(requireTopicId()),
                        Optional.of(0L),
                        Consumer.of(0L),
                        PollingStrategy.offset(BigInteger.ZERO),
                        10L,
                        false)
                .messages();
    }

    private Long requireStreamId() {
        assertNotNull(streamId);
        return streamId;
    }

    private Long requireTopicId() {
        assertNotNull(topicId);
        return topicId;
    }

    private Long requireUserId() {
        assertNotNull(userId);
        return userId;
    }
}
