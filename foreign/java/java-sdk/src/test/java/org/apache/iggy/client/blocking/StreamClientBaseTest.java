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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class StreamClientBaseTest extends IntegrationTest {

    private static final List<String> SECRET_KEYS =
            List.of("encryption_key", "encryption_key_wrapped", "critical_keys");

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
        streamsClient.updateStream(
                streamDetails.id(),
                StreamUpdate.builder().name("test-stream-new").build());

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
    void givenStreamKeyWhenCreatingShouldReadMatchingProjection() {
        // given
        byte[] key = randomKey();
        var options = StreamOptions.builder().encryptionKey(key).build();

        // when
        var created = streamsClient.createStream("keyed-stream", options);
        trackStream(created.id());
        var fetched = streamsClient.getStream(created.id()).orElseThrow();
        var listed = streamsClient.getStreams().stream()
                .filter(stream -> stream.id().equals(created.id()))
                .findFirst()
                .orElseThrow();

        // then
        String fingerprint = EncryptionKeys.fingerprint(key);
        for (StreamDetails stream : List.of(created, fetched)) {
            assertThat(stream.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
            assertThat(stream.encryptionKeyFingerprint()).contains(fingerprint);
            assertThat(stream.encryptionKekFingerprints()).isNotEmpty();
            assertNoKeyMaterial(key, stream.options(), stream.derivedOptions());
        }
        assertThat(listed.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
        assertThat(listed.encryptionKeyFingerprint()).contains(fingerprint);
        assertThat(listed.encryptionKekFingerprints()).isEqualTo(fetched.encryptionKekFingerprints());
        assertNoKeyMaterial(key, listed.options(), listed.derivedOptions());
    }

    @Test
    void givenKeylessStreamWhenReadingShouldReportServerGeneratedKey() {
        // when
        var created = streamsClient.createStream("keyless-stream");
        trackStream(created.id());
        var fetched = streamsClient.getStream(created.id()).orElseThrow();

        // then
        assertThat(fetched.encryptionKeySource()).contains(EncryptionKeySource.SERVER);
        assertThat(fetched.encryptionKeyFingerprint())
                .hasValueSatisfying(fingerprint -> assertThat(fingerprint).matches("[0-9a-f]{16}"));
        assertThat(fetched.encryptionKekFingerprints()).isNotEmpty();
        assertThat(created.encryptionKeyFingerprint()).isEqualTo(fetched.encryptionKeyFingerprint());
        assertThat(fetched.derivedOptions()).doesNotContainKeys(SECRET_KEYS.toArray(String[]::new));
    }

    @Test
    void givenStreamRenameWhenUpdatingShouldPreserveEncryptionProjection() {
        // given
        byte[] key = randomKey();
        var created = streamsClient.createStream(
                "rename-keyed-stream",
                StreamOptions.builder().encryptionKey(key).build());
        trackStream(created.id());
        var streamId = StreamId.of(created.id());
        client.topics().createTopic(streamId, "events", 1L);
        client.messages()
                .sendMessages(
                        streamId, TopicId.of("events"), Partitioning.partitionId(0L), List.of(Message.of("before")));
        var before = streamsClient.getStream(streamId).orElseThrow();

        // when
        streamsClient.updateStream(
                streamId, StreamUpdate.builder().name("renamed-keyed-stream").build());

        // then
        var after = streamsClient.getStream(created.id()).orElseThrow();
        assertThat(after.id()).isEqualTo(created.id());
        assertThat(after.name()).isEqualTo("renamed-keyed-stream");
        assertThat(after.encryptionKeySource()).isEqualTo(before.encryptionKeySource());
        assertThat(after.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
        assertThat(after.encryptionKekFingerprints()).isEqualTo(before.encryptionKekFingerprints());
        var polled = client.messages()
                .pollMessages(
                        streamId,
                        TopicId.of("events"),
                        Optional.of(0L),
                        Consumer.of(0L),
                        PollingStrategy.offset(BigInteger.ZERO),
                        1L,
                        false);
        assertThat(polled.messages()).hasSize(1);
        assertThat(new String(polled.messages().get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo("before");
    }

    @Test
    void givenStreamKeyResetWhenUpdatingShouldRejectBeforeSendingAndKeepState() {
        // given
        byte[] key = randomKey();
        var created = streamsClient.createStream(
                "reset-keyed-stream", StreamOptions.builder().encryptionKey(key).build());
        trackStream(created.id());

        // when
        var update = StreamUpdate.builder().reset(StreamOptionKey.ENCRYPTION_KEY);

        // then
        assertThatThrownBy(update::build)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.OPTION_NOT_UPDATABLE));
        var after = streamsClient.getStream(created.id()).orElseThrow();
        assertThat(after.name()).isEqualTo("reset-keyed-stream");
        assertThat(after.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
    }

    private static byte[] randomKey() {
        byte[] key = new byte[EncryptionKeys.KEY_LENGTH];
        new SecureRandom().nextBytes(key);
        return key;
    }

    private static void assertNoKeyMaterial(
            byte[] key, Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived) {
        String base64 = Base64.getEncoder().encodeToString(key);
        for (Map<String, HeaderValue> options : List.of(explicit, derived)) {
            assertThat(options).doesNotContainKeys(SECRET_KEYS.toArray(String[]::new));
            assertThat(options.values())
                    .noneMatch(value -> Arrays.equals(value.value(), key)
                            || value.toStringValue().contains(base64));
        }
    }
}
