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

package org.apache.iggy.client.async;

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.TopicOptionKey;
import org.apache.iggy.topic.TopicOptions;
import org.apache.iggy.topic.TopicUpdate;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.apache.iggy.user.UserUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The shared resource options cases through the async TCP client. */
class ResourceOptionsTest extends BaseIntegrationTest {

    private static final int TIMEOUT_SECONDS = 5;
    private static final BigInteger HOUR_MICROS = BigInteger.valueOf(3_600_000_000L);
    private static final BigInteger GIB = BigInteger.valueOf(1_073_741_824L);
    private static final BigInteger U64_MAX = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private final List<Long> streams = new ArrayList<>();
    private final List<Long> users = new ArrayList<>();
    private AsyncIggyTcpClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = connect();
        await(client.users().login("iggy", "iggy"));
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Long stream : streams) {
            client.streams().deleteStream(stream).exceptionally(ignored -> null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        for (Long user : users) {
            client.users().deleteUser(user).exceptionally(ignored -> null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        client.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void givenStreamKeyWhenCreatingShouldReadMatchingProjection() throws Exception {
        // given
        byte[] key = randomKey();

        // when
        StreamDetails created = await(client.streams()
                .createStream(
                        unique("async-keyed"),
                        StreamOptions.builder().encryptionKey(key).build()));
        streams.add(created.id());
        StreamDetails fetched = await(client.streams().getStream(created.id())).orElseThrow();
        var listed = await(client.streams().getStreams()).stream()
                .filter(stream -> stream.id().equals(created.id()))
                .findFirst()
                .orElseThrow();

        // then
        String fingerprint = EncryptionKeys.fingerprint(key);
        assertThat(created.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
        assertThat(fetched.encryptionKeyFingerprint()).contains(fingerprint);
        assertThat(listed.encryptionKeyFingerprint()).contains(fingerprint);
        assertThat(fetched.encryptionKekFingerprints()).isNotEmpty().isEqualTo(listed.encryptionKekFingerprints());
        for (Map<String, HeaderValue> options :
                List.of(fetched.options(), fetched.derivedOptions(), listed.options(), listed.derivedOptions())) {
            assertThat(options).doesNotContainKeys("encryption_key", "encryption_key_wrapped", "critical_keys");
        }
    }

    @Test
    void givenKeylessStreamWhenReadingShouldReportServerGeneratedKey() throws Exception {
        // when
        StreamDetails created = await(client.streams().createStream(unique("async-keyless")));
        streams.add(created.id());
        StreamDetails fetched = await(client.streams().getStream(created.id())).orElseThrow();

        // then
        assertThat(fetched.encryptionKeySource()).contains(EncryptionKeySource.SERVER);
        assertThat(fetched.encryptionKeyFingerprint())
                .hasValueSatisfying(fingerprint -> assertThat(fingerprint).matches("[0-9a-f]{16}"));
        assertThat(fetched.encryptionKekFingerprints()).isNotEmpty();
    }

    @Test
    void givenStreamRenameWhenUpdatingShouldPreserveEncryptionProjection() throws Exception {
        // given
        byte[] key = randomKey();
        StreamDetails created = await(client.streams()
                .createStream(
                        unique("async-rename"),
                        StreamOptions.builder().encryptionKey(key).build()));
        streams.add(created.id());
        StreamId streamId = StreamId.of(created.id());
        await(client.topics().createTopic(streamId, "events", 1L));
        await(client.messages()
                .sendMessages(
                        streamId, TopicId.of("events"), Partitioning.partitionId(0L), List.of(Message.of("kept"))));
        String renamed = unique("async-renamed");

        // when
        await(client.streams()
                .updateStream(streamId, StreamUpdate.builder().name(renamed).build()));

        // then
        StreamDetails after = await(client.streams().getStream(created.id())).orElseThrow();
        assertThat(after.name()).isEqualTo(renamed);
        assertThat(after.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
        assertThat(after.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
        var polled = await(client.messages()
                .pollMessages(
                        streamId,
                        TopicId.of("events"),
                        Optional.of(0L),
                        Consumer.of(0L),
                        PollingStrategy.offset(BigInteger.ZERO),
                        1L,
                        false));
        assertThat(new String(polled.messages().get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo("kept");
    }

    @Test
    void givenTopicResetWhenUpdatingShouldPreserveUnmentionedValues() throws Exception {
        // given
        StreamDetails stream = await(client.streams().createStream(unique("async-topic")));
        streams.add(stream.id());
        StreamId streamId = StreamId.of(stream.id());
        var created = await(client.topics()
                .createTopic(
                        streamId,
                        "patched",
                        1L,
                        TopicOptions.builder()
                                .compressionAlgorithm(CompressionAlgorithm.Gzip)
                                .messageExpiry(HOUR_MICROS)
                                .maxTopicSize(GIB)
                                .build()));
        TopicId topicId = TopicId.of(created.id());

        // when
        await(client.topics()
                .updateTopic(
                        streamId,
                        topicId,
                        TopicUpdate.builder()
                                .messageExpiry(HOUR_MICROS.shiftLeft(1))
                                .reset(TopicOptionKey.MAX_TOPIC_SIZE)
                                .build()));

        // then
        var topic = await(client.topics().getTopic(streamId, topicId)).orElseThrow();
        assertThat(topic.name()).isEqualTo("patched");
        assertThat(topic.options().get("message_expiry")).isEqualTo(HeaderValue.fromUint64(HOUR_MICROS.shiftLeft(1)));
        assertThat(topic.derivedOptions().get("max_topic_size")).isEqualTo(HeaderValue.fromUint64(U64_MAX));
        assertThat(topic.options().get("compression_algorithm")).isEqualTo(HeaderValue.fromString("gzip"));
    }

    @Test
    void givenUserOptionsWhenCreatingAndUpdatingShouldPreserveUserBehavior() throws Exception {
        // given
        String firstName = unique("u1");
        String secondName = unique("u2");
        var first = await(client.users()
                .createUser(firstName, "secret", UserStatus.Active, Optional.empty(), UserOptions.empty()));
        users.add(first.id());
        var second = await(client.users().createUser(secondName, "secret", UserStatus.Active, Optional.empty()));
        users.add(second.id());
        String renamed = unique("u1r");

        // when
        await(client.users()
                .updateUser(
                        first.id(),
                        UserUpdate.builder()
                                .username(renamed)
                                .status(UserStatus.Inactive)
                                .build()));

        // then
        var updated = await(client.users().getUser(first.id())).orElseThrow();
        assertThat(updated.username()).isEqualTo(renamed);
        assertThat(updated.status()).isEqualTo(UserStatus.Inactive);
        assertThat(updated.options()).isEmpty();
        assertThat(updated.derivedOptions()).isEmpty();
        assertThatThrownBy(() -> await(client.users()
                        .updateUser(
                                second.id(),
                                UserUpdate.builder()
                                        .username(renamed)
                                        .status(UserStatus.Inactive)
                                        .build())))
                .isInstanceOf(IggyServerException.class);
        var untouched = await(client.users().getUser(second.id())).orElseThrow();
        assertThat(untouched.username()).isEqualTo(secondName);
        assertThat(untouched.status()).isEqualTo(UserStatus.Active);
    }

    @Test
    void givenLoggedInUserRenameWhenReconnectingShouldKeepNewUsername() throws Exception {
        // given
        String username = unique("self");
        String renamed = unique("self-renamed");
        var user =
                await(client.users().createUser(username, "secret", UserStatus.Active, Optional.of(allPermissions())));
        users.add(user.id());
        AsyncIggyTcpClient self = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .credentials(username, "secret")
                .build();
        try {
            self.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            await(self.login());
            StreamDetails stream = await(self.streams().createStream(unique("self-stream")));
            streams.add(stream.id());

            // when a stream rename and a refused rename leave the credentials alone
            await(self.streams()
                    .updateStream(
                            stream.id(),
                            StreamUpdate.builder()
                                    .name(unique("self-stream-renamed"))
                                    .build()));
            assertThatThrownBy(() -> await(self.users()
                            .updateUser(
                                    user.id(),
                                    UserUpdate.builder().username("iggy").build())))
                    .isInstanceOf(IggyServerException.class);
            await(self.users().logout());
            assertThat(await(self.login()).userId()).isEqualTo(user.id());

            // and a committed rename of the signed-in user moves them
            await(self.users()
                    .updateUser(
                            UserId.of(user.id()),
                            UserUpdate.builder().username(renamed).build()));
            await(self.users().logout());

            // then
            assertThat(await(self.login()).userId()).isEqualTo(user.id());
            assertThat(await(client.users().getUser(user.id())).orElseThrow().username())
                    .isEqualTo(renamed);
        } finally {
            self.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    @Test
    void givenRawResetWhenUpdatingShouldLetTheServerDecide() throws Exception {
        // given
        StreamDetails stream = await(client.streams().createStream(unique("async-raw-reset")));
        streams.add(stream.id());
        StreamId streamId = StreamId.of(stream.id());
        var created = await(client.topics()
                .createTopic(
                        streamId,
                        "raw",
                        1L,
                        TopicOptions.builder().messageExpiry(HOUR_MICROS).build()));
        TopicId topicId = TopicId.of(created.id());

        // when
        await(client.topics()
                .updateTopic(
                        streamId,
                        topicId,
                        TopicUpdate.builder().rawReset("message_expiry").build()));

        // then
        var topic = await(client.topics().getTopic(streamId, topicId)).orElseThrow();
        assertThat(topic.derivedOptions().get("message_expiry")).isEqualTo(HeaderValue.fromUint64(U64_MAX));
        assertThatThrownBy(() -> await(client.streams()
                        .updateStream(
                                streamId,
                                StreamUpdate.builder().rawReset("critical_keys").build())))
                .isInstanceOfSatisfying(
                        IggyServerException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.READ_ONLY_OPTION));
    }

    private AsyncIggyTcpClient connect() throws Exception {
        AsyncIggyTcpClient connected = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        connected.connect().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return connected;
    }

    /** Waits for {@code future}, rethrowing an Iggy failure as itself rather than wrapped. */
    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            throw failed;
        }
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static byte[] randomKey() {
        byte[] key = new byte[EncryptionKeys.KEY_LENGTH];
        new SecureRandom().nextBytes(key);
        return key;
    }

    private static Permissions allPermissions() {
        return new Permissions(
                new GlobalPermissions(true, true, true, true, true, true, true, true, true, true), Map.of());
    }
}
