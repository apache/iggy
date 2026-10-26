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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeKind;
import org.apache.iggy.system.MetadataChangeRecord;
import org.apache.iggy.system.MetadataChangeStart;
import org.apache.iggy.system.MetadataChangeStatus;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.MetadataCurrentItem;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Metadata changes through the async TCP client. */
class MetadataChangesTest extends BaseIntegrationTest {

    private static final int TIMEOUT_SECONDS = 5;
    private static final int FOLLOW_ATTEMPTS = 100;
    private static final long FOLLOW_INTERVAL_MILLIS = 100;

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
    void givenKeyedStreamWhenFollowingFromLatestShouldReturnItsRecordWithFingerprintsAndNoKey() throws Exception {
        // given
        var latest = await(client.system().getMetadataChanges(query(MetadataChangeStart.latest(), 1)));
        assertThat(latest.status()).isEqualTo(MetadataChangeStatus.OK);
        byte[] key = new byte[EncryptionKeys.KEY_LENGTH];
        new SecureRandom().nextBytes(key);
        String name = unique("async-changes");

        // when
        var created = await(client.streams()
                .createStream(name, StreamOptions.builder().encryptionKey(key).build()));
        streams.add(created.id());
        var records = follow(
                latest.next().orElseThrow(),
                followed -> followed.stream().anyMatch(record -> isCreationOf(record, created.id(), name)));

        // then
        var record = records.stream()
                .filter(followed -> isCreationOf(followed, created.id(), name))
                .findFirst()
                .orElseThrow();
        assertThat(record.actorUserId()).contains(0L);
        assertThat(record.group()).contains(MetadataChangeGroup.STREAMS);
        var stream = (MetadataChangeBody.StreamChange) record.body();
        assertThat(stream.streamCreatedOp()).isEqualTo(record.op());
        assertThat(stream.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
        assertThat(stream.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
        assertThat(stream.derivedOptions()).doesNotContainKeys("encryption_key", "encryption_key_wrapped");
        assertThat(stream.options()).doesNotContainKeys("encryption_key", "encryption_key_wrapped");
    }

    @Test
    void givenCurrentStateScanWhenPagingShouldEndWithAnAnchorToFollowFrom() throws Exception {
        // given
        streams.add(
                await(client.streams().createStream(unique("async-current"))).id());
        streams.add(
                await(client.streams().createStream(unique("async-current"))).id());

        // when
        List<MetadataCurrentItem> items = new ArrayList<>();
        var start = MetadataChangeStart.current();
        MetadataChanges page;
        do {
            page = await(client.system().getMetadataChanges(query(start, 1)));
            assertThat(page.status()).isEqualTo(MetadataChangeStatus.OK);
            items.addAll(page.items());
            start = page.currentPageToken().map(MetadataChangeStart::current).orElse(start);
        } while (page.currentPageToken().isPresent());
        var followed = await(client.system()
                .getMetadataChanges(query(MetadataChangeStart.after(page.next().orElseThrow()), 1)));

        // then
        assertThat(items.stream()
                        .map(MetadataCurrentItem::body)
                        .filter(MetadataChangeBody.StreamChange.class::isInstance)
                        .map(body -> ((MetadataChangeBody.StreamChange) body).streamId()))
                .containsAll(streams);
        assertThat(followed.status()).isEqualTo(MetadataChangeStatus.OK);
    }

    @Test
    void givenQueryTheRulesRefuseWhenReadingChangesShouldFailTheFutureBeforeSending() {
        var query = MetadataChangesQuery.builder()
                .start(MetadataChangeStart.current())
                .groups(MetadataChangeGroup.TOPICS)
                .build();

        assertThatThrownBy(() -> await(client.system().getMetadataChanges(query)))
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }

    @Test
    void givenUserWithoutManageServersWhenReadingChangesShouldBeRefusedAsUnauthorized() throws Exception {
        // given
        String username = unique("async-reader");
        var permissions = new Permissions(
                new GlobalPermissions(false, true, false, false, false, false, false, false, false, false), Map.of());
        users.add(await(client.users()
                        .createUser(username, "changes-secret", UserStatus.Active, Optional.of(permissions)))
                .id());
        var reader = connect();

        try {
            await(reader.users().login(username, "changes-secret"));

            // then
            assertThatThrownBy(() -> await(reader.system()
                            .getMetadataChanges(MetadataChangesQuery.builder().build())))
                    .isInstanceOfSatisfying(
                            IggyServerException.class,
                            refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHORIZED));
        } finally {
            reader.close().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    private List<MetadataChangeRecord> follow(MetadataChangeCursor cursor, Predicate<List<MetadataChangeRecord>> done)
            throws Exception {
        List<MetadataChangeRecord> records = new ArrayList<>();
        var position = cursor;
        for (int attempt = 0; attempt < FOLLOW_ATTEMPTS; attempt++) {
            var page = await(client.system()
                    .getMetadataChanges(query(MetadataChangeStart.after(position), MetadataChangesQuery.MAX_LIMIT)));
            assertThat(page.status()).isEqualTo(MetadataChangeStatus.OK);
            position = page.next().orElseThrow();
            records.addAll(page.records());
            if (!page.hasMore() && done.test(records)) {
                return records;
            }
            if (!page.hasMore()) {
                Thread.sleep(FOLLOW_INTERVAL_MILLIS);
            }
        }
        throw new AssertionError("The followed metadata changes never reached the expected records");
    }

    private static MetadataChangesQuery query(MetadataChangeStart start, long limit) {
        return MetadataChangesQuery.builder()
                .start(start)
                .groups(MetadataChangeGroup.STREAMS)
                .limit(limit)
                .build();
    }

    private static boolean isCreationOf(MetadataChangeRecord record, long streamId, String name) {
        return record.kind().equals(Optional.of(MetadataChangeKind.STREAM_CREATED))
                && record.body() instanceof MetadataChangeBody.StreamChange stream
                && stream.streamId() == streamId
                && stream.name().equals(name);
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
}
