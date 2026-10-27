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

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamOptionKey;
import org.apache.iggy.stream.StreamOptions;
import org.apache.iggy.stream.StreamUpdate;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeKind;
import org.apache.iggy.system.MetadataChangePhase;
import org.apache.iggy.system.MetadataChangeRecord;
import org.apache.iggy.system.MetadataChangeStart;
import org.apache.iggy.system.MetadataChangeStatus;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataChangesQuery;
import org.apache.iggy.system.MetadataCurrentItem;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserOptionKey;
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.iggy.ResourceOptionsFixture.section;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class SystemClientBaseTest extends IntegrationTest {

    private static final long ROOT_USER_ID = 0;
    private static final int FOLLOW_ATTEMPTS = 100;
    private static final long FOLLOW_INTERVAL_MILLIS = 100;
    private static final List<String> HIDDEN_OPTIONS =
            List.of("encryption_key", "encryption_key_wrapped", "critical_keys");

    protected SystemClient systemClient;

    @BeforeEach
    void beforeEachBase() {
        systemClient = client.system();

        login();
    }

    @Test
    void shouldDescribeTheTopicOptionCatalog() {
        // when
        var specs = systemClient.describeOptions(OptionsScope.Topic);

        // then
        var byKey = specs.stream().collect(Collectors.toMap(OptionSpec::key, spec -> spec));
        assertThat(byKey).containsKeys("segment_size", "durability");
        var segmentSize = byKey.get("segment_size");
        assertThat(segmentSize.defaultValue().kind()).isEqualTo(HeaderKind.Uint64);
        assertThat(segmentSize.defaultValue().value()).isNotEmpty();
        assertThat(segmentSize.description()).isNotBlank();
    }

    @Test
    void shouldDescribeStreamAndUserOptionCatalogs() {
        var stream = systemClient.describeOptions(OptionsScope.Stream).stream()
                .collect(Collectors.toMap(OptionSpec::key, spec -> spec));
        assertThat(stream).containsKeys(StreamOptionKey.NAME.value(), StreamOptionKey.ENCRYPTION_KEY.value());
        assertThat(stream.get(StreamOptionKey.ENCRYPTION_KEY.value()).secret()).isTrue();
        assertThat(stream.get(StreamOptionKey.NAME.value()).updatable()).isTrue();

        var user = systemClient.describeOptions(OptionsScope.User).stream()
                .collect(Collectors.toMap(OptionSpec::key, spec -> spec));
        assertThat(user).containsKeys(UserOptionKey.USERNAME.value(), UserOptionKey.STATUS.value());
    }

    @Test
    void shouldDescribeEveryScopeWithTheFixtureKindsAndFlags() {
        for (JsonNode vector : section("describe_options")) {
            var scope = OptionsScope.valueOf(
                    StringUtils.capitalize(vector.get("scope").asString()));
            List<String> expected = new ArrayList<>();
            vector.get("entries")
                    .forEach(entry -> expected.add(entry.get("key").asString() + " "
                            + entry.get("kind").asString() + " "
                            + entry.get("flags").asInt()));

            var described = systemClient.describeOptions(scope).stream()
                    .map(spec -> spec.key() + " "
                            + spec.defaultValue().kind().name().toLowerCase(Locale.ROOT) + " " + spec.flags())
                    .toList();

            assertThat(described).as(scope.name()).isEqualTo(expected);
        }
    }

    @Test
    void shouldGetStats() {
        // when
        var stats = systemClient.getStats();

        // then
        assertThat(stats).isNotNull();
        assertThat(stats.processId()).isNotNull();
        assertThat(stats.hostname()).isNotEmpty();
        assertThat(stats.threadsCount()).isNotNull();
        assertThat(stats.freeDiskSpace()).isNotNull();
        assertThat(stats.totalDiskSpace()).isNotNull();
        assertThat(stats.openFilesCount()).isNotNull();
        assertThat(stats.openFilesLimit()).isNotNull();
    }

    @Test
    void shouldGetClusterMetadataForSingleNode() {
        // when
        var metadata = systemClient.getClusterMetadata();

        // then
        assertThat(metadata).isNotNull();
        // The server runs standalone (cluster disabled), so it synthesizes
        // itself as the sole leader of a single-node roster.
        assertThat(metadata.name()).isEqualTo("single-node");
        assertThat(metadata.nodes()).hasSize(1);
        var node = metadata.nodes().get(0);
        assertThat(node.name()).isEqualTo("iggy-node");
        assertThat(node.role()).isEqualTo(ClusterNodeRole.Leader);
        assertThat(node.status()).isEqualTo(ClusterNodeStatus.Healthy);
        assertThat(node.endpoints().tcp()).isPositive();
    }

    @Test
    void shouldPing() {
        // when
        var result = systemClient.ping();

        // then
        assertThat(result).isEqualTo("pong");
    }

    @Test
    void shouldPingWithoutAuthentication() {
        // given — fresh client, connected but not logged in
        var unauthenticatedClient = getClient();

        // when
        var result = unauthenticatedClient.system().ping();

        // then
        assertThat(result).isEqualTo("pong");
    }

    @Test
    void givenStreamLifecycleWhenFollowingFromLatestShouldReturnItsRecords() throws InterruptedException {
        // given
        var latest = read(MetadataChangeStart.latest(), List.of(MetadataChangeGroup.STREAMS), 1);
        assertThat(latest.status()).isEqualTo(MetadataChangeStatus.OK);
        assertThat(latest.phase()).isEqualTo(MetadataChangePhase.CHANGES);
        assertThat(latest.records()).isEmpty();
        assertThat(latest.logId()).isEqualTo(latest.next().map(MetadataChangeCursor::logId));
        String name = unique("changes");
        String renamed = name + "-renamed";

        // when
        var created = client.streams().createStream(name);
        client.streams()
                .updateStream(created.id(), StreamUpdate.builder().name(renamed).build());
        client.streams().deleteStream(created.id());
        var records = follow(
                latest.next().orElseThrow(),
                List.of(MetadataChangeGroup.STREAMS),
                followed -> ofStream(followed, created.id(), name).size() >= 3);

        // then
        assertThat(records).allSatisfy(record -> assertThat(record.group()).contains(MetadataChangeGroup.STREAMS));
        var mine = ofStream(records, created.id(), name);
        assertThat(mine)
                .extracting(record -> record.kind().orElseThrow())
                .containsExactly(
                        MetadataChangeKind.STREAM_CREATED,
                        MetadataChangeKind.STREAM_UPDATED,
                        MetadataChangeKind.STREAM_DELETED);
        assertThat(mine).allSatisfy(record -> {
            assertThat(record.actorUserId()).contains(ROOT_USER_ID);
            assertThat(record.checksum()).isPositive();
        });
        var createdBody = (MetadataChangeBody.StreamChange) mine.get(0).body();
        assertThat(createdBody.streamCreatedOp()).isEqualTo(mine.get(0).op());
        assertThat(createdBody.optionsUnreadable()).isFalse();
        assertThat(((MetadataChangeBody.StreamChange) mine.get(1).body()).name())
                .isEqualTo(renamed);
        var deleted = (MetadataChangeBody.StreamDeleted) mine.get(2).body();
        assertThat(deleted.name()).isEqualTo(renamed);
        assertThat(deleted.createdAt()).isEqualTo(createdBody.createdAt());
        assertThat(deleted.requestedOp()).isEmpty();
    }

    @Test
    void givenTopicsGroupWhenFollowingShouldReturnOnlyTopicRecords() throws InterruptedException {
        // given
        var cursor = latestCursor(List.of(MetadataChangeGroup.TOPICS));
        var stream = client.streams().createStream(unique("changes-filter"));
        trackStream(stream.id());

        // when
        client.topics().createTopic(stream.id(), "events", 1L);
        var records = follow(
                cursor,
                List.of(MetadataChangeGroup.TOPICS),
                followed -> followed.stream().anyMatch(record -> isTopicOf(record, stream.id())));

        // then
        assertThat(records).allSatisfy(record -> assertThat(record.group()).contains(MetadataChangeGroup.TOPICS));
        var topic = (MetadataChangeBody.TopicChange) records.stream()
                .filter(record -> isTopicOf(record, stream.id()))
                .findFirst()
                .orElseThrow()
                .body();
        assertThat(topic.name()).isEqualTo("events");
        assertThat(topic.partitionsCount()).isEqualTo(1L);
        assertThat(topic.derivedOptions()).containsKey("segment_size");
    }

    @Test
    void givenLimitOfOneWhenFollowingShouldPageOneRecordAtATime() throws InterruptedException {
        // given
        var cursor = latestCursor(List.of(MetadataChangeGroup.STREAMS));
        List<String> names = List.of(unique("changes-page"), unique("changes-page"), unique("changes-page"));
        names.forEach(name -> trackStream(client.streams().createStream(name).id()));

        // when
        List<String> seen = new ArrayList<>();
        for (int attempt = 0; attempt < FOLLOW_ATTEMPTS && seen.size() < names.size(); attempt++) {
            var from = cursor;
            var page = read(MetadataChangeStart.after(from), List.of(MetadataChangeGroup.STREAMS), 1);
            assertThat(page.status()).isEqualTo(MetadataChangeStatus.OK);
            assertThat(page.records()).hasSizeLessThanOrEqualTo(1);
            var next = page.next().orElseThrow();
            page.records().forEach(record -> {
                assertThat(record.op()).isGreaterThan(from.op());
                assertThat(next.op()).isEqualTo(record.op());
                assertThat(next.ordinal()).isEqualTo(record.ordinal());
                if (record.body() instanceof MetadataChangeBody.StreamChange stream && names.contains(stream.name())) {
                    seen.add(stream.name());
                }
            });
            if (page.records().isEmpty() && !page.hasMore()) {
                Thread.sleep(FOLLOW_INTERVAL_MILLIS);
            }
            cursor = next;
        }

        // then
        assertThat(seen).isEqualTo(names);
    }

    @Test
    void givenCurrentStateScanWhenPagingShouldListTheStreamsTheListReturns() {
        // given
        var stream = client.streams().createStream(unique("changes-current"));
        trackStream(stream.id());
        trackStream(client.streams().createStream(unique("changes-current")).id());
        client.topics().createTopic(stream.id(), "events", 2L);
        var groups = List.of(MetadataChangeGroup.STREAMS, MetadataChangeGroup.TOPICS, MetadataChangeGroup.PARTITIONS);

        // when
        // A deferred delete of another test can complete at any time, so the scan is compared with a stream list
        // that did not change around it.
        List<MetadataCurrentItem> items = List.of();
        Map<Long, String> listed = Map.of();
        for (int attempt = 0; attempt < 3; attempt++) {
            Map<Long, String> before = listStreams();
            items = scanCurrent(groups);
            listed = listStreams();
            if (before.equals(listed)) {
                break;
            }
        }

        // then
        Map<Long, String> projected = items.stream()
                .map(MetadataCurrentItem::body)
                .filter(MetadataChangeBody.StreamChange.class::isInstance)
                .map(MetadataChangeBody.StreamChange.class::cast)
                .collect(Collectors.toMap(
                        MetadataChangeBody.StreamChange::streamId, MetadataChangeBody.StreamChange::name));
        assertThat(projected).isEqualTo(listed);
        var topic = items.stream()
                .map(MetadataCurrentItem::body)
                .filter(MetadataChangeBody.TopicItem.class::isInstance)
                .map(MetadataChangeBody.TopicItem.class::cast)
                .filter(item -> item.streamId() == stream.id())
                .findFirst()
                .orElseThrow();
        assertThat(topic.name()).isEqualTo("events");
        assertThat(items.stream()
                        .map(MetadataCurrentItem::body)
                        .filter(MetadataChangeBody.PartitionsItem.class::isInstance)
                        .map(MetadataChangeBody.PartitionsItem.class::cast)
                        .filter(item -> item.streamId() == stream.id() && item.topicId() == topic.topicId())
                        .findFirst()
                        .orElseThrow()
                        .partitionsCount())
                .isEqualTo(2L);
    }

    @Test
    void givenCursorOfAnotherHistoryOrBeyondTheHeadWhenFollowingShouldAnswerResetOrBehind() {
        // given
        var latest = latestCursor(List.of(MetadataChangeGroup.STREAMS));
        var otherHistory = new MetadataChangeCursor(
                latest.logId().add(BigInteger.ONE), latest.op(), latest.checksum(), latest.ordinal());
        var beyondHead = new MetadataChangeCursor(
                latest.logId(), BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE), latest.checksum(), 0);

        // when
        var reset = read(MetadataChangeStart.after(otherHistory), List.of(MetadataChangeGroup.STREAMS), 100);
        var behind = read(MetadataChangeStart.after(beyondHead), List.of(MetadataChangeGroup.STREAMS), 100);

        // then
        assertThat(reset.status()).isEqualTo(MetadataChangeStatus.RESET);
        assertThat(reset.records()).isEmpty();
        assertThat(reset.next()).isEmpty();
        assertThat(behind.status()).isEqualTo(MetadataChangeStatus.BEHIND);
        assertThat(behind.next()).contains(beyondHead);
    }

    @Test
    void givenQueryTheRulesRefuseWhenReadingChangesShouldRefuseItBeforeSending() {
        // given
        trackStream(client.streams().createStream(unique("changes-refused")).id());
        trackStream(client.streams().createStream(unique("changes-refused")).id());
        var token = read(MetadataChangeStart.current(), List.of(MetadataChangeGroup.STREAMS), 1)
                .currentPageToken()
                .orElseThrow();
        var notCanonical = new MetadataChangeCursor(BigInteger.ONE, BigInteger.ZERO, BigInteger.TWO, 0);
        List<MetadataChangesQuery> refused = List.of(
                MetadataChangesQuery.builder().limit(0).build(),
                MetadataChangesQuery.builder()
                        .limit(MetadataChangesQuery.MAX_LIMIT + 1)
                        .build(),
                MetadataChangesQuery.builder()
                        .since(BigInteger.TWO)
                        .until(BigInteger.ONE)
                        .build(),
                MetadataChangesQuery.builder()
                        .start(MetadataChangeStart.latest())
                        .since(BigInteger.ONE)
                        .build(),
                MetadataChangesQuery.builder()
                        .start(MetadataChangeStart.current())
                        .groups(MetadataChangeGroup.TOPICS)
                        .build(),
                MetadataChangesQuery.builder()
                        .start(MetadataChangeStart.current(token))
                        .groups(MetadataChangeGroup.STREAMS, MetadataChangeGroup.TOPICS)
                        .build(),
                MetadataChangesQuery.builder()
                        .start(MetadataChangeStart.after(notCanonical))
                        .build());

        // then
        for (MetadataChangesQuery query : refused) {
            assertThatThrownBy(() -> systemClient.getMetadataChanges(query))
                    .isInstanceOfSatisfying(
                            IggyValidationException.class,
                            refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
        }
    }

    @Test
    void givenUserWithoutManageServersWhenReadingChangesShouldBeRefusedAsUnauthorized() {
        // given
        String username = unique("changes-reader");
        var permissions = new Permissions(
                new GlobalPermissions(false, true, false, false, false, false, false, false, false, false), Map.of());
        var user = client.users()
                .createUser(
                        username, "changes-secret", UserStatus.Active, Optional.of(permissions), UserOptions.empty());
        trackUser(user.id());
        var reader = getClient();
        reader.users().login(username, "changes-secret");

        // then
        assertThatThrownBy(() -> reader.system()
                        .getMetadataChanges(MetadataChangesQuery.builder().build()))
                .isInstanceOfSatisfying(
                        IggyServerException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.UNAUTHORIZED));
    }

    @Test
    void givenKeyedAndGeneratedStreamKeysWhenFollowingShouldReportFingerprintsAndNoKey() throws InterruptedException {
        // given
        var cursor = latestCursor(List.of(MetadataChangeGroup.STREAMS));
        byte[] key = new byte[EncryptionKeys.KEY_LENGTH];
        new SecureRandom().nextBytes(key);
        String keyedName = unique("changes-keyed");
        String generatedName = unique("changes-generated");

        // when
        var keyed = client.streams()
                .createStream(
                        keyedName, StreamOptions.builder().encryptionKey(key).build());
        trackStream(keyed.id());
        var generated = client.streams().createStream(generatedName);
        trackStream(generated.id());
        var records = follow(
                cursor,
                List.of(MetadataChangeGroup.STREAMS),
                followed -> ofStream(followed, keyed.id(), keyedName).size() == 1
                        && ofStream(followed, generated.id(), generatedName).size() == 1);

        // then
        var keyedStream = (MetadataChangeBody.StreamChange)
                ofStream(records, keyed.id(), keyedName).get(0).body();
        assertThat(keyedStream.encryptionKeySource()).contains(EncryptionKeySource.STREAM);
        assertThat(keyedStream.encryptionKeyFingerprint()).contains(EncryptionKeys.fingerprint(key));
        var generatedStream = (MetadataChangeBody.StreamChange)
                ofStream(records, generated.id(), generatedName).get(0).body();
        assertThat(generatedStream.encryptionKeySource()).contains(EncryptionKeySource.SERVER);
        assertThat(generatedStream.encryptionKeyFingerprint()).isPresent();
        for (var stream : List.of(keyedStream, generatedStream)) {
            for (var options : List.of(stream.options(), stream.derivedOptions())) {
                assertThat(options).doesNotContainKeys(HIDDEN_OPTIONS.toArray(String[]::new));
                assertThat(options.values())
                        .noneSatisfy(value -> assertThat(value.value()).isEqualTo(key));
            }
        }
    }

    private MetadataChanges read(MetadataChangeStart start, List<MetadataChangeGroup> groups, long limit) {
        return systemClient.getMetadataChanges(MetadataChangesQuery.builder()
                .start(start)
                .groups(groups)
                .limit(limit)
                .build());
    }

    private MetadataChangeCursor latestCursor(List<MetadataChangeGroup> groups) {
        var page = read(MetadataChangeStart.latest(), groups, 1);
        assertThat(page.status()).isEqualTo(MetadataChangeStatus.OK);
        return page.next().orElseThrow();
    }

    /**
     * Follows the change log from {@code cursor} until {@code done} holds. A two phase delete commits its record
     * after the call returns, so an empty tail is polled again.
     */
    private List<MetadataChangeRecord> follow(
            MetadataChangeCursor cursor, List<MetadataChangeGroup> groups, Predicate<List<MetadataChangeRecord>> done)
            throws InterruptedException {
        List<MetadataChangeRecord> records = new ArrayList<>();
        var position = cursor;
        for (int attempt = 0; attempt < FOLLOW_ATTEMPTS; attempt++) {
            var page = read(MetadataChangeStart.after(position), groups, MetadataChangesQuery.MAX_LIMIT);
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

    private List<MetadataCurrentItem> scanCurrent(List<MetadataChangeGroup> groups) {
        List<MetadataCurrentItem> items = new ArrayList<>();
        var start = MetadataChangeStart.current();
        while (true) {
            var page = read(start, groups, 2);
            assertThat(page.status()).isEqualTo(MetadataChangeStatus.OK);
            assertThat(page.phase()).isEqualTo(MetadataChangePhase.CURRENT);
            assertThat(page.records()).isEmpty();
            assertThat(page.hasMore()).isEqualTo(page.currentPageToken().isPresent());
            items.addAll(page.items());
            if (page.currentPageToken().isEmpty()) {
                var anchor = page.next().orElseThrow();
                assertThat(read(MetadataChangeStart.after(anchor), groups, 1).status())
                        .isEqualTo(MetadataChangeStatus.OK);
                return items;
            }
            assertThat(page.currentPageToken().orElseThrow().groups()).containsExactlyElementsOf(groups);
            start = MetadataChangeStart.current(page.currentPageToken().orElseThrow());
        }
    }

    private Map<Long, String> listStreams() {
        return client.streams().getStreams().stream().collect(Collectors.toMap(StreamBase::id, StreamBase::name));
    }

    /** The records of the stream created under {@code name}, matched by full identity because ids are reused. */
    private static List<MetadataChangeRecord> ofStream(List<MetadataChangeRecord> records, long streamId, String name) {
        Optional<BigInteger> createdOp = records.stream()
                .filter(record -> record.kind().equals(Optional.of(MetadataChangeKind.STREAM_CREATED)))
                .filter(record -> record.body() instanceof MetadataChangeBody.StreamChange stream
                        && stream.streamId() == streamId
                        && stream.name().equals(name))
                .map(MetadataChangeRecord::op)
                .findFirst();
        return records.stream()
                .filter(record -> createdOp.isPresent() && namesStream(record, streamId, createdOp.get()))
                .toList();
    }

    private static boolean namesStream(MetadataChangeRecord record, long streamId, BigInteger createdOp) {
        if (record.body() instanceof MetadataChangeBody.StreamChange stream) {
            return stream.streamId() == streamId && stream.streamCreatedOp().equals(createdOp);
        }
        return record.body() instanceof MetadataChangeBody.StreamDeleted deleted
                && deleted.streamId() == streamId
                && deleted.streamCreatedOp().equals(createdOp);
    }

    private static boolean isTopicOf(MetadataChangeRecord record, long streamId) {
        return record.body() instanceof MetadataChangeBody.TopicChange topic
                && record.kind().equals(Optional.of(MetadataChangeKind.TOPIC_CREATED))
                && topic.streamId() == streamId
                && topic.topicCreatedOp().equals(record.op());
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().replace("-", "");
    }
}
