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
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.EncryptionKeys;
import org.apache.iggy.stream.StreamBase;
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
import org.apache.iggy.user.UserOptions;
import org.apache.iggy.user.UserStatus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class MetadataChangesSteps {

    private static final long ROOT_USER_ID = 0;
    private static final int FOLLOW_ATTEMPTS = 100;
    private static final long FOLLOW_INTERVAL_MILLIS = 100;
    private static final List<String> HIDDEN_OPTIONS =
            List.of("encryption_key", "encryption_key_wrapped", "critical_keys");

    private IggyTcpClient client;
    private List<MetadataChangeGroup> groups = List.of();
    private MetadataChangeCursor cursor;
    private List<MetadataChangeRecord> followed = List.of();
    private List<MetadataCurrentItem> items = List.of();
    private Long userId;

    // The client may be signed in as a user without manage users by now, so root signs in again to remove it.
    @After
    public void close() {
        if (client == null) {
            return;
        }
        if (userId != null) {
            client.users().logout();
            client.users().login(TestEnvironment.rootUsername(), TestEnvironment.rootPassword());
            client.users().deleteUser(userId);
        }
        client.close();
    }

    @Given("I hold the latest metadata change cursor for the groups {string}")
    public void holdLatestCursor(String names) {
        groups = parseGroups(names);
        var page = read(MetadataChangeStart.latest());
        assertEquals(MetadataChangeStatus.OK, page.status());
        cursor = page.next().orElseThrow();
    }

    @When("I create and delete {int} streams named {string}")
    public void churnStreams(int count, String prefix) {
        for (int index = 0; index < count; index++) {
            var stream = client().streams().createStream(prefix + "-" + index);
            client().streams().deleteStream(stream.id());
        }
    }

    @Then("following the metadata changes should return the kinds {string} for the stream")
    public void followedKinds(String kinds) throws InterruptedException {
        List<MetadataChangeKind> expected = Arrays.stream(kinds.split(","))
                .map(name -> MetadataChangeKind.fromValue(name.trim()).orElseThrow())
                .toList();
        long streamId = ResourceOptionsSteps.current().createdStreamId();
        List<MetadataChangeRecord> records = new ArrayList<>();
        var position = cursor;
        // A two phase delete commits its record after the call returns.
        for (int attempt = 0; attempt < FOLLOW_ATTEMPTS; attempt++) {
            var page = read(MetadataChangeStart.after(position));
            assertEquals(MetadataChangeStatus.OK, page.status());
            position = page.next().orElseThrow();
            records.addAll(page.records());
            if (page.hasMore()) {
                continue;
            }
            if (records.stream().filter(record -> namesStream(record, streamId)).count() >= expected.size()) {
                break;
            }
            Thread.sleep(FOLLOW_INTERVAL_MILLIS);
        }
        for (MetadataChangeRecord record : records) {
            assertTrue(groups.contains(record.group().orElseThrow()), record.toString());
        }
        followed =
                records.stream().filter(record -> namesStream(record, streamId)).toList();
        assertEquals(
                expected,
                followed.stream().map(record -> record.kind().orElseThrow()).toList());
    }

    @Then("each followed change should name the root user as its actor")
    public void rootActor() {
        for (MetadataChangeRecord record : followed) {
            assertEquals(Optional.of(ROOT_USER_ID), record.actorUserId(), record.toString());
        }
    }

    @Then("the followed {string} change should carry a requested op before its own op")
    public void requestedOpBeforeOwnOp(String kind) {
        var record = followed(kind);
        var deleted = assertInstanceOf(MetadataChangeBody.StreamDeleted.class, record.body());
        assertTrue(deleted.requestedOp().orElseThrow().compareTo(record.op()) < 0, record.toString());
    }

    @Then("the followed {string} change should report encryption key source {string}")
    public void keySource(String kind, String source) {
        assertEquals(
                EncryptionKeySource.fromValue(source).orElseThrow(),
                followedStream(kind).encryptionKeySource().orElseThrow());
    }

    @Then("the followed {string} change should carry the stream key fingerprint")
    public void keyFingerprint(String kind) {
        assertEquals(
                EncryptionKeys.fingerprint(ResourceOptionsSteps.current().streamKey()),
                followedStream(kind).encryptionKeyFingerprint().orElseThrow());
    }

    @Then("the followed changes should expose no encryption key material")
    public void noKeyMaterial() {
        for (MetadataChangeRecord record : followed) {
            if (record.body() instanceof MetadataChangeBody.StreamChange stream) {
                assertFalse(stream.optionsUnreadable(), record.toString());
                for (String hidden : HIDDEN_OPTIONS) {
                    assertFalse(stream.options().containsKey(hidden), hidden);
                    assertFalse(stream.derivedOptions().containsKey(hidden), hidden);
                }
            }
        }
    }

    @Then("following the metadata changes should answer {string}")
    public void followStatus(String status) {
        var expected = MetadataChangeStatus.fromValue(status).orElseThrow();
        var page = read(MetadataChangeStart.after(cursor));
        assertEquals(expected, page.status());
        if (expected != MetadataChangeStatus.OK) {
            assertTrue(page.records().isEmpty());
            assertTrue(page.next().isEmpty());
        }
    }

    @When("I read the current state for the groups {string}")
    public void readCurrentState(String names) {
        groups = parseGroups(names);
        List<MetadataCurrentItem> scanned = new ArrayList<>();
        var start = MetadataChangeStart.current();
        while (true) {
            var page = read(start);
            assertEquals(MetadataChangeStatus.OK, page.status());
            scanned.addAll(page.items());
            if (page.currentPageToken().isEmpty()) {
                cursor = page.next().orElseThrow();
                break;
            }
            start = MetadataChangeStart.current(page.currentPageToken().orElseThrow());
        }
        items = scanned;
    }

    @Then("the current state should list the same streams as the stream list")
    public void currentStateMatchesList() {
        Map<Long, String> listed =
                client().streams().getStreams().stream().collect(Collectors.toMap(StreamBase::id, StreamBase::name));
        Map<Long, String> projected = items.stream()
                .map(MetadataCurrentItem::body)
                .filter(MetadataChangeBody.StreamChange.class::isInstance)
                .map(MetadataChangeBody.StreamChange.class::cast)
                .collect(Collectors.toMap(
                        MetadataChangeBody.StreamChange::streamId, MetadataChangeBody.StreamChange::name));
        assertEquals(listed, projected);
    }

    @Given("a user named {string} with password {string} and no server permissions exists")
    public void userWithoutServerPermissions(String username, String password) {
        var permissions = new Permissions(
                new GlobalPermissions(false, true, false, false, false, false, false, false, false, false), Map.of());
        var user = client().users()
                .createUser(username, password, UserStatus.Active, Optional.of(permissions), UserOptions.empty());
        userId = user.id();
    }

    @When("I authenticate as {string} with password {string}")
    public void authenticateAs(String username, String password) {
        client().users().logout();
        assertNotNull(client().users().login(username, password));
    }

    @Then("reading the metadata changes should be refused as unauthorized")
    public void readingRefused() {
        groups = List.of();
        var refusal = assertThrows(IggyServerException.class, () -> read(MetadataChangeStart.earliest()));
        assertEquals(IggyErrorCode.UNAUTHORIZED, refusal.getErrorCode());
    }

    private MetadataChanges read(MetadataChangeStart start) {
        return client().system()
                .getMetadataChanges(MetadataChangesQuery.builder()
                        .start(start)
                        .groups(groups)
                        .limit(MetadataChangesQuery.MAX_LIMIT)
                        .build());
    }

    private MetadataChangeRecord followed(String kind) {
        var parsed = MetadataChangeKind.fromValue(kind).orElseThrow();
        return followed.stream()
                .filter(record -> record.kind().equals(Optional.of(parsed)))
                .findFirst()
                .orElseThrow();
    }

    private MetadataChangeBody.StreamChange followedStream(String kind) {
        var stream = assertInstanceOf(
                MetadataChangeBody.StreamChange.class, followed(kind).body());
        assertFalse(stream.optionsUnreadable());
        return stream;
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

    private static List<MetadataChangeGroup> parseGroups(String names) {
        return Arrays.stream(names.split(","))
                .map(name -> MetadataChangeGroup.fromValue(name.trim()).orElseThrow())
                .toList();
    }

    private static boolean namesStream(MetadataChangeRecord record, long streamId) {
        var body = record.body();
        if (body instanceof MetadataChangeBody.StreamChange stream) {
            return stream.streamId() == streamId;
        }
        if (body instanceof MetadataChangeBody.StreamDeleted stream) {
            return stream.streamId() == streamId;
        }
        if (body instanceof MetadataChangeBody.TopicChange topic) {
            return topic.streamId() == streamId;
        }
        if (body instanceof MetadataChangeBody.TopicDeleted topic) {
            return topic.streamId() == streamId;
        }
        return body instanceof MetadataChangeBody.PartitionsChanged partitions && partitions.streamId() == streamId;
    }
}
