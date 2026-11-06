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

package org.apache.iggy.conformance;

import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeBody.ConsumerGroupCreated;
import org.apache.iggy.system.MetadataChangeBody.ConsumerGroupDeleted;
import org.apache.iggy.system.MetadataChangeBody.ConsumerGroupItem;
import org.apache.iggy.system.MetadataChangeBody.MemberItem;
import org.apache.iggy.system.MetadataChangeBody.MemberJoined;
import org.apache.iggy.system.MetadataChangeBody.MemberLeft;
import org.apache.iggy.system.MetadataChangeBody.PartitionHandedOver;
import org.apache.iggy.system.MetadataChangeBody.PartitionsChanged;
import org.apache.iggy.system.MetadataChangeBody.PartitionsItem;
import org.apache.iggy.system.MetadataChangeBody.Session;
import org.apache.iggy.system.MetadataChangeBody.SessionClosed;
import org.apache.iggy.system.MetadataChangeBody.StreamChange;
import org.apache.iggy.system.MetadataChangeBody.StreamDeleted;
import org.apache.iggy.system.MetadataChangeBody.TokenChange;
import org.apache.iggy.system.MetadataChangeBody.TokenDeleted;
import org.apache.iggy.system.MetadataChangeBody.TopicChange;
import org.apache.iggy.system.MetadataChangeBody.TopicDeleted;
import org.apache.iggy.system.MetadataChangeBody.TopicItem;
import org.apache.iggy.system.MetadataChangeBody.Unknown;
import org.apache.iggy.system.MetadataChangeBody.UserChange;
import org.apache.iggy.system.MetadataChangeBody.UserIdentity;
import org.apache.iggy.system.MetadataChangeBody.UserPermissionsUpdated;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeRecord;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataCurrentItem;
import org.apache.iggy.system.MetadataCurrentPageToken;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The canonical form of a metadata changes answer: the HTTP JSON that the fixture
 * core/common/fixtures/metadata_changes.json names "expected". The views follow the fixture test of the SDK,
 * MetadataChangesFixture.java: records and items are flat objects of their envelope and body fields, and 128-bit
 * values are 32 lowercase hex characters.
 */
final class MetadataView {

    private MetadataView() {}

    static ObjectNode changes(MetadataChanges changes) {
        ObjectNode view = Canonical.object();
        view.put("status", changes.status().value());
        view.put("phase", changes.phase().value());
        view.put("log_id", changes.logId().map(MetadataView::hex128).orElse(null));
        view.put("oldest_op", changes.oldestOp());
        view.put("retained_since", changes.retainedSince());
        view.put("head_op", changes.headOp());
        view.put("next", changes.next().map(MetadataChangeCursor::toString).orElse(null));
        view.put("has_more", changes.hasMore());
        view.put(
                "current_page_token",
                changes.currentPageToken()
                        .map(MetadataCurrentPageToken::toString)
                        .orElse(null));
        view.set("records", Canonical.list(changes.records(), MetadataView::record));
        view.set("items", Canonical.list(changes.items(), MetadataView::item));
        return view;
    }

    private static ObjectNode record(MetadataChangeRecord record) {
        ObjectNode view = Canonical.object();
        view.put("op", record.op());
        view.put("ordinal", record.ordinal());
        view.put("checksum", hex128(record.checksum()));
        view.put("timestamp", record.timestamp());
        record.actorUserId().ifPresent(actor -> view.put("actor_user_id", actor));
        if (record.body() instanceof Unknown unknown) {
            view.put("group", String.valueOf(unknown.group()));
            unknown.kind().ifPresent(kind -> view.put("kind", String.valueOf(kind)));
            view.put("body", Base64.getEncoder().encodeToString(unknown.body()));
            return view;
        }
        view.put("group", record.group().orElseThrow().value());
        view.put("kind", record.kind().orElseThrow().value());
        addBody(view, record.body());
        return view;
    }

    private static ObjectNode item(MetadataCurrentItem item) {
        ObjectNode view = Canonical.object();
        if (item.body() instanceof Unknown unknown) {
            view.put("group", String.valueOf(unknown.group()));
            view.put("body", Base64.getEncoder().encodeToString(unknown.body()));
            return view;
        }
        view.put("group", item.group().orElseThrow().value());
        addBody(view, item.body());
        return view;
    }

    private static void addBody(ObjectNode view, MetadataChangeBody body) {
        if (body instanceof StreamChange stream) {
            addStreamKey(view, stream.streamId(), stream.streamCreatedOp());
            view.put("created_at", stream.createdAt());
            view.put("name", stream.name());
            addOptions(view, stream.options(), stream.derivedOptions(), stream.optionsUnreadable());
        } else if (body instanceof StreamDeleted deleted) {
            addStreamKey(view, deleted.streamId(), deleted.streamCreatedOp());
            view.put("created_at", deleted.createdAt());
            view.put("name", deleted.name());
            deleted.requestedOp().ifPresent(op -> view.put("requested_op", op));
        } else if (body instanceof TopicChange topic) {
            addTopicKey(view, topic.streamId(), topic.streamCreatedOp(), topic.topicId(), topic.topicCreatedOp());
            view.put("created_at", topic.createdAt());
            view.put("name", topic.name());
            view.put("partitions_count", topic.partitionsCount());
            addOptions(view, topic.options(), topic.derivedOptions(), topic.optionsUnreadable());
        } else if (body instanceof TopicItem topic) {
            addTopicKey(view, topic.streamId(), topic.streamCreatedOp(), topic.topicId(), topic.topicCreatedOp());
            view.put("created_at", topic.createdAt());
            view.put("name", topic.name());
            addOptions(view, topic.options(), topic.derivedOptions(), topic.optionsUnreadable());
        } else if (body instanceof TopicDeleted deleted) {
            addTopicKey(
                    view, deleted.streamId(), deleted.streamCreatedOp(), deleted.topicId(), deleted.topicCreatedOp());
            view.put("created_at", deleted.createdAt());
            view.put("name", deleted.name());
            deleted.requestedOp().ifPresent(op -> view.put("requested_op", op));
        } else if (body instanceof PartitionsChanged changed) {
            addTopicKey(
                    view, changed.streamId(), changed.streamCreatedOp(), changed.topicId(), changed.topicCreatedOp());
            view.put("partitions_count", changed.partitionsCount());
            view.set("removed_partition_ids", ids(changed.removedPartitionIds()));
            changed.requestedOp().ifPresent(op -> view.put("requested_op", op));
        } else if (body instanceof PartitionsItem partitions) {
            addTopicKey(
                    view,
                    partitions.streamId(),
                    partitions.streamCreatedOp(),
                    partitions.topicId(),
                    partitions.topicCreatedOp());
            view.put("partitions_count", partitions.partitionsCount());
        } else if (body instanceof ConsumerGroupCreated created) {
            addTopicKey(
                    view, created.streamId(), created.streamCreatedOp(), created.topicId(), created.topicCreatedOp());
            view.put("group_id", created.groupId());
            view.put("name", created.name());
            view.put("consumer_groups_count", created.consumerGroupsCount());
        } else if (body instanceof ConsumerGroupDeleted deleted) {
            addTopicKey(
                    view, deleted.streamId(), deleted.streamCreatedOp(), deleted.topicId(), deleted.topicCreatedOp());
            view.put("group_id", deleted.groupId());
            view.put("name", deleted.name());
            view.put("consumer_groups_count", deleted.consumerGroupsCount());
            deleted.requestedOp().ifPresent(op -> view.put("requested_op", op));
        } else if (body instanceof ConsumerGroupItem group) {
            addTopicKey(view, group.streamId(), group.streamCreatedOp(), group.topicId(), group.topicCreatedOp());
            view.put("group_id", group.groupId());
            view.put("name", group.name());
        } else if (body instanceof MemberJoined joined) {
            addTopicKey(view, joined.streamId(), joined.streamCreatedOp(), joined.topicId(), joined.topicCreatedOp());
            view.put("group_id", joined.groupId());
            view.put("member_id", joined.memberId());
            view.put("client_id", hex128(joined.clientId()));
            view.put("members_count", joined.membersCount());
            view.set("partition_ids", ids(joined.partitionIds()));
        } else if (body instanceof MemberLeft left) {
            addTopicKey(view, left.streamId(), left.streamCreatedOp(), left.topicId(), left.topicCreatedOp());
            view.put("group_id", left.groupId());
            view.put("member_id", left.memberId());
            view.put("client_id", hex128(left.clientId()));
            view.put("members_count", left.membersCount());
            view.put("reason", left.reason().value());
        } else if (body instanceof PartitionHandedOver handedOver) {
            addTopicKey(
                    view,
                    handedOver.streamId(),
                    handedOver.streamCreatedOp(),
                    handedOver.topicId(),
                    handedOver.topicCreatedOp());
            view.put("group_id", handedOver.groupId());
            view.put("partition_id", handedOver.partitionId());
            handedOver.memberId().ifPresent(member -> view.put("member_id", member));
        } else if (body instanceof MemberItem member) {
            addTopicKey(view, member.streamId(), member.streamCreatedOp(), member.topicId(), member.topicCreatedOp());
            view.put("group_id", member.groupId());
            view.put("member_id", member.memberId());
            view.put("client_id", hex128(member.clientId()));
            view.set("partition_ids", ids(member.partitionIds()));
        } else if (body instanceof UserChange user) {
            addUserKey(view, user.userId(), user.userCreatedAt());
            view.put("username", user.username());
            view.put("status", Canonical.status(user.status()));
            view.set("permissions", permissions(user.permissions()));
            addOptions(view, user.options(), user.derivedOptions(), user.optionsUnreadable());
        } else if (body instanceof UserIdentity user) {
            addUserKey(view, user.userId(), user.userCreatedAt());
            view.put("username", user.username());
        } else if (body instanceof UserPermissionsUpdated updated) {
            addUserKey(view, updated.userId(), updated.userCreatedAt());
            view.put("username", updated.username());
            view.set("permissions", permissions(updated.permissions()));
            view.put("reason", updated.reason().value());
        } else if (body instanceof TokenChange token) {
            addUserKey(view, token.userId(), token.userCreatedAt());
            view.put("name", token.name());
            token.expiry().ifPresent(expiry -> view.put("expiry", expiry));
        } else if (body instanceof TokenDeleted token) {
            addUserKey(view, token.userId(), token.userCreatedAt());
            view.put("name", token.name());
            view.put("reason", token.reason().value());
        } else if (body instanceof Session session) {
            addUserKey(view, session.userId(), session.userCreatedAt());
            view.put("client_id", hex128(session.clientId()));
            view.put("session", session.session());
        } else if (body instanceof SessionClosed closed) {
            addUserKey(view, closed.userId(), closed.userCreatedAt());
            view.put("client_id", hex128(closed.clientId()));
            view.put("session", closed.session());
            view.put("reason", closed.reason().value());
        } else {
            throw new IllegalStateException("an unexpected metadata body " + body);
        }
    }

    private static void addStreamKey(ObjectNode view, long streamId, BigInteger streamCreatedOp) {
        view.put("stream_id", streamId);
        view.put("stream_created_op", streamCreatedOp);
    }

    private static void addTopicKey(
            ObjectNode view, long streamId, BigInteger streamCreatedOp, long topicId, BigInteger topicCreatedOp) {
        addStreamKey(view, streamId, streamCreatedOp);
        view.put("topic_id", topicId);
        view.put("topic_created_op", topicCreatedOp);
    }

    private static void addUserKey(ObjectNode view, long userId, BigInteger userCreatedAt) {
        view.put("user_id", userId);
        view.put("user_created_at", userCreatedAt);
    }

    /** Options that the server cannot read are null, with a flag that says so. */
    private static void addOptions(
            ObjectNode view, Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived, boolean unreadable) {
        if (unreadable) {
            view.putNull("options");
            view.put("options_unreadable", true);
            return;
        }
        view.set("options", Canonical.options(explicit, derived));
    }

    /**
     * The permissions as the change log renders them, with stream permissions. The binary form has no empty stream
     * or topic map, so the SDK's empty map is the null of the JSON form.
     */
    private static JsonNode permissions(Optional<Permissions> permissions) {
        if (permissions.isEmpty()) {
            return NullNode.getInstance();
        }
        ObjectNode view = Canonical.object();
        view.set("global", Canonical.globalPermissions(permissions.get().global()));
        Map<Long, StreamPermissions> streams = permissions.get().streams();
        if (streams == null || streams.isEmpty()) {
            view.putNull("streams");
            return view;
        }
        ObjectNode streamViews = view.putObject("streams");
        streams.forEach((streamId, stream) -> {
            ObjectNode streamView = streamViews.putObject(String.valueOf(streamId));
            streamView.put("manage_stream", stream.manageStream());
            streamView.put("read_stream", stream.readStream());
            streamView.put("manage_topics", stream.manageTopics());
            streamView.put("read_topics", stream.readTopics());
            streamView.put("poll_messages", stream.pollMessages());
            streamView.put("send_messages", stream.sendMessages());
            Map<Long, TopicPermissions> topics = stream.topics();
            if (topics == null || topics.isEmpty()) {
                streamView.putNull("topics");
                return;
            }
            ObjectNode topicViews = streamView.putObject("topics");
            topics.forEach((topicId, topic) -> {
                ObjectNode topicView = topicViews.putObject(String.valueOf(topicId));
                topicView.put("manage_topic", topic.manageTopic());
                topicView.put("read_topic", topic.readTopic());
                topicView.put("poll_messages", topic.pollMessages());
                topicView.put("send_messages", topic.sendMessages());
            });
        });
        return view;
    }

    private static ArrayNode ids(List<Long> ids) {
        ArrayNode array = Canonical.array();
        ids.forEach(array::add);
        return array;
    }

    private static String hex128(BigInteger value) {
        return String.format("%032x", value);
    }
}
