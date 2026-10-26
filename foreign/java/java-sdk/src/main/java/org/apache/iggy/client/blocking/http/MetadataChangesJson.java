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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeCursor;
import org.apache.iggy.system.MetadataChangeGroup;
import org.apache.iggy.system.MetadataChangeKind;
import org.apache.iggy.system.MetadataChangePhase;
import org.apache.iggy.system.MetadataChangeRecord;
import org.apache.iggy.system.MetadataChangeStatus;
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataCurrentItem;
import org.apache.iggy.system.MetadataCurrentPageToken;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserStatus;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Reads the JSON answer of {@code GET /metadata/changes}. Records and items are flat objects of the envelope fields
 * and the body fields, 128-bit values are 32 lowercase hex characters, and a record or item of a group or kind this
 * build does not know is kept whole as {@link MetadataChangeBody.UnknownJson}.
 */
final class MetadataChangesJson {

    private static final Pattern HEX_U128 = Pattern.compile("[0-9a-f]{32}");
    private static final TypeReference<Map<String, HttpOptionValue>> OPTIONS = new TypeReference<>() {};
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    private MetadataChangesJson() {}

    static MetadataChanges read(JsonNode answer) {
        Optional<BigInteger> logId =
                present(answer, "log_id") ? Optional.of(hexU128(answer, "log_id")) : Optional.empty();
        Optional<MetadataChangeCursor> next = present(answer, "next")
                ? Optional.of(parsed(answer, "next", MetadataChangeCursor::parse))
                : Optional.empty();
        Optional<MetadataCurrentPageToken> pageToken = present(answer, "current_page_token")
                ? Optional.of(parsed(answer, "current_page_token", MetadataCurrentPageToken::parse))
                : Optional.empty();
        return new MetadataChanges(
                MetadataChangeStatus.fromValue(text(answer, "status"))
                        .orElseThrow(() -> malformed("status is unknown")),
                MetadataChangePhase.fromValue(text(answer, "phase")).orElseThrow(() -> malformed("phase is unknown")),
                logId,
                u64(answer, "oldest_op"),
                u64(answer, "retained_since"),
                u64(answer, "head_op"),
                next,
                bool(answer, "has_more"),
                list(answer, "records", MetadataChangesJson::record),
                list(answer, "items", MetadataChangesJson::item),
                pageToken);
    }

    static MetadataChangeRecord record(JsonNode record) {
        String groupName = text(record, "group");
        String kindName = text(record, "kind");
        Optional<MetadataChangeKind> kind = knownKind(groupName, kindName);
        MetadataChangeBody body = kind.map(known -> changeBody(known, record))
                .orElseGet(
                        () -> new MetadataChangeBody.UnknownJson(groupName, Optional.of(kindName), record.toString()));
        Optional<Long> actorUserId =
                present(record, "actor_user_id") ? Optional.of(u32(record, "actor_user_id")) : Optional.empty();
        return new MetadataChangeRecord(
                u64(record, "op"),
                u32(record, "ordinal"),
                hexU128(record, "checksum"),
                u64(record, "timestamp"),
                actorUserId,
                kind,
                body);
    }

    static MetadataCurrentItem item(JsonNode item) {
        String groupName = text(item, "group");
        Optional<MetadataChangeGroup> group = MetadataChangeGroup.fromValue(groupName);
        MetadataChangeBody body = group.map(known -> itemBody(known, item))
                .orElseGet(() -> new MetadataChangeBody.UnknownJson(groupName, Optional.empty(), item.toString()));
        return new MetadataCurrentItem(group, body);
    }

    private static Optional<MetadataChangeKind> knownKind(String groupName, String kindName) {
        Optional<MetadataChangeGroup> group = MetadataChangeGroup.fromValue(groupName);
        Optional<MetadataChangeKind> kind = MetadataChangeKind.fromValue(kindName);
        if (group.isEmpty() || kind.isEmpty()) {
            return Optional.empty();
        }
        if (kind.get().group() != group.get()) {
            throw malformed("change kind " + kindName + " does not belong to group " + groupName);
        }
        return kind;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static MetadataChangeBody changeBody(MetadataChangeKind kind, JsonNode body) {
        return switch (kind) {
            case STREAM_CREATED, STREAM_UPDATED -> stream(body);
            case STREAM_DELETED -> streamDeleted(body);
            case TOPIC_CREATED, TOPIC_UPDATED -> topic(body);
            case TOPIC_DELETED -> topicDeleted(body);
            case PARTITIONS_CHANGED -> partitionsChanged(body);
            case CONSUMER_GROUP_CREATED -> consumerGroupCreated(body);
            case CONSUMER_GROUP_DELETED -> consumerGroupDeleted(body);
            case CONSUMER_GROUP_MEMBER_JOINED -> memberJoined(body);
            case CONSUMER_GROUP_MEMBER_LEFT -> memberLeft(body);
            case CONSUMER_GROUP_PARTITION_HANDED_OVER -> handedOver(body);
            case USER_CREATED, USER_UPDATED -> user(body);
            case USER_DELETED, USER_PASSWORD_CHANGED -> userIdentity(body);
            case USER_PERMISSIONS_UPDATED -> userPermissions(body);
            case PERSONAL_ACCESS_TOKEN_CREATED -> token(body);
            case PERSONAL_ACCESS_TOKEN_DELETED -> tokenDeleted(body);
            case SESSION_REGISTERED -> session(body);
            case SESSION_CLOSED -> sessionClosed(body);
        };
    }

    private static MetadataChangeBody itemBody(MetadataChangeGroup group, JsonNode body) {
        return switch (group) {
            case STREAMS -> stream(body);
            case TOPICS -> topicItem(body);
            case PARTITIONS -> partitionsItem(body);
            case CONSUMER_GROUPS -> consumerGroupItem(body);
            case CONSUMER_GROUP_MEMBERS -> memberItem(body);
            case USERS -> user(body);
            case PERSONAL_ACCESS_TOKENS -> token(body);
            case SESSIONS -> session(body);
        };
    }

    private static MetadataChangeBody stream(JsonNode body) {
        var options = options(body);
        return new MetadataChangeBody.StreamChange(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u64(body, "created_at"),
                text(body, "name"),
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody streamDeleted(JsonNode body) {
        return new MetadataChangeBody.StreamDeleted(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u64(body, "created_at"),
                text(body, "name"),
                optionalU64(body, "requested_op"));
    }

    private static MetadataChangeBody topic(JsonNode body) {
        var options = options(body);
        return new MetadataChangeBody.TopicChange(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u64(body, "created_at"),
                text(body, "name"),
                u32(body, "partitions_count"),
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody topicItem(JsonNode body) {
        var options = options(body);
        return new MetadataChangeBody.TopicItem(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u64(body, "created_at"),
                text(body, "name"),
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody topicDeleted(JsonNode body) {
        return new MetadataChangeBody.TopicDeleted(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u64(body, "created_at"),
                text(body, "name"),
                optionalU64(body, "requested_op"));
    }

    private static MetadataChangeBody partitionsChanged(JsonNode body) {
        return new MetadataChangeBody.PartitionsChanged(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "partitions_count"),
                ids(body, "removed_partition_ids"),
                optionalU64(body, "requested_op"));
    }

    private static MetadataChangeBody partitionsItem(JsonNode body) {
        return new MetadataChangeBody.PartitionsItem(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "partitions_count"));
    }

    private static MetadataChangeBody consumerGroupCreated(JsonNode body) {
        return new MetadataChangeBody.ConsumerGroupCreated(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                text(body, "name"),
                u32(body, "consumer_groups_count"));
    }

    private static MetadataChangeBody consumerGroupDeleted(JsonNode body) {
        return new MetadataChangeBody.ConsumerGroupDeleted(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                text(body, "name"),
                u32(body, "consumer_groups_count"),
                optionalU64(body, "requested_op"));
    }

    private static MetadataChangeBody consumerGroupItem(JsonNode body) {
        return new MetadataChangeBody.ConsumerGroupItem(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                text(body, "name"));
    }

    private static MetadataChangeBody memberJoined(JsonNode body) {
        return new MetadataChangeBody.MemberJoined(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                u32(body, "member_id"),
                hexU128(body, "client_id"),
                u32(body, "members_count"),
                ids(body, "partition_ids"));
    }

    private static MetadataChangeBody memberLeft(JsonNode body) {
        return new MetadataChangeBody.MemberLeft(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                u32(body, "member_id"),
                hexU128(body, "client_id"),
                u32(body, "members_count"),
                reason(body, MetadataChangeBody.MemberLeftReason::fromValue));
    }

    private static MetadataChangeBody handedOver(JsonNode body) {
        Optional<Long> memberId = present(body, "member_id") ? Optional.of(u32(body, "member_id")) : Optional.empty();
        return new MetadataChangeBody.PartitionHandedOver(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                u32(body, "partition_id"),
                memberId);
    }

    private static MetadataChangeBody memberItem(JsonNode body) {
        return new MetadataChangeBody.MemberItem(
                u32(body, "stream_id"),
                u64(body, "stream_created_op"),
                u32(body, "topic_id"),
                u64(body, "topic_created_op"),
                u32(body, "group_id"),
                u32(body, "member_id"),
                hexU128(body, "client_id"),
                ids(body, "partition_ids"));
    }

    private static MetadataChangeBody user(JsonNode body) {
        var options = options(body);
        UserStatus status = switch (text(body, "status")) {
            case "active" -> UserStatus.Active;
            case "inactive" -> UserStatus.Inactive;
            default -> throw malformed("user status " + text(body, "status") + " is unknown");
        };
        return new MetadataChangeBody.UserChange(
                u32(body, "user_id"),
                u64(body, "user_created_at"),
                text(body, "username"),
                status,
                permissions(body),
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody userIdentity(JsonNode body) {
        return new MetadataChangeBody.UserIdentity(
                u32(body, "user_id"), u64(body, "user_created_at"), text(body, "username"));
    }

    private static MetadataChangeBody userPermissions(JsonNode body) {
        return new MetadataChangeBody.UserPermissionsUpdated(
                u32(body, "user_id"),
                u64(body, "user_created_at"),
                text(body, "username"),
                permissions(body),
                reason(body, MetadataChangeBody.PermissionsUpdatedReason::fromValue));
    }

    private static MetadataChangeBody token(JsonNode body) {
        return new MetadataChangeBody.TokenChange(
                u32(body, "user_id"), u64(body, "user_created_at"), text(body, "name"), optionalU64(body, "expiry"));
    }

    private static MetadataChangeBody tokenDeleted(JsonNode body) {
        return new MetadataChangeBody.TokenDeleted(
                u32(body, "user_id"),
                u64(body, "user_created_at"),
                text(body, "name"),
                reason(body, MetadataChangeBody.TokenDeletedReason::fromValue));
    }

    private static MetadataChangeBody session(JsonNode body) {
        return new MetadataChangeBody.Session(
                u32(body, "user_id"), u64(body, "user_created_at"), hexU128(body, "client_id"), u64(body, "session"));
    }

    private static MetadataChangeBody sessionClosed(JsonNode body) {
        return new MetadataChangeBody.SessionClosed(
                u32(body, "user_id"),
                u64(body, "user_created_at"),
                hexU128(body, "client_id"),
                u64(body, "session"),
                reason(body, MetadataChangeBody.SessionClosedReason::fromValue));
    }

    /**
     * Reads {@code options} and {@code options_unreadable}: an object of {@code value}, {@code kind} and
     * {@code explicit} entries with the flag absent or false, or null with the flag true.
     */
    private static ChangeOptions options(JsonNode body) {
        boolean unreadable = present(body, "options_unreadable") && bool(body, "options_unreadable");
        boolean readable = present(body, "options");
        if (readable == unreadable) {
            throw malformed("options must be an object, or null with options_unreadable true");
        }
        if (unreadable) {
            return new ChangeOptions(Map.of(), Map.of(), true);
        }
        Map<String, HttpOptionValue> entries;
        try {
            entries = OBJECT_MAPPER.convertValue(body.get("options"), OPTIONS);
        } catch (JacksonException | IllegalArgumentException invalid) {
            throw new IggyMalformedResponseException("Malformed metadata changes answer: options", invalid);
        }
        return new ChangeOptions(HttpOptionValue.select(entries, true), HttpOptionValue.select(entries, false), false);
    }

    private static Optional<Permissions> permissions(JsonNode body) {
        if (!present(body, "permissions")) {
            return Optional.empty();
        }
        try {
            return Optional.of(OBJECT_MAPPER.treeToValue(body.get("permissions"), Permissions.class));
        } catch (JacksonException | IllegalArgumentException invalid) {
            throw new IggyMalformedResponseException("Malformed metadata changes answer: permissions", invalid);
        }
    }

    private static <T> T reason(JsonNode body, Function<String, Optional<T>> fromValue) {
        String name = text(body, "reason");
        return fromValue.apply(name).orElseThrow(() -> malformed("reason " + name + " is unknown"));
    }

    private static <T> List<T> list(JsonNode body, String field, Function<JsonNode, T> reader) {
        JsonNode values = required(body, field);
        if (!values.isArray()) {
            throw malformed(field + " is not an array");
        }
        List<T> result = new ArrayList<>(values.size());
        values.forEach(value -> result.add(reader.apply(value)));
        return result;
    }

    private static List<Long> ids(JsonNode body, String field) {
        JsonNode values = required(body, field);
        if (!values.isArray()) {
            throw malformed(field + " is not an array");
        }
        List<Long> result = new ArrayList<>(values.size());
        values.forEach(value -> result.add(unsigned(value, Integer.SIZE, field).longValueExact()));
        return List.copyOf(result);
    }

    private static BigInteger u64(JsonNode body, String field) {
        return unsigned(required(body, field), Long.SIZE, field);
    }

    private static long u32(JsonNode body, String field) {
        return unsigned(required(body, field), Integer.SIZE, field).longValueExact();
    }

    private static Optional<BigInteger> optionalU64(JsonNode body, String field) {
        return present(body, field) ? Optional.of(u64(body, field)) : Optional.empty();
    }

    private static BigInteger unsigned(JsonNode value, int bits, String field) {
        if (!value.isIntegralNumber()) {
            throw malformed(field + " is not an integer");
        }
        BigInteger number = value.bigIntegerValue();
        if (number.signum() < 0 || number.bitLength() > bits) {
            throw malformed(field + " is outside an unsigned " + bits + "-bit value");
        }
        return number;
    }

    private static <T> T parsed(JsonNode body, String field, Function<String, T> parser) {
        try {
            return parser.apply(text(body, field));
        } catch (IggyValidationException refused) {
            throw new IggyMalformedResponseException("Malformed metadata changes answer: " + field, refused);
        }
    }

    private static BigInteger hexU128(JsonNode body, String field) {
        String hex = text(body, field);
        if (!HEX_U128.matcher(hex).matches()) {
            throw malformed(field + " must be 32 lowercase hex characters");
        }
        return new BigInteger(hex, 16);
    }

    private static boolean bool(JsonNode body, String field) {
        JsonNode value = required(body, field);
        if (!value.isBoolean()) {
            throw malformed(field + " is not a boolean");
        }
        return value.booleanValue();
    }

    private static String text(JsonNode body, String field) {
        JsonNode value = required(body, field);
        if (!value.isString()) {
            throw malformed(field + " is not a string");
        }
        return value.stringValue();
    }

    private static boolean present(JsonNode body, String field) {
        JsonNode value = body.get(field);
        return value != null && !value.isNull();
    }

    private static JsonNode required(JsonNode body, String field) {
        JsonNode value = body.get(field);
        if (value == null) {
            throw malformed(field + " is missing");
        }
        return value;
    }

    private static IggyMalformedResponseException malformed(String reason) {
        return new IggyMalformedResponseException("Malformed metadata changes answer: " + reason);
    }

    private record ChangeOptions(
            Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived, boolean unreadable) {}
}
