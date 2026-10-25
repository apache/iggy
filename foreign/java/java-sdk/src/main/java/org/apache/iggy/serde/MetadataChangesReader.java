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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
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
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.UserStatus;

import java.math.BigInteger;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Decodes metadata changes answers. Bodies are decoded strictly: unknown reason codes, a known kind under the
 * wrong group, presence bytes other than 0 and 1 and trailing bytes are refused, while a group or kind this build
 * does not know is kept with its raw body.
 */
final class MetadataChangesReader {

    private static final int RECORD_MIN_BYTES = 8 + 4 + 16 + 8 + 1 + 1 + 1 + 4;
    private static final int ITEM_MIN_BYTES = 1 + 4;
    private static final int U128_BYTES = 16;

    private MetadataChangesReader() {}

    static MetadataChanges readAnswer(ByteBuf buffer) {
        var status = MetadataChangeStatus.fromCode(buffer.readUnsignedByte())
                .orElseThrow(() -> malformed("the status is unknown"));
        var phase = MetadataChangePhase.fromCode(buffer.readUnsignedByte())
                .orElseThrow(() -> malformed("the phase is unknown"));
        Optional<BigInteger> logId = flag(buffer, "log id presence") ? Optional.of(u128(buffer)) : Optional.empty();
        var oldestOp = BytesDeserializer.readU64AsBigInteger(buffer);
        var retainedSince = BytesDeserializer.readU64AsBigInteger(buffer);
        var headOp = BytesDeserializer.readU64AsBigInteger(buffer);
        Optional<MetadataChangeCursor> next =
                flag(buffer, "next cursor presence") ? Optional.of(cursor(buffer)) : Optional.empty();
        boolean hasMore = flag(buffer, "has more");
        List<MetadataChangeRecord> records =
                readList(buffer, RECORD_MIN_BYTES, "records", MetadataChangesReader::readRecord);
        List<MetadataCurrentItem> items = readList(buffer, ITEM_MIN_BYTES, "items", MetadataChangesReader::readItem);
        Optional<MetadataCurrentPageToken> pageToken = pageToken(buffer);
        boolean holdsOtherPhase =
                phase == MetadataChangePhase.CHANGES ? !items.isEmpty() || pageToken.isPresent() : !records.isEmpty();
        if (holdsOtherPhase) {
            throw malformed("a changes page carries only records and a current page only items");
        }
        return new MetadataChanges(
                status, phase, logId, oldestOp, retainedSince, headOp, next, hasMore, records, items, pageToken);
    }

    static MetadataChangeRecord readRecord(ByteBuf buffer) {
        var op = BytesDeserializer.readU64AsBigInteger(buffer);
        long ordinal = buffer.readUnsignedIntLE();
        var checksum = u128(buffer);
        var timestamp = BytesDeserializer.readU64AsBigInteger(buffer);
        Optional<Long> actorUserId =
                flag(buffer, "actor presence") ? Optional.of(buffer.readUnsignedIntLE()) : Optional.empty();
        int group = buffer.readUnsignedByte();
        int kind = buffer.readUnsignedByte();
        ByteBuf body = buffer.readSlice(length(buffer));
        return new MetadataChangeRecord(
                op,
                ordinal,
                checksum,
                timestamp,
                actorUserId,
                knownKind(group, kind),
                readChangeBody(group, kind, body));
    }

    static MetadataCurrentItem readItem(ByteBuf buffer) {
        int group = buffer.readUnsignedByte();
        ByteBuf body = buffer.readSlice(length(buffer));
        return new MetadataCurrentItem(MetadataChangeGroup.fromCode(group), readItemBody(group, body));
    }

    static MetadataChangeBody readChangeBody(int group, int kind, ByteBuf body) {
        return knownKind(group, kind)
                .map(known -> exact(body, changeBody(known, body)))
                .orElseGet(() -> new MetadataChangeBody.Unknown(group, Optional.of(kind), bytes(body)));
    }

    static MetadataChangeBody readItemBody(int group, ByteBuf body) {
        return MetadataChangeGroup.fromCode(group)
                .map(known -> exact(body, itemBody(known, body)))
                .orElseGet(() -> new MetadataChangeBody.Unknown(group, Optional.empty(), bytes(body)));
    }

    private static Optional<MetadataChangeKind> knownKind(int group, int kind) {
        Optional<MetadataChangeGroup> knownGroup = MetadataChangeGroup.fromCode(group);
        Optional<MetadataChangeKind> knownKind = MetadataChangeKind.fromCode(kind);
        if (knownGroup.isEmpty() || knownKind.isEmpty()) {
            return Optional.empty();
        }
        if (knownKind.get().group() != knownGroup.get()) {
            throw malformed("change kind " + kind + " does not belong to group " + group);
        }
        return knownKind;
    }

    private static MetadataChangeBody exact(ByteBuf body, MetadataChangeBody decoded) {
        if (body.isReadable()) {
            throw malformed(body.readableBytes() + " trailing bytes after a body");
        }
        return decoded;
    }

    private static <T> List<T> readList(ByteBuf buffer, int minimumBytes, String field, Function<ByteBuf, T> reader) {
        int count = BytesDeserializer.validatedCollectionSize(
                buffer.readUnsignedIntLE(), buffer.readableBytes(), minimumBytes, "Metadata changes " + field);
        List<T> values = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            values.add(reader.apply(buffer));
        }
        return values;
    }

    private static Optional<MetadataCurrentPageToken> pageToken(ByteBuf buffer) {
        int length = buffer.readUnsignedShortLE();
        if (length == 0) {
            return Optional.empty();
        }
        byte[] payload = new byte[length];
        buffer.readBytes(payload);
        return Optional.of(MetadataCurrentPageToken.fromPayload(payload)
                .orElseThrow(() -> malformed("the page token is malformed")));
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private static MetadataChangeBody changeBody(MetadataChangeKind kind, ByteBuf body) {
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

    private static MetadataChangeBody itemBody(MetadataChangeGroup group, ByteBuf body) {
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

    private static MetadataChangeBody stream(ByteBuf body) {
        long streamId = body.readUnsignedIntLE();
        var streamCreatedOp = BytesDeserializer.readU64AsBigInteger(body);
        var createdAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        var options = options(body);
        return new MetadataChangeBody.StreamChange(
                streamId,
                streamCreatedOp,
                createdAt,
                name,
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody streamDeleted(ByteBuf body) {
        long streamId = body.readUnsignedIntLE();
        var streamCreatedOp = BytesDeserializer.readU64AsBigInteger(body);
        var createdAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        return new MetadataChangeBody.StreamDeleted(streamId, streamCreatedOp, createdAt, name, optionalU64(body));
    }

    private static MetadataChangeBody topic(ByteBuf body) {
        var key = TopicKey.read(body);
        var createdAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        long partitionsCount = body.readUnsignedIntLE();
        var options = options(body);
        return new MetadataChangeBody.TopicChange(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                createdAt,
                name,
                partitionsCount,
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody topicItem(ByteBuf body) {
        var key = TopicKey.read(body);
        var createdAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        var options = options(body);
        return new MetadataChangeBody.TopicItem(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                createdAt,
                name,
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody topicDeleted(ByteBuf body) {
        var key = TopicKey.read(body);
        var createdAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        return new MetadataChangeBody.TopicDeleted(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                createdAt,
                name,
                optionalU64(body));
    }

    private static MetadataChangeBody partitionsChanged(ByteBuf body) {
        var key = TopicKey.read(body);
        long partitionsCount = body.readUnsignedIntLE();
        var removed = partitionIds(body);
        return new MetadataChangeBody.PartitionsChanged(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                partitionsCount,
                removed,
                optionalU64(body));
    }

    private static MetadataChangeBody partitionsItem(ByteBuf body) {
        var key = TopicKey.read(body);
        return new MetadataChangeBody.PartitionsItem(
                key.streamId(), key.streamCreatedOp(), key.topicId(), key.topicCreatedOp(), body.readUnsignedIntLE());
    }

    private static MetadataChangeBody consumerGroupCreated(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        var name = name(body);
        return new MetadataChangeBody.ConsumerGroupCreated(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                name,
                body.readUnsignedIntLE());
    }

    private static MetadataChangeBody consumerGroupDeleted(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        var name = name(body);
        long consumerGroupsCount = body.readUnsignedIntLE();
        return new MetadataChangeBody.ConsumerGroupDeleted(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                name,
                consumerGroupsCount,
                optionalU64(body));
    }

    private static MetadataChangeBody consumerGroupItem(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        return new MetadataChangeBody.ConsumerGroupItem(
                key.streamId(), key.streamCreatedOp(), key.topicId(), key.topicCreatedOp(), groupId, name(body));
    }

    private static MetadataChangeBody memberJoined(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        long memberId = body.readUnsignedIntLE();
        var clientId = u128(body);
        long membersCount = body.readUnsignedIntLE();
        return new MetadataChangeBody.MemberJoined(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                memberId,
                clientId,
                membersCount,
                partitionIds(body));
    }

    private static MetadataChangeBody memberLeft(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        long memberId = body.readUnsignedIntLE();
        var clientId = u128(body);
        long membersCount = body.readUnsignedIntLE();
        return new MetadataChangeBody.MemberLeft(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                memberId,
                clientId,
                membersCount,
                reason(body, MetadataChangeBody.MemberLeftReason::fromCode, "member left reason"));
    }

    private static MetadataChangeBody handedOver(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        long partitionId = body.readUnsignedIntLE();
        Optional<Long> memberId =
                flag(body, "member presence") ? Optional.of(body.readUnsignedIntLE()) : Optional.empty();
        return new MetadataChangeBody.PartitionHandedOver(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                partitionId,
                memberId);
    }

    private static MetadataChangeBody memberItem(ByteBuf body) {
        var key = TopicKey.read(body);
        long groupId = body.readUnsignedIntLE();
        long memberId = body.readUnsignedIntLE();
        var clientId = u128(body);
        return new MetadataChangeBody.MemberItem(
                key.streamId(),
                key.streamCreatedOp(),
                key.topicId(),
                key.topicCreatedOp(),
                groupId,
                memberId,
                clientId,
                partitionIds(body));
    }

    private static MetadataChangeBody user(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var username = name(body);
        UserStatus status = userStatus(body.readUnsignedByte());
        var permissions = permissions(body);
        var options = options(body);
        return new MetadataChangeBody.UserChange(
                userId,
                userCreatedAt,
                username,
                status,
                permissions,
                options.explicit(),
                options.derived(),
                options.unreadable());
    }

    private static MetadataChangeBody userIdentity(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        return new MetadataChangeBody.UserIdentity(userId, userCreatedAt, name(body));
    }

    private static MetadataChangeBody userPermissions(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var username = name(body);
        var permissions = permissions(body);
        return new MetadataChangeBody.UserPermissionsUpdated(
                userId,
                userCreatedAt,
                username,
                permissions,
                reason(body, MetadataChangeBody.PermissionsUpdatedReason::fromCode, "permissions updated reason"));
    }

    private static MetadataChangeBody token(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        var expiry = BytesDeserializer.readU64AsBigInteger(body);
        return new MetadataChangeBody.TokenChange(
                userId, userCreatedAt, name, expiry.signum() == 0 ? Optional.empty() : Optional.of(expiry));
    }

    private static MetadataChangeBody tokenDeleted(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var name = name(body);
        return new MetadataChangeBody.TokenDeleted(
                userId,
                userCreatedAt,
                name,
                reason(body, MetadataChangeBody.TokenDeletedReason::fromCode, "token deleted reason"));
    }

    private static MetadataChangeBody session(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var clientId = u128(body);
        return new MetadataChangeBody.Session(
                userId, userCreatedAt, clientId, BytesDeserializer.readU64AsBigInteger(body));
    }

    private static MetadataChangeBody sessionClosed(ByteBuf body) {
        long userId = body.readUnsignedIntLE();
        var userCreatedAt = BytesDeserializer.readU64AsBigInteger(body);
        var clientId = u128(body);
        var session = BytesDeserializer.readU64AsBigInteger(body);
        return new MetadataChangeBody.SessionClosed(
                userId,
                userCreatedAt,
                clientId,
                session,
                reason(body, MetadataChangeBody.SessionClosedReason::fromCode, "session closed reason"));
    }

    private static UserStatus userStatus(int code) {
        try {
            return UserStatus.fromCode(code);
        } catch (IggyInvalidArgumentException unknown) {
            throw malformed("user status " + code + " is unknown");
        }
    }

    /**
     * Reads {@code [state:u8]}: 0 followed by the explicit and derived option blocks exactly as the read responses
     * carry them, or 1 for stored state the server cannot read.
     */
    private static ChangeOptions options(ByteBuf body) {
        int state = body.readUnsignedByte();
        if (state == 1) {
            return new ChangeOptions(Map.of(), Map.of(), true);
        }
        if (state != 0) {
            throw malformed("options state " + state + " is unknown");
        }
        var explicit = BytesDeserializer.readOptionsBlock(body, "change explicit options");
        var derived = BytesDeserializer.readOptionsBlock(body, "change derived options");
        return new ChangeOptions(explicit, derived, false);
    }

    /** Reads the optional public permission set: ten global flags, then streams and their topics. */
    private static Optional<Permissions> permissions(ByteBuf body) {
        if (!flag(body, "permissions presence")) {
            return Optional.empty();
        }
        var global = BytesDeserializer.readGlobalPermissions(body);
        Map<Long, StreamPermissions> streams = new HashMap<>();
        while (body.readBoolean()) {
            long streamId = body.readUnsignedIntLE();
            streams.put(streamId, BytesDeserializer.readStreamPermissions(body));
        }
        return Optional.of(new Permissions(global, streams));
    }

    /** Reads {@code [count:u32][id:u32]*}, refusing ids that are not strictly ascending. */
    private static List<Long> partitionIds(ByteBuf body) {
        int count = BytesDeserializer.validatedCollectionSize(
                body.readUnsignedIntLE(), body.readableBytes(), Integer.BYTES, "Metadata change partition ids");
        List<Long> ids = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            long id = body.readUnsignedIntLE();
            if (!ids.isEmpty() && ids.get(ids.size() - 1) >= id) {
                throw malformed("partition ids must be strictly ascending");
            }
            ids.add(id);
        }
        return List.copyOf(ids);
    }

    private static <T> T reason(ByteBuf body, IntFunction<Optional<T>> fromCode, String field) {
        int code = body.readUnsignedByte();
        return fromCode.apply(code).orElseThrow(() -> malformed(field + " " + code + " is unknown"));
    }

    private static boolean flag(ByteBuf buffer, String field) {
        int value = buffer.readUnsignedByte();
        if (value > 1) {
            throw malformed(field + " byte " + value + " is neither 0 nor 1");
        }
        return value == 1;
    }

    private static Optional<BigInteger> optionalU64(ByteBuf body) {
        return flag(body, "requested op presence")
                ? Optional.of(BytesDeserializer.readU64AsBigInteger(body))
                : Optional.empty();
    }

    private static String name(ByteBuf body) {
        int length = body.readUnsignedByte();
        if (length == 0) {
            throw malformed("a name is empty");
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(body.readSlice(length).nioBuffer())
                    .toString();
        } catch (CharacterCodingException notUtf8) {
            throw malformed("a name is not UTF-8");
        }
    }

    private static MetadataChangeCursor cursor(ByteBuf buffer) {
        byte[] bytes = new byte[MetadataChangeCursor.SIZE];
        buffer.readBytes(bytes);
        var cursor = MetadataChangeCursor.fromBytes(bytes, 0);
        if (!cursor.isCanonical()) {
            throw malformed("a cursor at op 0 carries a checksum or ordinal");
        }
        return cursor;
    }

    static BigInteger u128(ByteBuf buffer) {
        byte[] bytes = new byte[U128_BYTES];
        buffer.readBytes(bytes);
        byte[] bigEndian = new byte[U128_BYTES];
        for (int index = 0; index < U128_BYTES; index++) {
            bigEndian[U128_BYTES - 1 - index] = bytes[index];
        }
        return new BigInteger(1, bigEndian);
    }

    private static int length(ByteBuf buffer) {
        long length = buffer.readUnsignedIntLE();
        if (length > buffer.readableBytes()) {
            throw malformed("a body of " + length + " bytes overruns the remaining " + buffer.readableBytes());
        }
        return (int) length;
    }

    private static byte[] bytes(ByteBuf body) {
        byte[] bytes = new byte[body.readableBytes()];
        body.readBytes(bytes);
        return bytes;
    }

    static IggyMalformedResponseException malformed(String reason) {
        return new IggyMalformedResponseException("Malformed metadata changes answer: " + reason);
    }

    private record ChangeOptions(
            Map<String, HeaderValue> explicit, Map<String, HeaderValue> derived, boolean unreadable) {}

    private record TopicKey(long streamId, BigInteger streamCreatedOp, long topicId, BigInteger topicCreatedOp) {
        static TopicKey read(ByteBuf body) {
            long streamId = body.readUnsignedIntLE();
            var streamCreatedOp = BytesDeserializer.readU64AsBigInteger(body);
            long topicId = body.readUnsignedIntLE();
            return new TopicKey(streamId, streamCreatedOp, topicId, BytesDeserializer.readU64AsBigInteger(body));
        }
    }
}
