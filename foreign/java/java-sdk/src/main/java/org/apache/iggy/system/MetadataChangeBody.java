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

package org.apache.iggy.system;

import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.stream.EncryptionKeySource;
import org.apache.iggy.stream.StreamOptionKey;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserStatus;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The body of a metadata change record or current state item. Streams are named by
 * {@code (streamId, streamCreatedOp)}, topics by their stream key plus {@code (topicId, topicCreatedOp)} and users
 * by {@code (userId, userCreatedAt)}, because ids are reused after a delete.
 *
 * <p>Bodies with options carry the public projection the read routes return, split into what a client set and
 * what the server derived. When the server cannot read the stored state both maps are empty and
 * {@code optionsUnreadable} is set, and a mirror keeps its previous values.
 */
public sealed interface MetadataChangeBody {

    /** Why a consumer group member was removed. */
    enum MemberLeftReason {
        /** The member left the group. */
        LEFT,
        /** The member's session ended. */
        SESSION_ENDED
    }

    /** Why a personal access token was removed. */
    enum TokenDeletedReason {
        /** A client deleted the token. */
        DELETED,
        /** The expiry cleaner removed the token. */
        EXPIRED
    }

    /** Why a session was closed. */
    enum SessionClosedReason {
        /** The client logged out. */
        LOGOUT,
        /** The session lease expired. */
        EXPIRED
    }

    /** Why a user's public permission set changed. */
    enum PermissionsUpdatedReason {
        /** A permissions update replaced the set. */
        UPDATED,
        /** A stream or topic delete pruned the user's grants on it. */
        RESOURCE_DELETED
    }

    /** A stream after a create or update, and a streams item. */
    record StreamChange(
            long streamId,
            BigInteger streamCreatedOp,
            BigInteger createdAt,
            String name,
            Map<String, HeaderValue> options,
            Map<String, HeaderValue> derivedOptions,
            boolean optionsUnreadable)
            implements MetadataChangeBody {

        /** Who chose the key that encrypts the stream at rest, empty when the record reports no source. */
        public Optional<EncryptionKeySource> encryptionKeySource() {
            return text(StreamOptionKey.ENCRYPTION_KEY_SOURCE).flatMap(EncryptionKeySource::fromValue);
        }

        /** Fingerprint of the stream's key, empty for a plaintext stream. */
        public Optional<String> encryptionKeyFingerprint() {
            return text(StreamOptionKey.ENCRYPTION_KEY_FINGERPRINT);
        }

        private Optional<String> text(StreamOptionKey key) {
            HeaderValue value = derivedOptions.get(key.value());
            if (value == null) {
                value = options.get(key.value());
            }
            return Optional.ofNullable(value)
                    .filter(found -> found.kind() == HeaderKind.String)
                    .map(HeaderValue::asString);
        }
    }

    /** A removed stream. {@code requestedOp} is the op of a deferred delete's request. */
    record StreamDeleted(
            long streamId,
            BigInteger streamCreatedOp,
            BigInteger createdAt,
            String name,
            Optional<BigInteger> requestedOp)
            implements MetadataChangeBody {}

    /** A topic after a create or update. */
    record TopicChange(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            BigInteger createdAt,
            String name,
            long partitionsCount,
            Map<String, HeaderValue> options,
            Map<String, HeaderValue> derivedOptions,
            boolean optionsUnreadable)
            implements MetadataChangeBody {}

    /** A topics item. Partition counts travel as partitions items. */
    record TopicItem(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            BigInteger createdAt,
            String name,
            Map<String, HeaderValue> options,
            Map<String, HeaderValue> derivedOptions,
            boolean optionsUnreadable)
            implements MetadataChangeBody {}

    /** A removed topic. */
    record TopicDeleted(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            BigInteger createdAt,
            String name,
            Optional<BigInteger> requestedOp)
            implements MetadataChangeBody {}

    /**
     * A partition count change. {@code removedPartitionIds} lists the deleted ids in ascending order and is empty
     * on create. Stored ids can be sparse, so the count is not an id boundary.
     */
    record PartitionsChanged(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long partitionsCount,
            List<Long> removedPartitionIds,
            Optional<BigInteger> requestedOp)
            implements MetadataChangeBody {}

    /** A partitions item. */
    record PartitionsItem(
            long streamId, BigInteger streamCreatedOp, long topicId, BigInteger topicCreatedOp, long partitionsCount)
            implements MetadataChangeBody {}

    /** A created consumer group with the topic's resulting group count. */
    record ConsumerGroupCreated(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            String name,
            long consumerGroupsCount)
            implements MetadataChangeBody {}

    /** A removed consumer group with the topic's resulting group count. */
    record ConsumerGroupDeleted(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            String name,
            long consumerGroupsCount,
            Optional<BigInteger> requestedOp)
            implements MetadataChangeBody {}

    /** A consumer groups item. */
    record ConsumerGroupItem(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            String name)
            implements MetadataChangeBody {}

    /** A member that joined, with its installed partitions, normally none because a handover installs them. */
    record MemberJoined(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            long memberId,
            BigInteger clientId,
            long membersCount,
            List<Long> partitionIds)
            implements MetadataChangeBody {}

    /** A member that left the group. */
    record MemberLeft(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            long memberId,
            BigInteger clientId,
            long membersCount,
            MemberLeftReason reason)
            implements MetadataChangeBody {}

    /** A completed partition handover. {@code memberId} is empty when the last member left. */
    record PartitionHandedOver(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            long partitionId,
            Optional<Long> memberId)
            implements MetadataChangeBody {}

    /** A consumer group members item. */
    record MemberItem(
            long streamId,
            BigInteger streamCreatedOp,
            long topicId,
            BigInteger topicCreatedOp,
            long groupId,
            long memberId,
            BigInteger clientId,
            List<Long> partitionIds)
            implements MetadataChangeBody {}

    /** A user after a create or update, and a users item. */
    record UserChange(
            long userId,
            BigInteger userCreatedAt,
            String username,
            UserStatus status,
            Optional<Permissions> permissions,
            Map<String, HeaderValue> options,
            Map<String, HeaderValue> derivedOptions,
            boolean optionsUnreadable)
            implements MetadataChangeBody {}

    /** A deleted user, or one whose password changed. */
    record UserIdentity(long userId, BigInteger userCreatedAt, String username) implements MetadataChangeBody {}

    /** A user's public permission set after a change. */
    record UserPermissionsUpdated(
            long userId,
            BigInteger userCreatedAt,
            String username,
            Optional<Permissions> permissions,
            PermissionsUpdatedReason reason)
            implements MetadataChangeBody {}

    /** A created personal access token, and a personal access tokens item. Never the token itself. */
    record TokenChange(long userId, BigInteger userCreatedAt, String name, Optional<BigInteger> expiry)
            implements MetadataChangeBody {}

    /** A removed personal access token. */
    record TokenDeleted(long userId, BigInteger userCreatedAt, String name, TokenDeletedReason reason)
            implements MetadataChangeBody {}

    /** A registered session, and a sessions item. */
    record Session(long userId, BigInteger userCreatedAt, BigInteger clientId, BigInteger session)
            implements MetadataChangeBody {}

    /** A closed session. */
    record SessionClosed(
            long userId, BigInteger userCreatedAt, BigInteger clientId, BigInteger session, SessionClosedReason reason)
            implements MetadataChangeBody {}

    /**
     * A binary record or item of a group or kind this build does not know, kept with its codes and raw body.
     *
     * @param group the group code
     * @param kind the kind code, empty for an item
     * @param body the body exactly as the server sent it
     */
    record Unknown(int group, Optional<Integer> kind, byte[] body) implements MetadataChangeBody {}

    /**
     * A JSON record or item of a group or kind this build does not know, kept as the whole object.
     *
     * @param group the group name
     * @param kind the kind name, empty for an item
     * @param json the record or item object
     */
    record UnknownJson(String group, Optional<String> kind, String json) implements MetadataChangeBody {}
}
