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

import java.util.Optional;

/** Kind of a metadata change record. The codes are the wire codes, and a kind always belongs to one group. */
public enum MetadataChangeKind {
    STREAM_CREATED(1, "stream_created", MetadataChangeGroup.STREAMS),
    STREAM_UPDATED(2, "stream_updated", MetadataChangeGroup.STREAMS),
    STREAM_DELETED(3, "stream_deleted", MetadataChangeGroup.STREAMS),
    TOPIC_CREATED(10, "topic_created", MetadataChangeGroup.TOPICS),
    TOPIC_UPDATED(11, "topic_updated", MetadataChangeGroup.TOPICS),
    TOPIC_DELETED(12, "topic_deleted", MetadataChangeGroup.TOPICS),
    PARTITIONS_CHANGED(20, "partitions_changed", MetadataChangeGroup.PARTITIONS),
    CONSUMER_GROUP_CREATED(30, "consumer_group_created", MetadataChangeGroup.CONSUMER_GROUPS),
    CONSUMER_GROUP_DELETED(31, "consumer_group_deleted", MetadataChangeGroup.CONSUMER_GROUPS),
    CONSUMER_GROUP_MEMBER_JOINED(40, "consumer_group_member_joined", MetadataChangeGroup.CONSUMER_GROUP_MEMBERS),
    CONSUMER_GROUP_MEMBER_LEFT(41, "consumer_group_member_left", MetadataChangeGroup.CONSUMER_GROUP_MEMBERS),
    CONSUMER_GROUP_PARTITION_HANDED_OVER(
            42, "consumer_group_partition_handed_over", MetadataChangeGroup.CONSUMER_GROUP_MEMBERS),
    USER_CREATED(50, "user_created", MetadataChangeGroup.USERS),
    USER_UPDATED(51, "user_updated", MetadataChangeGroup.USERS),
    USER_DELETED(52, "user_deleted", MetadataChangeGroup.USERS),
    USER_PASSWORD_CHANGED(53, "user_password_changed", MetadataChangeGroup.USERS),
    USER_PERMISSIONS_UPDATED(54, "user_permissions_updated", MetadataChangeGroup.USERS),
    PERSONAL_ACCESS_TOKEN_CREATED(60, "personal_access_token_created", MetadataChangeGroup.PERSONAL_ACCESS_TOKENS),
    PERSONAL_ACCESS_TOKEN_DELETED(61, "personal_access_token_deleted", MetadataChangeGroup.PERSONAL_ACCESS_TOKENS),
    SESSION_REGISTERED(70, "session_registered", MetadataChangeGroup.SESSIONS),
    SESSION_CLOSED(71, "session_closed", MetadataChangeGroup.SESSIONS);

    private final int code;
    private final String value;
    private final MetadataChangeGroup group;

    MetadataChangeKind(int code, String value, MetadataChangeGroup group) {
        this.code = code;
        this.value = value;
        this.group = group;
    }

    /** The kind with wire code {@code code}, or empty for a kind this build does not know. */
    public static Optional<MetadataChangeKind> fromCode(int code) {
        for (MetadataChangeKind kind : values()) {
            if (kind.code == code) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }

    /** The kind named {@code value}, or empty for a kind this build does not know. */
    public static Optional<MetadataChangeKind> fromValue(String value) {
        for (MetadataChangeKind kind : values()) {
            if (kind.value.equals(value)) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }

    public int asCode() {
        return code;
    }

    public String value() {
        return value;
    }

    /** The group every record of this kind belongs to. */
    public MetadataChangeGroup group() {
        return group;
    }
}
