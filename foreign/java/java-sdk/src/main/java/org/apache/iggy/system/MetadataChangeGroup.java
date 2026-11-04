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

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Family of metadata change records and the unit of filtering. The codes are the wire codes. */
public enum MetadataChangeGroup {
    /** Stream created, updated and deleted. */
    STREAMS(1, "streams"),
    /** Topic created, updated and deleted. */
    TOPICS(2, "topics"),
    /** Partition count changes. */
    PARTITIONS(3, "partitions"),
    /** Consumer group created and deleted. */
    CONSUMER_GROUPS(4, "consumer_groups"),
    /** Consumer group members joining, leaving and receiving partitions. */
    CONSUMER_GROUP_MEMBERS(5, "consumer_group_members"),
    /** User created, updated, deleted, password and permission changes. */
    USERS(6, "users"),
    /** Personal access token created and deleted. */
    PERSONAL_ACCESS_TOKENS(7, "personal_access_tokens"),
    /** Session registered and closed. */
    SESSIONS(8, "sessions");

    private final int code;
    private final String value;

    MetadataChangeGroup(int code, String value) {
        this.code = code;
        this.value = value;
    }

    /** The group with wire code {@code code}, or empty for a group this build does not know. */
    public static Optional<MetadataChangeGroup> fromCode(int code) {
        for (MetadataChangeGroup group : values()) {
            if (group.code == code) {
                return Optional.of(group);
            }
        }
        return Optional.empty();
    }

    /** The group named {@code value}, or empty for a group this build does not know. */
    public static Optional<MetadataChangeGroup> fromValue(String value) {
        for (MetadataChangeGroup group : values()) {
            if (group.value.equals(value)) {
                return Optional.of(group);
            }
        }
        return Optional.empty();
    }

    /** The groups a page token's mask selects, in code order. */
    public static Set<MetadataChangeGroup> fromMask(int mask) {
        Set<MetadataChangeGroup> groups = EnumSet.noneOf(MetadataChangeGroup.class);
        for (MetadataChangeGroup group : values()) {
            if ((mask & group.bit()) != 0) {
                groups.add(group);
            }
        }
        return groups;
    }

    /** The mask of {@code groups}, every group when none is requested. */
    public static int mask(List<MetadataChangeGroup> groups) {
        if (groups.isEmpty()) {
            return 0xFF;
        }
        int mask = 0;
        for (MetadataChangeGroup group : groups) {
            mask |= group.bit();
        }
        return mask;
    }

    public int asCode() {
        return code;
    }

    public String value() {
        return value;
    }

    /** Bit {@code code - 1} of a group mask. */
    public int bit() {
        return 1 << (code - 1);
    }

    /**
     * Groups a current state scan of this group must include, so a mirror can apply the cascades of their
     * deletes.
     */
    public List<MetadataChangeGroup> parents() {
        return switch (this) {
            case TOPICS -> List.of(STREAMS);
            case PARTITIONS, CONSUMER_GROUPS -> List.of(TOPICS, STREAMS);
            case CONSUMER_GROUP_MEMBERS -> List.of(CONSUMER_GROUPS, PARTITIONS, TOPICS, STREAMS);
            case PERSONAL_ACCESS_TOKENS, SESSIONS -> List.of(USERS);
            default -> List.of();
        };
    }
}
