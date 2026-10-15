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

package org.apache.iggy.client.async.tcp;

import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-client balanced cursors and consumer-group assignments. Assignment state
 * belongs to the current session and is cleared when that session changes.
 * Typed keys distinguish numeric identifiers from same-text names.
 */
final class ClientRoutingState {

    private final Map<TopicKey, AtomicInteger> balancedCursors = new ConcurrentHashMap<>();
    private final Map<GroupKey, GroupAssignment> assignments = new ConcurrentHashMap<>();

    static TopicKey topicKey(StreamId streamId, TopicId topicId) {
        return new TopicKey(IdentifierKey.from(streamId), IdentifierKey.from(topicId));
    }

    static GroupKey groupKey(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        return new GroupKey(topicKey(streamId, topicId), IdentifierKey.from(groupId));
    }

    /**
     * The next balanced produce partition for a topic, advancing the cursor.
     * A per-topic monotonic counter modulo the partition count, so three
     * partitions yield 0, 1, 2, 0, ...
     */
    long nextBalancedPartition(TopicKey topicKey, long partitionCount) {
        if (partitionCount <= 0) {
            return 0;
        }
        var cursor = balancedCursors.computeIfAbsent(topicKey, ignored -> new AtomicInteger());
        return Integer.toUnsignedLong(cursor.getAndIncrement()) % partitionCount;
    }

    /**
     * Replaces the cached assignment for a group. A generation change (a
     * rebalance) resets the round-robin cursor so selection restarts cleanly;
     * an unchanged generation keeps the cursor so an assignment refresh does
     * not disturb the polling rotation.
     */
    void setAssignment(GroupKey groupKey, long generation, List<Long> partitions, long syncedAtNanos) {
        assignments.compute(groupKey, (ignored, existing) -> {
            var cursor = existing != null && existing.generation() == generation ? existing.cursor() : 0;
            return new GroupAssignment(generation, List.copyOf(partitions), cursor, syncedAtNanos);
        });
    }

    /**
     * The next assigned partition to poll for a group, advancing the
     * round-robin cursor. Empty when the group has no cached assignment or
     * the member currently owns no partitions.
     */
    OptionalLong nextGroupPartition(GroupKey groupKey) {
        var next = new long[] {-1};
        assignments.computeIfPresent(groupKey, (ignored, assignment) -> {
            if (assignment.partitions().isEmpty()) {
                return assignment;
            }
            var index =
                    Math.floorMod(assignment.cursor(), assignment.partitions().size());
            next[0] = assignment.partitions().get(index);
            return new GroupAssignment(
                    assignment.generation(),
                    assignment.partitions(),
                    assignment.cursor() + 1,
                    assignment.syncedAtNanos());
        });
        return next[0] < 0 ? OptionalLong.empty() : OptionalLong.of(next[0]);
    }

    Optional<GroupAssignment> assignment(GroupKey groupKey) {
        return Optional.ofNullable(assignments.get(groupKey));
    }

    void invalidateAssignment(GroupKey groupKey) {
        assignments.remove(groupKey);
    }

    /**
     * Drops every cached group assignment. Called when the VSR session is
     * reset or the client retargets to another node, since the server keys
     * group membership by the session's client id.
     */
    void clearAssignments() {
        assignments.clear();
    }

    record IdentifierKey(int kind, Long id, String name) {
        static IdentifierKey from(Identifier identifier) {
            return new IdentifierKey(identifier.getKind(), identifier.getId(), identifier.getName());
        }
    }

    record TopicKey(IdentifierKey stream, IdentifierKey topic) {}

    record GroupKey(TopicKey topic, IdentifierKey consumer) {}

    record GroupAssignment(long generation, List<Long> partitions, int cursor, long syncedAtNanos) {}
}
