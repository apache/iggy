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
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-client cache of the routing facts needed to resolve partitioning and
 * consumer-group polling client-side: topic partition counts, balanced
 * round-robin cursors, and consumer-group assignments. Keys are
 * {@code stream|topic[|group]} strings built from the request identifiers,
 * matching the Rust SDK's {@code ConsumerGroupClientState}.
 *
 * <p>Partition counts and balanced cursors survive reconnects; group
 * assignments are bound to the server-side VSR session (the member is keyed
 * by the connection's client id) and must be cleared whenever that session is
 * reset or the client moves to another node. Partition counts carry their
 * fetch timestamp so callers can refresh them past a staleness budget — a
 * count cached forever would keep hashing keys with the wrong modulus after
 * a partition-count change (the Rust SDK still has that gap).
 */
final class ClientRoutingState {

    private final Map<String, CachedPartitionCount> partitionCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> balancedCursors = new ConcurrentHashMap<>();
    private final Map<String, GroupAssignment> assignments = new ConcurrentHashMap<>();

    static String topicKey(StreamId streamId, TopicId topicId) {
        return streamId + "|" + topicId;
    }

    static String groupKey(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        return streamId + "|" + topicId + "|" + groupId;
    }

    Optional<CachedPartitionCount> partitionCount(String topicKey) {
        return Optional.ofNullable(partitionCounts.get(topicKey));
    }

    void setPartitionCount(String topicKey, long count, long fetchedAtNanos) {
        partitionCounts.put(topicKey, new CachedPartitionCount(count, fetchedAtNanos));
    }

    /**
     * Drops the cached partition count for a topic. Called when a send that
     * was routed with the cached count is refused with a not-found error,
     * meaning the topic shrank or was recreated and the count is stale.
     */
    void invalidatePartitionCount(String topicKey) {
        partitionCounts.remove(topicKey);
    }

    /**
     * The next balanced produce partition for a topic, advancing the cursor.
     * A per-topic monotonic counter modulo the partition count, so three
     * partitions yield 0, 1, 2, 0, ...
     */
    long nextBalancedPartition(String topicKey, long partitionCount) {
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
    void setAssignment(String groupKey, long generation, List<Long> partitions, long syncedAtNanos) {
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
    OptionalLong nextGroupPartition(String groupKey) {
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

    Optional<GroupAssignment> assignment(String groupKey) {
        return Optional.ofNullable(assignments.get(groupKey));
    }

    void invalidateAssignment(String groupKey) {
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

    record GroupAssignment(long generation, List<Long> partitions, int cursor, long syncedAtNanos) {}

    record CachedPartitionCount(long count, long fetchedAtNanos) {}
}
