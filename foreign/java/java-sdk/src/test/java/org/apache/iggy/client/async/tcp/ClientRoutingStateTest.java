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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClientRoutingStateTest {

    private final ClientRoutingState state = new ClientRoutingState();

    @Nested
    class Keys {

        @Test
        void shouldDistinguishNumericAndNamedIdentifiersWithTheSameText() {
            assertThat(ClientRoutingState.topicKey(StreamId.of(1L), TopicId.of(2L)))
                    .isNotEqualTo(ClientRoutingState.topicKey(StreamId.of("1"), TopicId.of("2")));
        }

        @Test
        void shouldDistinguishIdentifiersContainingTheFormerDelimiter() {
            assertThat(ClientRoutingState.topicKey(StreamId.of("orders|created"), TopicId.of("events")))
                    .isNotEqualTo(ClientRoutingState.topicKey(StreamId.of("orders"), TopicId.of("created|events")));
        }

        @Test
        void shouldDistinguishConsumerIdentifiersWithTheSameText() {
            assertThat(ClientRoutingState.groupKey(StreamId.of(1L), TopicId.of(2L), ConsumerId.of(3L)))
                    .isNotEqualTo(ClientRoutingState.groupKey(StreamId.of(1L), TopicId.of(2L), ConsumerId.of("3")));
        }
    }

    @Nested
    class BalancedCursor {

        @Test
        void shouldRoundRobinAcrossPartitions() {
            // pinned to the Rust SDK: 3 partitions give 0, 1, 2, 0
            var topic = topic("s", "t");

            assertThat(state.nextBalancedPartition(topic, 3)).isEqualTo(0);
            assertThat(state.nextBalancedPartition(topic, 3)).isEqualTo(1);
            assertThat(state.nextBalancedPartition(topic, 3)).isEqualTo(2);
            assertThat(state.nextBalancedPartition(topic, 3)).isEqualTo(0);
        }

        @Test
        void shouldKeepIndependentCursorsPerTopic() {
            var first = topic("s", "a");
            var second = topic("s", "b");

            assertThat(state.nextBalancedPartition(first, 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition(second, 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition(first, 2)).isEqualTo(1);
        }

        @Test
        void shouldKeepIndependentCursorsForFormerlyCollidingTopics() {
            var numeric = ClientRoutingState.topicKey(StreamId.of(1L), TopicId.of(2L));
            var named = ClientRoutingState.topicKey(StreamId.of("1"), TopicId.of("2"));

            assertThat(state.nextBalancedPartition(numeric, 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition(named, 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition(numeric, 2)).isEqualTo(1);
        }

        @Test
        void shouldFallBackToZeroWithoutPartitions() {
            assertThat(state.nextBalancedPartition(topic("s", "t"), 0)).isEqualTo(0);
        }
    }

    @Nested
    class GroupAssignments {

        @Test
        void shouldRoundRobinAcrossAssignedPartitions() {
            // pinned to the Rust SDK: assignment [0, 1, 2] gives 0, 1, 2, 0
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L, 1L, 2L), 0);

            assertThat(state.nextGroupPartition(group)).hasValue(0);
            assertThat(state.nextGroupPartition(group)).hasValue(1);
            assertThat(state.nextGroupPartition(group)).hasValue(2);
            assertThat(state.nextGroupPartition(group)).hasValue(0);
        }

        @Test
        void shouldReturnEmptyWithoutAssignment() {
            assertThat(state.nextGroupPartition(group("s", "t", "g"))).isEmpty();
        }

        @Test
        void shouldReturnEmptyForMemberOwningNoPartitions() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(), 0);

            assertThat(state.nextGroupPartition(group)).isEmpty();
        }

        @Test
        void shouldResetCursorWhenGenerationAdvances() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition(group);
            state.nextGroupPartition(group);

            state.setAssignment(group, 2, List.of(0L, 1L, 2L), 0);

            assertThat(state.nextGroupPartition(group)).hasValue(0);
        }

        @Test
        void shouldKeepCursorWhenGenerationIsUnchanged() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition(group);

            state.setAssignment(group, 1, List.of(0L, 1L, 2L), 100);

            assertThat(state.nextGroupPartition(group)).hasValue(1);
        }

        @Test
        void shouldWrapCursorPositionWhenAssignmentShrinks() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition(group);
            state.nextGroupPartition(group);

            state.setAssignment(group, 1, List.of(5L), 0);

            assertThat(state.nextGroupPartition(group)).hasValue(5);
        }

        @Test
        void shouldInvalidateSingleAssignment() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L), 0);
            state.invalidateAssignment(group);

            assertThat(state.assignment(group)).isEmpty();
        }

        @Test
        void shouldClearAllAssignments() {
            var first = group("s", "t", "g1");
            var second = group("s", "t", "g2");
            state.setAssignment(first, 1, List.of(0L), 0);
            state.setAssignment(second, 1, List.of(1L), 0);

            state.clearAssignments();

            assertThat(state.assignment(first)).isEmpty();
            assertThat(state.assignment(second)).isEmpty();
        }

        @Test
        void shouldKeepAssignmentsIndependentForFormerlyCollidingGroups() {
            var numeric = ClientRoutingState.groupKey(StreamId.of(1L), TopicId.of(2L), ConsumerId.of(3L));
            var named = ClientRoutingState.groupKey(StreamId.of("1"), TopicId.of("2"), ConsumerId.of("3"));
            state.setAssignment(numeric, 1, List.of(1L), 0);
            state.setAssignment(named, 1, List.of(2L), 0);

            assertThat(state.nextGroupPartition(numeric)).hasValue(1);
            assertThat(state.nextGroupPartition(named)).hasValue(2);
        }

        @Test
        void shouldExposeSyncTimestampForStalenessChecks() {
            var group = group("s", "t", "g");
            state.setAssignment(group, 1, List.of(0L), 42L);

            assertThat(state.assignment(group)).hasValueSatisfying(assignment -> assertThat(assignment.syncedAtNanos())
                    .isEqualTo(42L));
        }
    }

    @Nested
    class PartitionCounts {

        @Test
        void shouldCachePartitionCountWithFetchTimestamp() {
            var topic = topic("s", "t");
            assertThat(state.partitionCount(topic)).isEmpty();

            state.setPartitionCount(topic, 4L, 42L);

            assertThat(state.partitionCount(topic)).hasValueSatisfying(cached -> {
                assertThat(cached.count()).isEqualTo(4L);
                assertThat(cached.fetchedAtNanos()).isEqualTo(42L);
            });
        }

        @Test
        void shouldInvalidatePartitionCount() {
            var topic = topic("s", "t");
            state.setPartitionCount(topic, 4L, 0L);

            state.invalidatePartitionCount(topic);

            assertThat(state.partitionCount(topic)).isEmpty();
        }

        @Test
        void shouldKeepCountsIndependentForFormerlyCollidingTopics() {
            var numeric = ClientRoutingState.topicKey(StreamId.of(1L), TopicId.of(2L));
            var named = ClientRoutingState.topicKey(StreamId.of("1"), TopicId.of("2"));
            state.setPartitionCount(numeric, 4L, 0L);
            state.setPartitionCount(named, 8L, 0L);

            assertThat(state.partitionCount(numeric))
                    .hasValueSatisfying(cached -> assertThat(cached.count()).isEqualTo(4L));
            assertThat(state.partitionCount(named))
                    .hasValueSatisfying(cached -> assertThat(cached.count()).isEqualTo(8L));
        }
    }

    private static ClientRoutingState.TopicKey topic(String stream, String topic) {
        return ClientRoutingState.topicKey(StreamId.of(stream), TopicId.of(topic));
    }

    private static ClientRoutingState.GroupKey group(String stream, String topic, String consumer) {
        return ClientRoutingState.groupKey(StreamId.of(stream), TopicId.of(topic), ConsumerId.of(consumer));
    }
}
