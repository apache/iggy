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
        void shouldBuildTopicKeyFromIdentifiers() {
            assertThat(ClientRoutingState.topicKey(StreamId.of(1L), TopicId.of(2L)))
                    .isEqualTo("1|2");
            assertThat(ClientRoutingState.topicKey(StreamId.of("orders"), TopicId.of("created")))
                    .isEqualTo("orders|created");
        }

        @Test
        void shouldBuildGroupKeyFromIdentifiers() {
            assertThat(ClientRoutingState.groupKey(StreamId.of(1L), TopicId.of(2L), ConsumerId.of(3L)))
                    .isEqualTo("1|2|3");
        }
    }

    @Nested
    class BalancedCursor {

        @Test
        void shouldRoundRobinAcrossPartitions() {
            // pinned to the Rust SDK: 3 partitions give 0, 1, 2, 0
            assertThat(state.nextBalancedPartition("s|t", 3)).isEqualTo(0);
            assertThat(state.nextBalancedPartition("s|t", 3)).isEqualTo(1);
            assertThat(state.nextBalancedPartition("s|t", 3)).isEqualTo(2);
            assertThat(state.nextBalancedPartition("s|t", 3)).isEqualTo(0);
        }

        @Test
        void shouldKeepIndependentCursorsPerTopic() {
            assertThat(state.nextBalancedPartition("s|a", 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition("s|b", 2)).isEqualTo(0);
            assertThat(state.nextBalancedPartition("s|a", 2)).isEqualTo(1);
        }

        @Test
        void shouldFallBackToZeroWithoutPartitions() {
            assertThat(state.nextBalancedPartition("s|t", 0)).isEqualTo(0);
        }
    }

    @Nested
    class GroupAssignments {

        @Test
        void shouldRoundRobinAcrossAssignedPartitions() {
            // pinned to the Rust SDK: assignment [0, 1, 2] gives 0, 1, 2, 0
            state.setAssignment("s|t|g", 1, List.of(0L, 1L, 2L), 0);

            assertThat(state.nextGroupPartition("s|t|g")).hasValue(0);
            assertThat(state.nextGroupPartition("s|t|g")).hasValue(1);
            assertThat(state.nextGroupPartition("s|t|g")).hasValue(2);
            assertThat(state.nextGroupPartition("s|t|g")).hasValue(0);
        }

        @Test
        void shouldReturnEmptyWithoutAssignment() {
            assertThat(state.nextGroupPartition("s|t|g")).isEmpty();
        }

        @Test
        void shouldReturnEmptyForMemberOwningNoPartitions() {
            state.setAssignment("s|t|g", 1, List.of(), 0);

            assertThat(state.nextGroupPartition("s|t|g")).isEmpty();
        }

        @Test
        void shouldResetCursorWhenGenerationAdvances() {
            state.setAssignment("s|t|g", 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition("s|t|g");
            state.nextGroupPartition("s|t|g");

            state.setAssignment("s|t|g", 2, List.of(0L, 1L, 2L), 0);

            assertThat(state.nextGroupPartition("s|t|g")).hasValue(0);
        }

        @Test
        void shouldKeepCursorWhenGenerationIsUnchanged() {
            state.setAssignment("s|t|g", 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition("s|t|g");

            state.setAssignment("s|t|g", 1, List.of(0L, 1L, 2L), 100);

            assertThat(state.nextGroupPartition("s|t|g")).hasValue(1);
        }

        @Test
        void shouldWrapCursorPositionWhenAssignmentShrinks() {
            state.setAssignment("s|t|g", 1, List.of(0L, 1L, 2L), 0);
            state.nextGroupPartition("s|t|g");
            state.nextGroupPartition("s|t|g");

            state.setAssignment("s|t|g", 1, List.of(5L), 0);

            assertThat(state.nextGroupPartition("s|t|g")).hasValue(5);
        }

        @Test
        void shouldInvalidateSingleAssignment() {
            state.setAssignment("s|t|g", 1, List.of(0L), 0);
            state.invalidateAssignment("s|t|g");

            assertThat(state.assignment("s|t|g")).isEmpty();
        }

        @Test
        void shouldClearAllAssignments() {
            state.setAssignment("s|t|g1", 1, List.of(0L), 0);
            state.setAssignment("s|t|g2", 1, List.of(1L), 0);

            state.clearAssignments();

            assertThat(state.assignment("s|t|g1")).isEmpty();
            assertThat(state.assignment("s|t|g2")).isEmpty();
        }

        @Test
        void shouldExposeSyncTimestampForStalenessChecks() {
            state.setAssignment("s|t|g", 1, List.of(0L), 42L);

            assertThat(state.assignment("s|t|g"))
                    .hasValueSatisfying(
                            assignment -> assertThat(assignment.syncedAtNanos()).isEqualTo(42L));
        }
    }

    @Nested
    class PartitionCounts {

        @Test
        void shouldCachePartitionCountWithFetchTimestamp() {
            assertThat(state.partitionCount("s|t")).isEmpty();

            state.setPartitionCount("s|t", 4L, 42L);

            assertThat(state.partitionCount("s|t")).hasValueSatisfying(cached -> {
                assertThat(cached.count()).isEqualTo(4L);
                assertThat(cached.fetchedAtNanos()).isEqualTo(42L);
            });
        }

        @Test
        void shouldInvalidatePartitionCount() {
            state.setPartitionCount("s|t", 4L, 0L);

            state.invalidatePartitionCount("s|t");

            assertThat(state.partitionCount("s|t")).isEmpty();
        }
    }
}
