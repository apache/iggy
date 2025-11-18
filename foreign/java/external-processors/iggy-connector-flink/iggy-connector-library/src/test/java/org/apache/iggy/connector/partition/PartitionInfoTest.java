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

package org.apache.iggy.connector.partition;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionInfoTest {

    @Test
    void shouldCreatePartitionInfoWithValidParameters() {
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 0, 100, 500);

        assertThat(partitionInfo.getStreamId()).isEqualTo("stream1");
        assertThat(partitionInfo.getTopicId()).isEqualTo("topic1");
        assertThat(partitionInfo.getPartitionId()).isZero();
        assertThat(partitionInfo.getCurrentOffset()).isEqualTo(100);
        assertThat(partitionInfo.getEndOffset()).isEqualTo(500);
        assertThat(partitionInfo.getAvailableMessages()).isEqualTo(400);
    }

    @Test
    void shouldCalculateAvailableMessagesCorrectly() {
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 0, 100, 500);
        assertThat(partitionInfo.getAvailableMessages()).isEqualTo(400);

        PartitionInfo emptyPartition = PartitionInfo.of("stream1", "topic1", 0, 500, 500);
        assertThat(emptyPartition.getAvailableMessages()).isZero();

        PartitionInfo newPartition = PartitionInfo.of("stream1", "topic1", 0, 0, 1000);
        assertThat(newPartition.getAvailableMessages()).isEqualTo(1000);
    }

    @Test
    void shouldReturnZeroWhenCurrentOffsetExceedsEndOffset() {
        // This can happen in edge cases during offset management
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 0, 600, 500);
        assertThat(partitionInfo.getAvailableMessages()).isZero();
    }

    @Test
    void shouldThrowExceptionWhenStreamIdIsNull() {
        assertThatThrownBy(() -> PartitionInfo.of(null, "topic1", 0, 100, 500))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("streamId must not be null");
    }

    @Test
    void shouldThrowExceptionWhenTopicIdIsNull() {
        assertThatThrownBy(() -> PartitionInfo.of("stream1", null, 0, 100, 500))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topicId must not be null");
    }

    @Test
    void shouldThrowExceptionWhenPartitionIdIsNegative() {
        assertThatThrownBy(() -> PartitionInfo.of("stream1", "topic1", -1, 100, 500))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionId must be non-negative");
    }

    @Test
    void shouldThrowExceptionWhenCurrentOffsetIsNegative() {
        assertThatThrownBy(() -> PartitionInfo.of("stream1", "topic1", 0, -1, 500))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("currentOffset must be non-negative");
    }

    @Test
    void shouldThrowExceptionWhenEndOffsetIsNegative() {
        assertThatThrownBy(() -> PartitionInfo.of("stream1", "topic1", 0, 100, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endOffset must be non-negative");
    }

    @Test
    void shouldAcceptZeroValues() {
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 0, 0, 0);

        assertThat(partitionInfo.getPartitionId()).isZero();
        assertThat(partitionInfo.getCurrentOffset()).isZero();
        assertThat(partitionInfo.getEndOffset()).isZero();
        assertThat(partitionInfo.getAvailableMessages()).isZero();
    }

    @Test
    void shouldSupportMultiplePartitions() {
        PartitionInfo partition0 = PartitionInfo.of("stream1", "topic1", 0, 100, 500);
        PartitionInfo partition1 = PartitionInfo.of("stream1", "topic1", 1, 200, 600);
        PartitionInfo partition99 = PartitionInfo.of("stream1", "topic1", 99, 0, 1000);

        assertThat(partition0.getPartitionId()).isZero();
        assertThat(partition1.getPartitionId()).isEqualTo(1);
        assertThat(partition99.getPartitionId()).isEqualTo(99);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        PartitionInfo partition1 = PartitionInfo.of("stream1", "topic1", 0, 100, 500);
        PartitionInfo partition2 = PartitionInfo.of("stream1", "topic1", 0, 100, 500);
        PartitionInfo partition3 = PartitionInfo.of("stream2", "topic1", 0, 100, 500);
        PartitionInfo partition4 = PartitionInfo.of("stream1", "topic2", 0, 100, 500);
        PartitionInfo partition5 = PartitionInfo.of("stream1", "topic1", 1, 100, 500);
        PartitionInfo partition6 = PartitionInfo.of("stream1", "topic1", 0, 200, 500);
        PartitionInfo partition7 = PartitionInfo.of("stream1", "topic1", 0, 100, 600);

        assertThat(partition1).isEqualTo(partition2);
        assertThat(partition1).hasSameHashCodeAs(partition2);
        assertThat(partition1).isNotEqualTo(partition3);
        assertThat(partition1).isNotEqualTo(partition4);
        assertThat(partition1).isNotEqualTo(partition5);
        assertThat(partition1).isNotEqualTo(partition6);
        assertThat(partition1).isNotEqualTo(partition7);
        assertThat(partition1).isEqualTo(partition1);
        assertThat(partition1).isNotEqualTo(null);
        assertThat(partition1).isNotEqualTo(new Object());
    }

    @Test
    void shouldImplementToStringCorrectly() {
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 5, 100, 500);

        String toString = partitionInfo.toString();
        assertThat(toString).contains("PartitionInfo");
        assertThat(toString).contains("stream1");
        assertThat(toString).contains("topic1");
        assertThat(toString).contains("partitionId=5");
        assertThat(toString).contains("currentOffset=100");
        assertThat(toString).contains("endOffset=500");
        assertThat(toString).contains("available=400");
    }

    @Test
    void shouldBeSerializable() {
        PartitionInfo partitionInfo = PartitionInfo.of("stream1", "topic1", 0, 100, 500);
        assertThat(partitionInfo).isInstanceOf(java.io.Serializable.class);
    }
}
