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

package org.apache.iggy.connector.pinot.metadata;

import org.apache.iggy.partition.Partition;
import org.apache.iggy.partition.PartitionContext;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IggyStreamMetadataProviderTest {

    @Test
    void nextOffsetOfEmptyPartitionIsZero() {
        assertThat(IggyStreamMetadataProvider.nextOffset(partition(0, 0))).isZero();
    }

    @Test
    void nextOffsetFollowsSingleMessage() {
        assertThat(IggyStreamMetadataProvider.nextOffset(partition(0, 1))).isEqualTo(1);
    }

    @Test
    void nextOffsetIgnoresMessagesRemovedByRetention() {
        assertThat(IggyStreamMetadataProvider.nextOffset(partition(99, 10))).isEqualTo(100);
    }

    @Test
    void nextOffsetFollowsLastOffsetWhenRetentionRemovedAllMessages() {
        assertThat(IggyStreamMetadataProvider.nextOffset(partition(99, 0))).isEqualTo(100);
    }

    private static Partition partition(long currentOffset, long messagesCount) {
        return new Partition(
                0L,
                BigInteger.ZERO,
                1L,
                BigInteger.valueOf(currentOffset),
                "0",
                BigInteger.valueOf(messagesCount),
                PartitionContext.EMPTY,
                List.of(),
                List.of());
    }
}
