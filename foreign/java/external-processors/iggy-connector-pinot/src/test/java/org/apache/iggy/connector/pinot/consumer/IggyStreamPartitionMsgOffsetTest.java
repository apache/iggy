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

package org.apache.iggy.connector.pinot.consumer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyStreamPartitionMsgOffsetTest {

    @Test
    void testOffsetCreation() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        assertThat(offset.getOffset()).isEqualTo(100L);
    }

    @Test
    void testCompareTo() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(200L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(100L);

        assertThat(offset1.compareTo(offset2)).isNegative();
        assertThat(offset2.compareTo(offset1)).isPositive();
        assertThat(offset1.compareTo(offset3)).isZero();
    }

    @Test
    void testEquals() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(200L);

        assertThat(offset1).isEqualTo(offset2);
        assertThat(offset1).isNotEqualTo(offset3);
        assertThat(offset1).isEqualTo(offset1);
        assertThat(offset1).isNotEqualTo(null);
        assertThat(offset1).isNotEqualTo("string");
    }

    @Test
    void testHashCode() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(200L);

        assertThat(offset1.hashCode()).isEqualTo(offset2.hashCode());
        assertThat(offset1.hashCode()).isNotEqualTo(offset3.hashCode());
    }

    @Test
    void testToString() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(12345L);
        assertThat(offset.toString()).isEqualTo("12345");
    }

    @Test
    void testZeroOffset() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(0L);
        assertThat(offset.getOffset()).isEqualTo(0L);
        assertThat(offset.toString()).isEqualTo("0");
    }

    @Test
    void testLargeOffset() {
        long largeOffset = Long.MAX_VALUE - 1;
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(largeOffset);
        assertThat(offset.getOffset()).isEqualTo(largeOffset);
        assertThat(offset.toString()).isEqualTo(String.valueOf(largeOffset));
    }
}
