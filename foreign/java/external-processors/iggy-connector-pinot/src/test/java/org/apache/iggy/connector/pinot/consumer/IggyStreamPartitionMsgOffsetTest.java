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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IggyStreamPartitionMsgOffsetTest {

    @Test
    void testOffsetCreation() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        assertEquals(100L, offset.getOffset());
    }

    @Test
    void testCompareTo() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(200L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(100L);

        assertTrue(offset1.compareTo(offset2) < 0);
        assertTrue(offset2.compareTo(offset1) > 0);
        assertEquals(0, offset1.compareTo(offset3));
    }

    @Test
    void testEquals() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(200L);

        assertEquals(offset1, offset2);
        assertNotEquals(offset1, offset3);
        assertEquals(offset1, offset1);
        assertNotEquals(offset1, null);
        assertNotEquals(offset1, "string");
    }

    @Test
    void testHashCode() {
        IggyStreamPartitionMsgOffset offset1 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset2 = new IggyStreamPartitionMsgOffset(100L);
        IggyStreamPartitionMsgOffset offset3 = new IggyStreamPartitionMsgOffset(200L);

        assertEquals(offset1.hashCode(), offset2.hashCode());
        assertNotEquals(offset1.hashCode(), offset3.hashCode());
    }

    @Test
    void testToString() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(12345L);
        assertEquals("12345", offset.toString());
    }

    @Test
    void testZeroOffset() {
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(0L);
        assertEquals(0L, offset.getOffset());
        assertEquals("0", offset.toString());
    }

    @Test
    void testLargeOffset() {
        long largeOffset = Long.MAX_VALUE - 1;
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(largeOffset);
        assertEquals(largeOffset, offset.getOffset());
        assertEquals(String.valueOf(largeOffset), offset.toString());
    }
}
