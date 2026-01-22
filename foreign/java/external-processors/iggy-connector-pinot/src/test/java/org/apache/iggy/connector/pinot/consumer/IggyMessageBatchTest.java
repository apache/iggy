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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IggyMessageBatchTest {

    @Test
    void testEmptyBatch() {
        IggyMessageBatch batch = new IggyMessageBatch(new ArrayList<>());
        assertEquals(0, batch.getMessageCount());
    }

    @Test
    void testSingleMessage() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        byte[] payload = "test message".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertEquals(1, batch.getMessageCount());
        assertArrayEquals(payload, batch.getMessageAtIndex(0));
        assertEquals(payload.length, batch.getMessageLengthAtIndex(0));
        assertEquals(100L, batch.getNextStreamMessageOffsetAtIndex(0));
        assertEquals(offset, batch.getNextStreamPartitionMsgOffsetAtIndex(0));
    }

    @Test
    void testMultipleMessages() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(i * 100L);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));
        }

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertEquals(10, batch.getMessageCount());

        for (int i = 0; i < 10; i++) {
            byte[] expectedPayload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            assertArrayEquals(expectedPayload, batch.getMessageAtIndex(i));
            assertEquals(expectedPayload.length, batch.getMessageLengthAtIndex(i));
            assertEquals(i * 100L, batch.getNextStreamMessageOffsetAtIndex(i));
            assertEquals(i, batch.getMessageOffsetAtIndex(i));
        }
    }

    @Test
    void testMessageAndOffsetWrapper() {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(123L);

        IggyMessageBatch.IggyMessageAndOffset wrapper = new IggyMessageBatch.IggyMessageAndOffset(payload, offset);

        assertArrayEquals(payload, wrapper.getMessage());
        assertEquals(offset, wrapper.getOffset());
        assertEquals(123L, wrapper.getOffset().getOffset());
    }

    @Test
    void testNullOffsetAtInvalidIndex() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertNull(batch.getNextStreamPartitionMsgOffsetAtIndex(-1));
        assertNull(batch.getNextStreamPartitionMsgOffsetAtIndex(10));
        assertEquals(0, batch.getNextStreamMessageOffsetAtIndex(-1));
        assertEquals(0, batch.getNextStreamMessageOffsetAtIndex(10));
    }

    @Test
    void testLargeMessageBatch() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();

        // Create 1000 messages
        for (int i = 0; i < 1000; i++) {
            byte[] payload = new byte[1024]; // 1KB per message
            IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(i);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));
        }

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertEquals(1000, batch.getMessageCount());
        assertEquals(1024, batch.getMessageLengthAtIndex(0));
        assertEquals(1024, batch.getMessageLengthAtIndex(999));
    }
}
