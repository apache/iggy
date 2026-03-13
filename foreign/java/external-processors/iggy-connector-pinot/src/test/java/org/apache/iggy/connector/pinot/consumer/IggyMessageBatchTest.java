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

import static org.assertj.core.api.Assertions.assertThat;

class IggyMessageBatchTest {

    @Test
    void testEmptyBatch() {
        IggyMessageBatch batch = new IggyMessageBatch(new ArrayList<>());
        assertThat(batch.getMessageCount()).isEqualTo(0);
    }

    @Test
    void testSingleMessage() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        byte[] payload = "test message".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertThat(batch.getMessageCount()).isEqualTo(1);
        assertThat(batch.getMessageAtIndex(0)).isEqualTo(payload);
        assertThat(batch.getMessageLengthAtIndex(0)).isEqualTo(payload.length);
        assertThat(batch.getNextStreamMessageOffsetAtIndex(0)).isEqualTo(100L);
        assertThat(batch.getNextStreamPartitionMsgOffsetAtIndex(0)).isEqualTo(offset);
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

        assertThat(batch.getMessageCount()).isEqualTo(10);

        for (int i = 0; i < 10; i++) {
            byte[] expectedPayload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            assertThat(batch.getMessageAtIndex(i)).isEqualTo(expectedPayload);
            assertThat(batch.getMessageLengthAtIndex(i)).isEqualTo(expectedPayload.length);
            assertThat(batch.getNextStreamMessageOffsetAtIndex(i)).isEqualTo(i * 100L);
            assertThat(batch.getMessageOffsetAtIndex(i)).isEqualTo(i);
        }
    }

    @Test
    void testMessageAndOffsetWrapper() {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(123L);

        IggyMessageBatch.IggyMessageAndOffset wrapper = new IggyMessageBatch.IggyMessageAndOffset(payload, offset);

        assertThat(wrapper.getMessage()).isEqualTo(payload);
        assertThat(wrapper.getOffset()).isEqualTo(offset);
        assertThat(wrapper.getOffset().getOffset()).isEqualTo(123L);
    }

    @Test
    void testNullOffsetAtInvalidIndex() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertThat(batch.getNextStreamPartitionMsgOffsetAtIndex(-1)).isNull();
        assertThat(batch.getNextStreamPartitionMsgOffsetAtIndex(10)).isNull();
        assertThat(batch.getNextStreamMessageOffsetAtIndex(-1)).isEqualTo(0);
        assertThat(batch.getNextStreamMessageOffsetAtIndex(10)).isEqualTo(0);
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

        assertThat(batch.getMessageCount()).isEqualTo(1000);
        assertThat(batch.getMessageLengthAtIndex(0)).isEqualTo(1024);
        assertThat(batch.getMessageLengthAtIndex(999)).isEqualTo(1024);
    }
}
