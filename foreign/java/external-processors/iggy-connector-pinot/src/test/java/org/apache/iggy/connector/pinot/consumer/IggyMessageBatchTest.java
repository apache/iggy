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

import org.apache.pinot.spi.stream.StreamMessage;
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
        assertThat(batch.getSizeInBytes()).isEqualTo(0);
    }

    @Test
    void testSingleMessage() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        byte[] payload = "test message".getBytes(StandardCharsets.UTF_8);
        IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(100L);
        messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertThat(batch.getMessageCount()).isEqualTo(1);
        assertThat(batch.getSizeInBytes()).isEqualTo(payload.length);

        StreamMessage<byte[]> message = batch.getStreamMessage(0);
        assertThat(message.getValue()).isEqualTo(payload);
        assertThat(message.getLength()).isEqualTo(payload.length);
        assertThat(message.getMetadata().getOffset()).isEqualTo(offset);
    }

    @Test
    void testMultipleMessages() {
        List<IggyMessageBatch.IggyMessageAndOffset> messages = new ArrayList<>();
        long totalBytes = 0;

        for (int i = 0; i < 10; i++) {
            byte[] payload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            totalBytes += payload.length;
            IggyStreamPartitionMsgOffset offset = new IggyStreamPartitionMsgOffset(i * 100L);
            messages.add(new IggyMessageBatch.IggyMessageAndOffset(payload, offset));
        }

        IggyMessageBatch batch = new IggyMessageBatch(messages);

        assertThat(batch.getMessageCount()).isEqualTo(10);
        assertThat(batch.getSizeInBytes()).isEqualTo(totalBytes);

        for (int i = 0; i < 10; i++) {
            byte[] expectedPayload = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            StreamMessage<byte[]> message = batch.getStreamMessage(i);
            assertThat(message.getValue()).isEqualTo(expectedPayload);
            assertThat(message.getLength()).isEqualTo(expectedPayload.length);
            assertThat(message.getMetadata().getOffset())
                    .isEqualTo(messages.get(i).getOffset());
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
        assertThat(batch.getSizeInBytes()).isEqualTo(1000L * 1024);
        assertThat(batch.getStreamMessage(0).getLength()).isEqualTo(1024);
        assertThat(batch.getStreamMessage(999).getLength()).isEqualTo(1024);
    }
}
