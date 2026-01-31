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

import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.util.List;

/**
 * Implementation of Pinot's MessageBatch for Iggy messages.
 * Wraps a list of messages with their offsets for consumption by Pinot.
 */
public class IggyMessageBatch implements MessageBatch<byte[]> {

    private final List<IggyMessageAndOffset> messages;

    /**
     * Creates a new message batch.
     *
     * @param messages list of messages with offsets
     */
    public IggyMessageBatch(List<IggyMessageAndOffset> messages) {
        this.messages = messages;
    }

    @Override
    public int getMessageCount() {
        return messages.size();
    }

    @Override
    public byte[] getMessageAtIndex(int index) {
        return messages.get(index).message;
    }

    @Override
    public int getMessageOffsetAtIndex(int index) {
        return index;
    }

    @Override
    public int getMessageLengthAtIndex(int index) {
        return messages.get(index).message.length;
    }

    @Override
    public long getNextStreamMessageOffsetAtIndex(int index) {
        if (index >= 0 && index < messages.size()) {
            return messages.get(index).offset.getOffset();
        }
        return 0;
    }

    @Override
    public StreamPartitionMsgOffset getNextStreamPartitionMsgOffsetAtIndex(int index) {
        if (index >= 0 && index < messages.size()) {
            return messages.get(index).offset;
        }
        return null;
    }

    @Override
    public StreamMessage<byte[]> getStreamMessage(int index) {
        IggyMessageAndOffset messageAndOffset = messages.get(index);

        // Calculate next offset (current + 1)
        long currentOffset = messageAndOffset.offset.getOffset();
        IggyStreamPartitionMsgOffset nextOffset = new IggyStreamPartitionMsgOffset(currentOffset + 1);

        // Create metadata with offset information
        StreamMessageMetadata metadata = new StreamMessageMetadata.Builder()
                .setRecordIngestionTimeMs(System.currentTimeMillis())
                .setOffset(messageAndOffset.offset, nextOffset)
                .build();

        // Create and return StreamMessage
        return new StreamMessage<>(null, messageAndOffset.message, messageAndOffset.message.length, metadata);
    }

    @Override
    public StreamPartitionMsgOffset getOffsetOfNextBatch() {
        if (messages.isEmpty()) {
            return new IggyStreamPartitionMsgOffset(0);
        }
        // Return the offset after the last message
        long lastOffset = messages.get(messages.size() - 1).offset.getOffset();
        return new IggyStreamPartitionMsgOffset(lastOffset + 1);
    }

    /**
     * Container for an Iggy message and its offset.
     */
    public static class IggyMessageAndOffset {
        private final byte[] message;
        private final IggyStreamPartitionMsgOffset offset;

        public IggyMessageAndOffset(byte[] message, IggyStreamPartitionMsgOffset offset) {
            this.message = message;
            this.offset = offset;
        }

        public byte[] getMessage() {
            return message;
        }

        public IggyStreamPartitionMsgOffset getOffset() {
            return offset;
        }
    }
}
