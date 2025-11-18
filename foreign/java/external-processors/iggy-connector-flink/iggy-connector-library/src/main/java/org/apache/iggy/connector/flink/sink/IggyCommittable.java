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

package org.apache.iggy.connector.flink.sink;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents committable information for exactly-once delivery guarantees.
 * Used in two-phase commit protocol with Flink checkpointing.
 */
public class IggyCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamId;
    private final String topicId;
    private final int partitionId;
    private final long lastWrittenOffset;
    private final int messageCount;

    /**
     * Creates a new committable instance.
     *
     * @param streamId the stream identifier
     * @param topicId the topic identifier
     * @param partitionId the partition ID
     * @param lastWrittenOffset the last offset written to this partition
     * @param messageCount the number of messages in this commit
     */
    public IggyCommittable(String streamId, String topicId, int partitionId, long lastWrittenOffset, int messageCount) {

        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalArgumentException("streamId cannot be null or empty");
        }
        if (topicId == null || topicId.isEmpty()) {
            throw new IllegalArgumentException("topicId cannot be null or empty");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be >= 0");
        }
        if (lastWrittenOffset < 0) {
            throw new IllegalArgumentException("lastWrittenOffset must be >= 0");
        }
        if (messageCount < 0) {
            throw new IllegalArgumentException("messageCount must be >= 0");
        }

        this.streamId = streamId;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.lastWrittenOffset = lastWrittenOffset;
        this.messageCount = messageCount;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getTopicId() {
        return topicId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getLastWrittenOffset() {
        return lastWrittenOffset;
    }

    public int getMessageCount() {
        return messageCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IggyCommittable that = (IggyCommittable) o;
        return partitionId == that.partitionId
                && lastWrittenOffset == that.lastWrittenOffset
                && messageCount == that.messageCount
                && Objects.equals(streamId, that.streamId)
                && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, topicId, partitionId, lastWrittenOffset, messageCount);
    }

    @Override
    public String toString() {
        return "IggyCommittable{"
                + "streamId='" + streamId + '\''
                + ", topicId='" + topicId + '\''
                + ", partitionId=" + partitionId
                + ", lastWrittenOffset=" + lastWrittenOffset
                + ", messageCount=" + messageCount
                + '}';
    }
}
