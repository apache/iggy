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

import java.io.Serializable;
import java.util.Objects;

/**
 * Information about an Iggy partition.
 * This is a framework-agnostic representation that can be used
 * by different stream processing engines.
 */
public final class PartitionInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamId;
    private final String topicId;
    private final int partitionId;
    private final long currentOffset;
    private final long endOffset;

    private PartitionInfo(String streamId, String topicId, int partitionId, long currentOffset, long endOffset) {
        this.streamId = Objects.requireNonNull(streamId, "streamId must not be null");
        this.topicId = Objects.requireNonNull(topicId, "topicId must not be null");
        this.partitionId = partitionId;
        this.currentOffset = currentOffset;
        this.endOffset = endOffset;

        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be non-negative");
        }
        if (currentOffset < 0) {
            throw new IllegalArgumentException("currentOffset must be non-negative");
        }
        if (endOffset < 0) {
            throw new IllegalArgumentException("endOffset must be non-negative");
        }
    }

    public static PartitionInfo of(
            String streamId, String topicId, int partitionId, long currentOffset, long endOffset) {
        return new PartitionInfo(streamId, topicId, partitionId, currentOffset, endOffset);
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

    public long getCurrentOffset() {
        return currentOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    /**
     * Returns the number of messages available in this partition.
     */
    public long getAvailableMessages() {
        return Math.max(0, endOffset - currentOffset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionInfo that = (PartitionInfo) o;
        return partitionId == that.partitionId
                && currentOffset == that.currentOffset
                && endOffset == that.endOffset
                && Objects.equals(streamId, that.streamId)
                && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, topicId, partitionId, currentOffset, endOffset);
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
                + "streamId='" + streamId + '\''
                + ", topicId='" + topicId + '\''
                + ", partitionId=" + partitionId
                + ", currentOffset=" + currentOffset
                + ", endOffset=" + endOffset
                + ", available=" + getAvailableMessages()
                + '}';
    }
}
