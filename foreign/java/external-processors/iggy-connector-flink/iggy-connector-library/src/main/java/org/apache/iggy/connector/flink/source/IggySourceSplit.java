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

package org.apache.iggy.connector.flink.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single Iggy partition as a Flink source split.
 * Each split corresponds to one partition in an Iggy topic.
 */
public class IggySourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    private final String streamId;
    private final String topicId;
    private final int partitionId;
    private final long startOffset;
    private final long currentOffset;

    /**
     * Creates a new Iggy source split.
     *
     * @param streamId the stream identifier
     * @param topicId the topic identifier
     * @param partitionId the partition ID
     * @param startOffset the initial offset to start reading from
     * @param currentOffset the current offset (for checkpointing)
     */
    public IggySourceSplit(String streamId, String topicId, int partitionId, long startOffset, long currentOffset) {

        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalArgumentException("streamId cannot be null or empty");
        }
        if (topicId == null || topicId.isEmpty()) {
            throw new IllegalArgumentException("topicId cannot be null or empty");
        }
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId must be >= 0");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be >= 0");
        }
        if (currentOffset < startOffset) {
            throw new IllegalArgumentException("currentOffset must be >= startOffset");
        }

        this.streamId = streamId;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.startOffset = startOffset;
        this.currentOffset = currentOffset;
    }

    /**
     * Creates a new split starting from the specified offset.
     *
     * @param streamId the stream identifier
     * @param topicId the topic identifier
     * @param partitionId the partition ID
     * @param startOffset the offset to start reading from
     * @return a new source split
     */
    public static IggySourceSplit create(String streamId, String topicId, int partitionId, long startOffset) {
        return new IggySourceSplit(streamId, topicId, partitionId, startOffset, startOffset);
    }

    @Override
    public String splitId() {
        return String.format("%s-%s-%d", streamId, topicId, partitionId);
    }

    /**
     * Creates a new split with updated current offset (for checkpointing).
     *
     * @param newOffset the new current offset
     * @return a new split with the updated offset
     */
    public IggySourceSplit withCurrentOffset(long newOffset) {
        return new IggySourceSplit(streamId, topicId, partitionId, startOffset, newOffset);
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

    public long getStartOffset() {
        return startOffset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IggySourceSplit that = (IggySourceSplit) o;
        return partitionId == that.partitionId
                && startOffset == that.startOffset
                && currentOffset == that.currentOffset
                && Objects.equals(streamId, that.streamId)
                && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, topicId, partitionId, startOffset, currentOffset);
    }

    @Override
    public String toString() {
        return "IggySourceSplit{"
                + "streamId='" + streamId + '\''
                + ", topicId='" + topicId + '\''
                + ", partitionId=" + partitionId
                + ", startOffset=" + startOffset
                + ", currentOffset=" + currentOffset
                + '}';
    }
}
