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

package org.apache.iggy.connector.serialization;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata about a record from Iggy.
 * Contains information like offset, partition, timestamp that may be
 * useful during deserialization.
 */
public final class RecordMetadata implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final RecordMetadata EMPTY = new RecordMetadata(null, null, null, null);

    private final String streamId;
    private final String topicId;
    private final Integer partitionId;
    private final Long offset;

    private RecordMetadata(String streamId, String topicId, Integer partitionId, Long offset) {
        this.streamId = streamId;
        this.topicId = topicId;
        this.partitionId = partitionId;
        this.offset = offset;
    }

    public static RecordMetadata empty() {
        return EMPTY;
    }

    public static RecordMetadata of(String streamId, String topicId, Integer partitionId, Long offset) {
        return new RecordMetadata(streamId, topicId, partitionId, offset);
    }

    public String getStreamId() {
        return streamId;
    }

    public String getTopicId() {
        return topicId;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public Long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordMetadata that = (RecordMetadata) o;
        return Objects.equals(streamId, that.streamId)
                && Objects.equals(topicId, that.topicId)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamId, topicId, partitionId, offset);
    }

    @Override
    public String toString() {
        return "RecordMetadata{"
                + "streamId='" + streamId + '\''
                + ", topicId='" + topicId + '\''
                + ", partitionId=" + partitionId
                + ", offset=" + offset
                + '}';
    }
}
