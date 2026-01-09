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

import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

/**
 * Represents an offset in an Iggy stream partition.
 * Iggy uses monotonically increasing long values for offsets.
 */
public class IggyStreamPartitionMsgOffset implements StreamPartitionMsgOffset {

    private final long offset;

    /**
     * Creates a new offset wrapper.
     *
     * @param offset the Iggy offset value
     */
    public IggyStreamPartitionMsgOffset(long offset) {
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public int compareTo(StreamPartitionMsgOffset other) {
        if (other instanceof IggyStreamPartitionMsgOffset) {
            IggyStreamPartitionMsgOffset otherOffset = (IggyStreamPartitionMsgOffset) other;
            return Long.compare(this.offset, otherOffset.offset);
        } else if (other instanceof LongMsgOffset) {
            // Handle comparison with Pinot's LongMsgOffset
            LongMsgOffset longOffset = (LongMsgOffset) other;
            return Long.compare(this.offset, longOffset.getOffset());
        }
        throw new IllegalArgumentException("Cannot compare with incompatible offset type: " + other.getClass());
    }

    @Override
    public String toString() {
        return String.valueOf(offset);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IggyStreamPartitionMsgOffset that = (IggyStreamPartitionMsgOffset) o;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset);
    }
}
