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

package org.apache.iggy.message;

import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.Objects;

/**
 * The position after one delivered message: its partition, its offset and the history of the partition it came from.
 * Only a poll or a reader makes one, with {@link PolledMessages#checkpoint()} or {@link PolledMessages#checkpointAt}.
 * Storing it stores the offset of that message for the consumer.
 */
public final class Checkpoint {
    private final long partitionId;
    private final BigInteger offset;
    private final PartitionContext context;

    Checkpoint(long partitionId, BigInteger offset, PartitionContext context) {
        this.partitionId = partitionId;
        this.offset = Objects.requireNonNull(offset, "offset");
        this.context = Objects.requireNonNull(context, "context");
    }

    public long partitionId() {
        return partitionId;
    }

    /** The offset of the delivered message. */
    public BigInteger offset() {
        return offset;
    }

    /** The history of the partition that the message came from, which a store sends so it fails on another one. */
    public PartitionContext context() {
        return context;
    }

    /** The continuation after this message, bound to its partition. */
    public PollingStrategy next() {
        return PollingStrategy.continuation(partitionId, offset.add(BigInteger.ONE), context);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Checkpoint checkpoint
                && partitionId == checkpoint.partitionId
                && offset.equals(checkpoint.offset)
                && context.equals(checkpoint.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, offset, context);
    }

    @Override
    public String toString() {
        return "Checkpoint[partitionId=" + partitionId + ", offset=" + offset + ", context=" + context + "]";
    }
}
