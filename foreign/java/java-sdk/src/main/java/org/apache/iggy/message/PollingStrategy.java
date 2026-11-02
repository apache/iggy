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

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

/**
 * Where a poll starts.
 *
 * @param kind        how {@code value} is read
 * @param value       the offset or timestamp of {@code kind}, zero for the other kinds
 * @param context     the history of the partition that an offset came from
 * @param partitionId the partition of a continuation, which a poll or a seek of another partition refuses
 */
public record PollingStrategy(
        PollingKind kind, BigInteger value, Optional<PartitionContext> context, Optional<Long> partitionId) {

    public PollingStrategy {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(partitionId, "partitionId");
    }

    public PollingStrategy(PollingKind kind, BigInteger value, Optional<PartitionContext> context) {
        this(kind, value, context, Optional.empty());
    }

    public PollingStrategy(PollingKind kind, BigInteger value) {
        this(kind, value, Optional.empty());
    }

    /** The offset {@code next} of {@code partitionId} under {@code context}, as a poll or a checkpoint continues. */
    static PollingStrategy continuation(long partitionId, BigInteger next, PartitionContext context) {
        return new PollingStrategy(PollingKind.Offset, next, Optional.of(context), Optional.of(partitionId));
    }

    public static PollingStrategy offset(BigInteger value) {
        return new PollingStrategy(PollingKind.Offset, value);
    }

    public static PollingStrategy timestamp(BigInteger value) {
        return new PollingStrategy(PollingKind.Timestamp, value);
    }

    public static PollingStrategy first() {
        return new PollingStrategy(PollingKind.First, BigInteger.ZERO);
    }

    public static PollingStrategy last() {
        return new PollingStrategy(PollingKind.Last, BigInteger.ZERO);
    }

    public static PollingStrategy next() {
        return new PollingStrategy(PollingKind.Next, BigInteger.ZERO);
    }

    /**
     * Continues at an offset under the partition incarnation and owner that produced it.
     *
     * <p>Pass the {@link PolledMessages#context()} of the poll the offset came from. The
     * client sends this context with the poll and with every retry of it. If the partition
     * was deleted and recreated since, the server refuses the poll with error 87 instead of
     * reading the offset in the new partition. If a group consumer's partition changed
     * owner, the server refuses it with error 5009.
     *
     * @param context the context of the poll the offset came from
     * @return a copy of this strategy that carries {@code context}
     */
    public PollingStrategy withContext(PartitionContext context) {
        return new PollingStrategy(kind, value, Optional.of(context), partitionId);
    }

    /**
     * Refuses a continuation of another partition.
     *
     * @param partition the partition that the strategy is used for
     * @throws IggyValidationException with {@link IggyErrorCode#INVALID_COMMAND} for a continuation of another partition
     */
    public void requirePartition(long partition) {
        if (partitionId.isPresent() && partitionId.get() != partition) {
            throw new IggyValidationException(
                    IggyErrorCode.INVALID_COMMAND,
                    IggyErrorCode.INVALID_COMMAND.getCode(),
                    "A continuation of partition " + partitionId.get() + " cannot start partition " + partition,
                    Optional.empty(),
                    Optional.empty());
        }
    }
}
