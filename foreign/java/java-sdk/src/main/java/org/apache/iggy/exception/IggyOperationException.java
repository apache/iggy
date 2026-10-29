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

package org.apache.iggy.exception;

import java.util.Optional;
import java.util.OptionalLong;

/** A local recovery outcome carrying an unsigned recovery id, also for fetches without replayable receipts. */
public final class IggyOperationException extends IggyServerException {
    private final long operationId;
    private final OptionalLong partitionId;

    public IggyOperationException(IggyErrorCode code, long operationId) {
        this(code, operationId, OptionalLong.empty());
    }

    public IggyOperationException(IggyErrorCode code, long operationId, long partitionId) {
        this(code, operationId, OptionalLong.of(partitionId));
    }

    private IggyOperationException(IggyErrorCode code, long operationId, OptionalLong partitionId) {
        super(
                code,
                code.getCode(),
                code == IggyErrorCode.PARTITION_LANE_BLOCKED && partitionId.isPresent()
                        ? "Partition " + partitionId.getAsLong() + " blocked by operation "
                                + Long.toUnsignedString(operationId)
                        : "Operation " + Long.toUnsignedString(operationId),
                Optional.empty(),
                Optional.empty());
        this.operationId = operationId;
        this.partitionId = partitionId;
    }

    /** Returns the unsigned recovery id bits. Negative values are valid ids; fetch ids cannot be replayed. */
    public long getOperationId() {
        return operationId;
    }

    /** Returns the blocked partition as an unsigned 32-bit value, if this outcome names a partition. */
    public OptionalLong getPartitionId() {
        return partitionId;
    }
}
