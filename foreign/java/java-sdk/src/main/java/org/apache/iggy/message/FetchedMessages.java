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

import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;

import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * One section of a fetch reply: what one partition read, or why it read nothing. A section
 * whose status is not {@link #STATUS_OK} holds the error code of its partition, no records and
 * zero in every other field, so the next fetch of that partition keeps the start point and the
 * context that it sent.
 *
 * @param partitionId   the partition of this section
 * @param status        {@link #STATUS_OK}, or the error code of this partition
 * @param currentOffset the current offset of the partition, as in a poll reply
 * @param count         the number of messages in {@code records}
 * @param context       the partition context for the strategy of the next fetch of this partition
 * @param nextOffset    the offset where the next fetch of this partition starts
 * @param records       the batch records as stored, decoded by {@link #decode()}
 * @param recoveryOperationId local recovery id when this section leaves committing progress uncertain
 */
public record FetchedMessages(
        long partitionId,
        long status,
        BigInteger currentOffset,
        long count,
        PartitionContext context,
        BigInteger nextOffset,
        byte[] records,
        OptionalLong recoveryOperationId) {
    public static final long STATUS_OK = 0;

    public FetchedMessages(
            long partitionId,
            long status,
            BigInteger currentOffset,
            long count,
            PartitionContext context,
            BigInteger nextOffset,
            byte[] records) {
        this(partitionId, status, currentOffset, count, context, nextOffset, records, OptionalLong.empty());
    }

    /** A local recovery identity for uncertain progress, independent of the original wire status. */
    public Optional<IggyOperationException> recoveryError() {
        return recoveryOperationId.isPresent()
                ? Optional.of(new IggyOperationException(
                        IggyErrorCode.FETCH_PROGRESS_UNCERTAIN, recoveryOperationId.getAsLong()))
                : Optional.empty();
    }

    /**
     * Decodes the records of this section.
     *
     * @return the messages of this section, with its partition, current offset and context
     * @throws IggyServerException for a section whose status is not {@link #STATUS_OK}, with
     *                             that status as its code
     */
    public PolledMessages decode() {
        recoveryError().ifPresent(error -> {
            throw error;
        });
        if (status != STATUS_OK) {
            throw IggyServerException.fromTcpResponse(status, new byte[0]);
        }
        return new PolledMessages(
                partitionId,
                currentOffset,
                count,
                BytesDeserializer.readMessages(Unpooled.wrappedBuffer(records), count),
                context);
    }
}
