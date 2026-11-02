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

package org.apache.iggy.client.async.reader;

import org.apache.iggy.exception.IggyException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.message.DecodeFailure;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An error of a read. The reader stays usable after it and retries by itself. The cause is the error of the server
 * or the client.
 */
public final class ReadException extends IggyException {
    private static final byte[] NO_RECORD = new byte[0];

    private final OptionalLong partitionId;
    private final transient Optional<PollingStrategy> skip;
    private final transient Optional<BigInteger> offset;
    private final byte[] record;

    ReadException(OptionalLong partitionId, Throwable cause, Optional<PollingStrategy> skip) {
        this(partitionId, cause, skip, Optional.empty(), NO_RECORD);
    }

    ReadException(OptionalLong partitionId, DecodeFailure failure, Optional<PollingStrategy> skip) {
        this(partitionId, failure.error(), skip, failure.offset(), failure.record());
    }

    private ReadException(
            OptionalLong partitionId,
            Throwable cause,
            Optional<PollingStrategy> skip,
            Optional<BigInteger> offset,
            byte[] record) {
        super(
                partitionId.isPresent()
                        ? "Partition " + partitionId.getAsLong() + ": " + cause.getMessage()
                        : String.valueOf(cause.getMessage()),
                cause);
        this.partitionId = partitionId;
        this.skip = skip;
        this.offset = offset;
        this.record = record;
    }

    /** The partition of the error, or empty for an error of the whole reader. */
    public OptionalLong partitionId() {
        return partitionId;
    }

    /**
     * For a message that does not decode, the continuation after it. The reader reads the message again until a
     * {@link PartitionReader#seek} to this continuation skips it. The messages before it arrive in a batch before this
     * error.
     */
    public Optional<PollingStrategy> skip() {
        return skip;
    }

    /**
     * For a message that does not decode, its offset, known when its frame decoded and a later check failed. Empty
     * when the framing of the reply is malformed, so no message can be isolated, and for every other error.
     */
    public Optional<BigInteger> offset() {
        return offset;
    }

    /**
     * For a message that does not decode, the message as the reply carried it: the frame header, the payload and the
     * user headers. For malformed framing, the remaining bytes of the section from where decoding stopped. Empty for
     * every other error.
     */
    public byte[] record() {
        return record;
    }

    /** The code of the server error behind this one, or empty for an error of the client. */
    public OptionalLong errorCode() {
        return getCause() instanceof IggyServerException server
                ? OptionalLong.of(server.getRawErrorCode())
                : OptionalLong.empty();
    }
}
