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

/** Where a poll or fetch that stores the offsets that it reads reported an error. */
public enum CommitErrorScope {
    /** The whole reply is the error. */
    REQUEST,
    /** One section of a fetch reply carries the error status. */
    SECTION;

    /**
     * Whether {@code status}, reported in this scope, proves that the server stored no offset. Any other status, a
     * lost reply and a transport failure after the send leave the outcome unknown.
     */
    public boolean provesNothingStored(long status) {
        return switch (IggyErrorCode.fromCode((int) status)) {
            // The server refuses a first message larger than the byte limit before it stores an offset, and a read
            // that needs a new offset key above the limit returns no messages.
            case TRANSIENT_NOT_ACCEPTED,
                    INVALID_SIZE_BYTES,
                    HISTORY_UNAVAILABLE,
                    NOT_PRIMARY,
                    PARTITION_NOT_FOUND,
                    TOO_MANY_CONSUMER_OFFSETS,
                    CONSUMER_GROUP_PARTITION_NOT_OWNED -> true;
            case FEATURE_UNAVAILABLE,
                    INVALID_IDENTIFIER,
                    DEFERRED_POLL_CAPACITY_EXCEEDED,
                    STREAM_ID_NOT_FOUND,
                    STREAM_NAME_NOT_FOUND,
                    TOPIC_ID_NOT_FOUND,
                    TOPIC_NAME_NOT_FOUND,
                    INVALID_OFFSET -> this == REQUEST;
            default -> false;
        };
    }
}
