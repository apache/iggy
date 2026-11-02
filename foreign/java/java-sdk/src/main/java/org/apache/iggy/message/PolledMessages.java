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
import java.util.List;
import java.util.Optional;

/**
 * One batch of one partition.
 *
 * @param partitionId   the partition of the batch
 * @param currentOffset the offset of the last message of the partition when the server read it
 * @param count         the number of messages
 * @param messages      the messages in offset order
 * @param context       the history of the partition that the messages came from
 */
public record PolledMessages(
        Long partitionId, BigInteger currentOffset, Long count, List<Message> messages, PartitionContext context) {

    /** The position after the last message, or empty for an empty batch. */
    public Optional<Checkpoint> checkpoint() {
        return messages.isEmpty() ? Optional.empty() : checkpointAt(messages.size() - 1);
    }

    /** The position after the message at {@code index}, or empty for an index outside the batch. */
    public Optional<Checkpoint> checkpointAt(int index) {
        if (index < 0 || index >= messages.size()) {
            return Optional.empty();
        }
        return Optional.of(
                new Checkpoint(partitionId, messages.get(index).header().offset(), context));
    }

    /**
     * Where the next poll of this partition starts: after the last message. Empty for an empty batch, whose start the
     * caller keeps. A poll or a seek of another partition refuses it.
     */
    public Optional<PollingStrategy> continuation() {
        return checkpoint().map(Checkpoint::next);
    }
}
