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

import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;

/**
 * The state of one partition of a {@link PartitionReader}. Only that reader reads and writes it, on its loop, so the
 * fields need no accessors.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
final class Partition {
    final long id;
    long generation;
    PollingStrategy position;
    // The position came from a read, so it carries the history of the partition.
    boolean fromRead;
    boolean inFlight;
    // The read in flight holds quiet partitions, so it may wait.
    boolean parked;
    long retryAt;
    // The read turn that last returned messages of the partition.
    long servedAt;
    // The last read ended before the head of the partition.
    boolean backlogged;
    int lastingRefusals;
    ReadException stopped;
    PartitionContext context = PartitionContext.EMPTY;
    final PartitionOwners owners = new PartitionOwners();
    // The last offset that a read of the current generation returned.
    BigInteger received;
    BigInteger baseline;
    BigInteger firstDelivered;
    Checkpoint delivered;
    BigInteger committed;
    // Under OnRead, the start of the messages that a lost read stored, and the batch read after them.
    BigInteger gapFrom;
    BigInteger gapEnd;
    PolledMessages held;
    // Under OnRead, the start of what the server stored without handing it over, see PartitionReader.lose().
    PartitionReader.Lost lost;
    // Under OnRead, a start other than Next that waits for the lookup of the stored offset, see
    // PartitionReader.resolveStart().
    boolean startUnresolved;
    // Under OnRead, the lookup of the start found no stored offset. Until a batch arrives, the close moves back only
    // what the reads stored, see PartitionReader.rewind().
    boolean startFoundNoOffset;
    // Under OnRead, after a seek write with an unknown outcome, the offset after the one that the server held before
    // it, or 0 for none. Until a batch arrives, the close restores that, see PartitionReader.abandonUnknown().
    BigInteger restorePoint;
    // Under OnRead, the seeks that did not end, in call order. The first one runs once no read of the partition is in
    // flight, and the partition starts no read until the last one ends.
    final Deque<PartitionReader.Seek> seeks = new ArrayDeque<>();
    // The writes of the first seek and its move, in flight. The next seek and the close wait for them.
    CompletableFuture<Void> seeking;
    // The server refused a read with NotPrimary, so the partition reads nothing until the next lookup of the groups
    // ends.
    boolean misrouted;
    Checkpoint counted;
    Checkpoint stored;
    long sinceStore;

    Partition(long id) {
        this.id = id;
    }

    /** Whether this reader delivered {@code checkpoint} in the current generation of the partition. */
    boolean delivered(Checkpoint checkpoint) {
        return delivered != null
                && firstDelivered != null
                && owners.named(checkpoint.context())
                && checkpoint.offset().compareTo(firstDelivered) >= 0
                && checkpoint.offset().compareTo(delivered.offset()) <= 0;
    }
}
