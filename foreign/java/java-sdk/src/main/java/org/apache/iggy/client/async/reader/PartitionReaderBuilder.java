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

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.iggy.client.async.reader.ReaderSupport.invalid;

/** Builds a {@link PartitionReader}. Get one from the {@code reader} method of a client. */
public final class PartitionReaderBuilder {
    /** The wait of a read of a reader, unless its options say otherwise. */
    public static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(5);

    public static final long DEFAULT_PREFETCH_MESSAGES = 16_000;
    public static final long DEFAULT_PREFETCH_BYTES = 16L * 1024 * 1024;
    public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(1);
    public static final int MIN_PREFETCH_SLOTS = 32;

    // The wire carries a partition id as an unsigned 32-bit integer, and the client refuses any other id unsent.
    private static final long MAX_PARTITION_ID = 0xFFFF_FFFFL;

    private final ReaderTransport transport;
    private List<Long> partitions;
    private PollingStrategy start = PollingStrategy.next();
    private PollOptions options = PollOptions.waitFor(DEFAULT_MAX_WAIT);
    private long prefetchMessages;
    private long prefetchBytes = DEFAULT_PREFETCH_BYTES;
    private Duration retryInterval = DEFAULT_RETRY_INTERVAL;

    PartitionReaderBuilder(ReaderTransport transport) {
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /**
     * The partitions of a standalone consumer, which requires them. A consumer group reads the assignment of its
     * member, so {@link #build()} refuses them for a group.
     */
    public PartitionReaderBuilder partitions(Collection<Long> partitions) {
        this.partitions = List.copyOf(partitions);
        return this;
    }

    /**
     * The start of a partition without a position. Default {@link PollingStrategy#next()}. With {@code OnRead}, the
     * start applies only to a partition without a stored offset. A partition with one resumes after it.
     * {@link #build()} refuses a continuation, because it belongs to one partition.
     */
    public PartitionReaderBuilder start(PollingStrategy start) {
        this.start = Objects.requireNonNull(start, "start");
        return this;
    }

    /**
     * The options of each read and the commit policy of the reader. Default {@code PollOptions.waitFor(5 s)}, which
     * stores nothing.
     */
    public PartitionReaderBuilder options(PollOptions options) {
        this.options = Objects.requireNonNull(options, "options");
        return this;
    }

    /**
     * The messages of the reads in flight and the queued batches. Default 16,000, which 0 keeps. The effective message
     * and byte budgets each hold at least {@value #MIN_PREFETCH_SLOTS} full batches, also when smaller limits are
     * requested, so with the default options a reader reads ahead up to 32,000 messages and 32 MiB. At most 32 reads
     * of one reader run at once.
     *
     * <p>With {@code OnRead}, the server stores the offset of each read before the application takes its batch. A
     * crash, or a reader that stops without {@link PartitionReader#closeAsync()}, loses the batches that the reader
     * held: up to the effective budget, which holds at least {@value #MIN_PREFETCH_SLOTS} batches. {@code Auto} or
     * {@code Manual} commit stores offsets after delivery instead.
     */
    public PartitionReaderBuilder prefetchMessages(long messages) {
        this.prefetchMessages = messages;
        return this;
    }

    /**
     * The bytes of the reads in flight and the queued batches. Default 16 MiB. The effective budget holds at least
     * {@value #MIN_PREFETCH_SLOTS} times the maximum bytes of a read, see {@link #prefetchMessages(long)}.
     */
    public PartitionReaderBuilder prefetchBytes(long bytes) {
        this.prefetchBytes = bytes;
        return this;
    }

    /** The pause before a refused or failed read is sent again. Default 1 s. */
    public PartitionReaderBuilder retryInterval(Duration retryInterval) {
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        return this;
    }

    /**
     * Starts the reader. A consumer group reader joins its group first. A join whose outcome stays unknown fails the
     * build, and the reader abandons it in the client, so the next build can join. If that join landed, the server
     * keeps the member until the session of the client ends.
     *
     * @return the reader, or the error of the join
     * @throws IggyValidationException with {@link IggyErrorCode#INVALID_COMMAND} for partitions of a group, no
     *                                 partitions of a standalone consumer, or a continuation as the start
     * @throws IggyInvalidArgumentException for a negative prefetch budget, a retry interval that is not positive, or a
     *                                      partition id outside 0 to 4294967295
     */
    public CompletableFuture<PartitionReader> build() {
        options.validate();
        if (start.partitionId().isPresent()) {
            throw invalid("A reader starts every partition, so its start cannot be the continuation of one");
        }
        if (transport.group() == (partitions != null)) {
            throw invalid(
                    transport.group()
                            ? "A consumer group reads the assignment of its member, not chosen partitions"
                            : "A standalone consumer needs its partitions");
        }
        if (Math.min(prefetchMessages, prefetchBytes) < 0 || retryInterval.isNegative() || retryInterval.isZero()) {
            throw new IggyInvalidArgumentException(
                    "A reader needs nonnegative prefetch budgets and a positive retry interval");
        }
        requireWirePartitionIds();
        long messages = prefetchMessages > 0 ? prefetchMessages : DEFAULT_PREFETCH_MESSAGES;
        int slots = (int) Math.min(
                Integer.MAX_VALUE,
                Math.max(MIN_PREFETCH_SLOTS, Math.min(messages / options.count(), prefetchBytes / options.maxBytes())));
        var reader = new PartitionReader(
                transport, partitions == null ? List.of() : partitions, startPosition(), options, slots, retryInterval);
        return reader.start();
    }

    private void requireWirePartitionIds() {
        if (partitions != null && partitions.stream().anyMatch(id -> id < 0 || id > MAX_PARTITION_ID)) {
            throw new IggyInvalidArgumentException("A partition id of a reader is between 0 and " + MAX_PARTITION_ID);
        }
    }

    /** Under OnRead the start applies only without a stored offset, and then the server answers Next from offset 0. */
    private PollingStrategy startPosition() {
        if (start.kind() != PollingKind.First) {
            return start;
        }
        PollingKind kind = options.commitsOnRead() ? PollingKind.Next : PollingKind.Offset;
        return new PollingStrategy(kind, BigInteger.ZERO, start.context());
    }
}
