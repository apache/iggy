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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Builds a {@link PartitionReader}. Get one from the {@code reader} method of a client. */
public final class PartitionReaderBuilder {
    /** The wait of a read of a reader, unless its options say otherwise. */
    public static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(5);

    public static final long DEFAULT_PREFETCH_MESSAGES = 16_000;
    public static final long DEFAULT_PREFETCH_BYTES = 16L * 1024 * 1024;
    public static final Duration DEFAULT_RETRY_INTERVAL = Duration.ofSeconds(1);

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
     * The start of a partition without a position. Default {@link PollingStrategy#next()}. {@link #build()} refuses a
     * continuation, because it belongs to one partition.
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

    /** The messages of the reads in flight and the queued batches. Default the larger of 16,000 and the count. */
    public PartitionReaderBuilder prefetchMessages(long messages) {
        this.prefetchMessages = messages;
        return this;
    }

    /** The bytes of the reads in flight and the queued batches. Default 16 MiB. */
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
     * Starts the reader. A consumer group reader joins its group first.
     *
     * @return the reader, or the error of the join
     * @throws IggyValidationException with {@link IggyErrorCode#INVALID_COMMAND} for partitions of a group, no
     *                                 partitions of a standalone consumer, or a continuation as the start
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
        if (prefetchBytes < options.maxBytes() || retryInterval.isNegative() || retryInterval.isZero()) {
            throw new IggyInvalidArgumentException(
                    "A reader needs room for one read in its prefetch and a positive retry interval");
        }
        long messages = prefetchMessages > 0 ? prefetchMessages : Math.max(DEFAULT_PREFETCH_MESSAGES, options.count());
        int slots = (int) Math.max(1, Math.min(messages / options.count(), prefetchBytes / options.maxBytes()));
        PollingStrategy first = start.kind() == PollingKind.First
                ? new PollingStrategy(PollingKind.Offset, BigInteger.ZERO, start.context())
                : start;
        var reader = new PartitionReader(
                transport, partitions == null ? List.of() : partitions, first, options, slots, retryInterval);
        return reader.start();
    }

    private static IggyValidationException invalid(String reason) {
        return new IggyValidationException(
                IggyErrorCode.INVALID_COMMAND,
                IggyErrorCode.INVALID_COMMAND.getCode(),
                reason,
                Optional.empty(),
                Optional.empty());
    }
}
