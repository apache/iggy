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

import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * When {@link CommitPolicy.Auto} stores. It needs an {@code interval} or an {@code every}. It also stores on close, on
 * shutdown and before a rebalance takes a partition away. A store only moves forward, and a position of an older
 * generation of its partition, after a seek or a new assignment, is dropped.
 *
 * @param interval store the latest counted position at this interval, when it moved
 * @param every    also store it on this event
 * @param after    when a message counts
 */
public record AutoCommit(Optional<Duration> interval, Optional<CommitEvery> every, CommitAfter after) {
    private static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);
    private static final long DEFAULT_MESSAGES = 10_000;

    public AutoCommit {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(every, "every");
        Objects.requireNonNull(after, "after");
        interval.ifPresent(period -> {
            if (period.isNegative() || period.isZero()) {
                throw new IggyInvalidArgumentException("An automatic commit interval must be positive, not " + period);
            }
        });
    }

    /** Every 10,000 messages or 5 s, whichever comes first, after {@link CommitAfter#PROCESSING}. */
    public static AutoCommit defaults() {
        return new AutoCommit(
                Optional.of(DEFAULT_INTERVAL),
                Optional.of(CommitEvery.messages(DEFAULT_MESSAGES)),
                CommitAfter.PROCESSING);
    }

    public AutoCommit withInterval(Duration interval) {
        return new AutoCommit(Optional.of(interval), every, after);
    }

    public AutoCommit withoutInterval() {
        return new AutoCommit(Optional.empty(), every, after);
    }

    public AutoCommit withEvery(CommitEvery every) {
        return new AutoCommit(interval, Optional.of(every), after);
    }

    public AutoCommit withoutEvery() {
        return new AutoCommit(interval, Optional.empty(), after);
    }

    public AutoCommit withAfter(CommitAfter after) {
        return new AutoCommit(interval, every, after);
    }
}
