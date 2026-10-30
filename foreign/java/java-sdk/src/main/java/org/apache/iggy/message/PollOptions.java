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

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The limits of one poll of one partition, and who stores its offsets.
 *
 * @param count    most messages in one batch, 1 to 4294967295, default 1000
 * @param maxWait  zero answers at once, at most {@link DeferredPollOptions#MAX_WAIT}, default zero
 * @param minCount messages that end the wait, 1 to {@code count}, default 1
 * @param maxBytes the largest reply body, at least {@link DeferredPollOptions#MIN_MAX_BYTES}, default 1 MiB. The
 *                 server returns a first message larger than this alone in its reply
 * @param commit   who stores the offsets of what the poll reads, default {@link CommitPolicy#manual()}
 */
public record PollOptions(long count, Duration maxWait, long minCount, long maxBytes, CommitPolicy commit) {
    public static final long DEFAULT_COUNT = 1000;

    /** The request timeout is the wait plus this margin. */
    public static final Duration REQUEST_TIMEOUT_MARGIN = Duration.ofSeconds(10);

    private static final PollOptions DEFAULTS = new PollOptions(
            DEFAULT_COUNT,
            Duration.ZERO,
            DeferredPollOptions.DEFAULT_MIN_COUNT,
            DeferredPollOptions.DEFAULT_MAX_BYTES,
            CommitPolicy.manual());

    public PollOptions {
        Objects.requireNonNull(maxWait, "maxWait");
        Objects.requireNonNull(commit, "commit");
    }

    /** 1000 messages at most, answered at once, up to 1 MiB, nothing stored. */
    public static PollOptions defaults() {
        return DEFAULTS;
    }

    /** The defaults with a wait of up to {@code maxWait} for {@code minCount} messages. */
    public static PollOptions waitFor(Duration maxWait) {
        return DEFAULTS.withMaxWait(maxWait);
    }

    public PollOptions withCount(long count) {
        return new PollOptions(count, maxWait, minCount, maxBytes, commit);
    }

    public PollOptions withMaxWait(Duration maxWait) {
        return new PollOptions(count, maxWait, minCount, maxBytes, commit);
    }

    public PollOptions withMinCount(long minCount) {
        return new PollOptions(count, maxWait, minCount, maxBytes, commit);
    }

    public PollOptions withMaxBytes(long maxBytes) {
        return new PollOptions(count, maxWait, minCount, maxBytes, commit);
    }

    public PollOptions withCommit(CommitPolicy commit) {
        return new PollOptions(count, maxWait, minCount, maxBytes, commit);
    }

    /** The limit of the whole request: {@code maxWait} plus {@link #REQUEST_TIMEOUT_MARGIN}. */
    public Duration requestTimeout() {
        return maxWait.plus(REQUEST_TIMEOUT_MARGIN);
    }

    /** Whether the server stores the offset of each read. */
    public boolean commitsOnRead() {
        return commit instanceof CommitPolicy.OnRead;
    }

    /** The automatic commit settings, under {@link CommitPolicy.Auto}. */
    public Optional<AutoCommit> autoCommit() {
        return commit instanceof CommitPolicy.Auto auto ? Optional.of(auto.autoCommit()) : Optional.empty();
    }

    /**
     * Refuses what the server would refuse, and an automatic commit that would store only on close.
     *
     * @throws org.apache.iggy.exception.IggyInvalidArgumentException for limits outside their ranges
     * @throws IggyValidationException with {@link IggyErrorCode#INVALID_CONFIGURATION} for an automatic commit
     *                                 without an interval or an event
     */
    public void validate() {
        if (autoCommit()
                .filter(auto -> auto.interval().isEmpty() && auto.every().isEmpty())
                .isPresent()) {
            throw new IggyValidationException(
                    IggyErrorCode.INVALID_CONFIGURATION,
                    IggyErrorCode.INVALID_CONFIGURATION.getCode(),
                    "An automatic commit needs an interval or an event, or it would store only on close",
                    Optional.empty(),
                    Optional.empty());
        }
        deferredOptions().validate(count);
    }

    /** The wire limits of the poll, with the request timeout of {@link #requestTimeout()}. */
    public DeferredPollOptions deferredOptions() {
        return new DeferredPollOptions(maxWait, minCount, maxBytes, requestTimeout());
    }
}
