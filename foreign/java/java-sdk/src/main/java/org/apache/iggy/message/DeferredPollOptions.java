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
import java.time.temporal.ChronoUnit;
import java.util.Optional;

/**
 * Readiness and response limits for a deferred (long) poll, independent of the
 * maximum message count. Mirrors {@code DeferredPollOptions} in
 * {@code core/common/src/types/message/deferred_poll_options.rs}.
 *
 * <p>A deferred poll returns as soon as {@code minCount} messages are readable,
 * when {@code maxBytes} prevents selecting more, or when {@code maxWait} expires.
 * Readiness expiry permits a final bounded read, so it can return partial or
 * empty data. {@code requestTimeout} is the total budget for routing, waiting,
 * reading and the reply; exhausting it is an error, never an empty success.
 * After a positive wait ends, the server still needs {@link #REPLY_MARGIN} for
 * the final read and the reply, so the budget must cover both.
 *
 * <p>{@code maxBytes} bounds the encoded response body, including the 40-byte
 * poll prefix and all batch framing, but excluding the 256-byte transport
 * header. It is not a payload-only limit, and not a bound on server disk work.
 * A first message larger than it comes alone in the reply, up to
 * {@link #MAX_RECORD_SIZE}.
 * The smallest limit, {@link #MIN_MAX_BYTES}, adds one batch header and one
 * message header to the prefix, the least room any message needs.
 *
 * @param maxWait        readiness wait, zero through 600 s; the server's
 *                       configured ceiling can be lower, 30 s by default
 * @param minCount       messages that make the poll ready, at least one and no
 *                       greater than the poll's {@code count}
 * @param maxBytes       maximum encoded response body, at least
 *                       {@link #MIN_MAX_BYTES} (344)
 * @param requestTimeout total request budget, positive and no more than 630 s;
 *                       with a positive {@code maxWait}, at least
 *                       {@code maxWait} plus {@link #REPLY_MARGIN}
 */
public record DeferredPollOptions(Duration maxWait, long minCount, long maxBytes, Duration requestTimeout) {

    /** Finite protocol ceiling; a server may admit less. */
    public static final Duration MAX_WAIT = Duration.ofSeconds(600);

    public static final Duration MAX_REQUEST_TIMEOUT = MAX_WAIT.plusSeconds(30);

    /** Mirrors {@code MIN_DEFERRED_POLL_REPLY_MARGIN_US}, which the server enforces when it decodes a poll. */
    public static final Duration REPLY_MARGIN = Duration.ofSeconds(1);

    /**
     * Mirrors {@code MIN_DEFERRED_POLL_MAX_BYTES}, the smallest limit that can return a message. The parts are
     * qualified because checkstyle wants these private constants declared below.
     */
    public static final long MIN_MAX_BYTES = DeferredPollOptions.POLL_RESPONSE_HEADER_SIZE
            + DeferredPollOptions.BATCH_HEADER_SIZE
            + DeferredPollOptions.BATCH_MESSAGE_HEADER_SIZE;

    /**
     * Mirrors {@code MAX_RECORD_SIZE}: the largest record of one message in a read reply, with its batch and message
     * headers. A read returns a first record up to this size even above its {@code maxBytes}, so every reply limit of
     * the client holds it.
     */
    public static final long MAX_RECORD_SIZE = DeferredPollOptions.BATCH_HEADER_SIZE
            + DeferredPollOptions.BATCH_MESSAGE_HEADER_SIZE
            + DeferredPollOptions.MAX_PAYLOAD_SIZE
            + DeferredPollOptions.MAX_USER_HEADERS_SIZE;

    /** The largest poll reply body that holds one record of {@link #MAX_RECORD_SIZE}. */
    public static final long MAX_RECORD_POLL_REPLY_SIZE =
            DeferredPollOptions.POLL_RESPONSE_HEADER_SIZE + MAX_RECORD_SIZE;

    public static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(1);
    public static final long DEFAULT_MIN_COUNT = 1;
    public static final long DEFAULT_MAX_BYTES = 1024L * 1024L;
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(11);

    private static final DeferredPollOptions DEFAULTS =
            new DeferredPollOptions(DEFAULT_MAX_WAIT, DEFAULT_MIN_COUNT, DEFAULT_MAX_BYTES, DEFAULT_REQUEST_TIMEOUT);

    private static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;
    private static final long NANOS_PER_MICRO = 1000;

    /** The poll response prefix, including the partition context. */
    private static final long POLL_RESPONSE_HEADER_SIZE = 40;

    private static final long BATCH_HEADER_SIZE = 256;
    private static final long BATCH_MESSAGE_HEADER_SIZE = 48;
    private static final long MAX_PAYLOAD_SIZE = 64_000_000;
    private static final long MAX_USER_HEADERS_SIZE = 100_000;

    public DeferredPollOptions {
        requireMicrosecondDuration(maxWait, "maxWait");
        requireMicrosecondDuration(requestTimeout, "requestTimeout");
        if (maxWait.compareTo(MAX_WAIT) > 0) {
            throw new IggyInvalidArgumentException("Deferred poll maxWait " + maxWait + " exceeds the protocol maximum "
                    + MAX_WAIT + "; a server can admit less");
        }
        if (requestTimeout.isZero() || requestTimeout.compareTo(MAX_REQUEST_TIMEOUT) > 0) {
            throw new IggyInvalidArgumentException("Deferred poll requestTimeout " + requestTimeout
                    + " must be positive and no more than " + MAX_REQUEST_TIMEOUT);
        }
        if (!maxWait.isZero() && requestTimeout.compareTo(maxWait.plus(REPLY_MARGIN)) < 0) {
            throw new IggyInvalidArgumentException("Deferred poll requestTimeout " + requestTimeout
                    + " is shorter than maxWait " + maxWait + " plus the reply margin " + REPLY_MARGIN);
        }
        if (minCount < 1 || minCount > MAX_UNSIGNED_INT) {
            throw new IggyInvalidArgumentException(
                    "Deferred poll minCount " + minCount + " must be between 1 and " + MAX_UNSIGNED_INT);
        }
        if (maxBytes < MIN_MAX_BYTES || maxBytes > MAX_UNSIGNED_INT) {
            throw new IggyInvalidArgumentException("Deferred poll maxBytes " + maxBytes + " must be between "
                    + MIN_MAX_BYTES + " and " + MAX_UNSIGNED_INT);
        }
    }

    /** Wake on the first readable message within one second, up to 1 MiB. */
    public static DeferredPollOptions defaults() {
        return DEFAULTS;
    }

    /**
     * Each {@code with} call validates the whole record, so a wait that leaves
     * less than {@link #REPLY_MARGIN} of the current {@code requestTimeout}
     * throws. With the defaults that is any wait above 10 s: raise the budget
     * with {@link #withRequestTimeout} first.
     */
    public DeferredPollOptions withMaxWait(Duration maxWait) {
        return new DeferredPollOptions(maxWait, minCount, maxBytes, requestTimeout);
    }

    public DeferredPollOptions withMinCount(long minCount) {
        return new DeferredPollOptions(maxWait, minCount, maxBytes, requestTimeout);
    }

    public DeferredPollOptions withMaxBytes(long maxBytes) {
        return new DeferredPollOptions(maxWait, minCount, maxBytes, requestTimeout);
    }

    public DeferredPollOptions withRequestTimeout(Duration requestTimeout) {
        return new DeferredPollOptions(maxWait, minCount, maxBytes, requestTimeout);
    }

    /**
     * Checks the one rule that depends on the poll itself. Call it before
     * allocating buffers or acquiring a connection.
     *
     * @param count the maximum message count the poll requests
     * @throws IggyInvalidArgumentException if the readiness target cannot be met
     */
    public void validate(long count) {
        if (count < 1 || count > MAX_UNSIGNED_INT) {
            throw new IggyInvalidArgumentException(
                    "Poll count " + count + " must be between 1 and " + MAX_UNSIGNED_INT);
        }
        if (minCount > count) {
            throw new IggyInvalidArgumentException(
                    "Deferred poll minCount " + minCount + " exceeds the requested count " + count);
        }
    }

    /**
     * What is left of both budgets once {@code elapsed} has passed, so a retry
     * continues them instead of starting over. Both shrink by the same whole
     * microseconds, which keeps the reply margin, and the wait stops at zero.
     *
     * @param elapsed time spent since the poll began
     * @return the remaining options, or empty once the request budget is spent
     */
    public Optional<DeferredPollOptions> remaining(Duration elapsed) {
        if (elapsed.compareTo(requestTimeout) >= 0) {
            return Optional.empty();
        }
        // Rounding the spent time up keeps whole microseconds and never overstates a budget.
        long spentMicros = (Math.max(0, elapsed.toNanos()) + NANOS_PER_MICRO - 1) / NANOS_PER_MICRO;
        long requestMicros = requestTimeoutMicros() - spentMicros;
        if (requestMicros <= 0) {
            return Optional.empty();
        }
        return Optional.of(new DeferredPollOptions(
                Duration.of(Math.max(0, maxWaitMicros() - spentMicros), ChronoUnit.MICROS),
                minCount,
                maxBytes,
                Duration.of(requestMicros, ChronoUnit.MICROS)));
    }

    public long maxWaitMicros() {
        return maxWait.toNanos() / NANOS_PER_MICRO;
    }

    public long requestTimeoutMicros() {
        return requestTimeout.toNanos() / NANOS_PER_MICRO;
    }

    /**
     * Rejects what the wire cannot carry exactly. Silently truncating a
     * sub-microsecond wait would change a caller's readiness budget, and a
     * duration too large for nanosecond arithmetic cannot be compared either.
     */
    private static void requireMicrosecondDuration(Duration duration, String name) {
        if (duration.isNegative()) {
            throw new IggyInvalidArgumentException("Deferred poll " + name + " " + duration + " must not be negative");
        }
        long nanos;
        try {
            nanos = duration.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IggyInvalidArgumentException(
                    "Deferred poll " + name + " " + duration + " is too large to express", overflow);
        }
        if (nanos % NANOS_PER_MICRO != 0) {
            throw new IggyInvalidArgumentException(
                    "Deferred poll " + name + " " + duration + " must be a whole number of microseconds");
        }
    }
}
