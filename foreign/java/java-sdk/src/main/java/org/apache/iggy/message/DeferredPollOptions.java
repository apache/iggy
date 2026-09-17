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
 *
 * <p>{@code maxBytes} bounds the encoded response body, including the 16-byte
 * poll prefix and all batch framing, but excluding the 256-byte transport
 * header. It is not a payload-only limit, and not a bound on server disk work.
 *
 * @param maxWait        readiness wait, zero through 600 s; the server's
 *                       configured ceiling can be lower, 30 s by default
 * @param minCount       messages that make the poll ready, at least one and no
 *                       greater than the poll's {@code count}
 * @param maxBytes       maximum encoded response body, at least 16
 * @param requestTimeout total request budget, at least {@code maxWait} and no
 *                       more than 630 s
 */
public record DeferredPollOptions(Duration maxWait, long minCount, long maxBytes, Duration requestTimeout) {

    /** Finite protocol ceiling; a server may admit less. */
    public static final Duration MAX_WAIT = Duration.ofSeconds(600);

    public static final Duration MAX_REQUEST_TIMEOUT = MAX_WAIT.plusSeconds(30);

    /** The poll response prefix ({@code partition:u32}, {@code offset:u64}, {@code count:u32}). */
    public static final long MIN_MAX_BYTES = 16;

    public static final Duration DEFAULT_MAX_WAIT = Duration.ofSeconds(1);
    public static final long DEFAULT_MIN_COUNT = 1;
    public static final long DEFAULT_MAX_BYTES = 1024L * 1024L;
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(11);

    private static final DeferredPollOptions DEFAULTS =
            new DeferredPollOptions(DEFAULT_MAX_WAIT, DEFAULT_MIN_COUNT, DEFAULT_MAX_BYTES, DEFAULT_REQUEST_TIMEOUT);

    private static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;
    private static final long NANOS_PER_MICRO = 1000;

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
        if (requestTimeout.compareTo(maxWait) < 0) {
            throw new IggyInvalidArgumentException(
                    "Deferred poll requestTimeout " + requestTimeout + " is shorter than maxWait " + maxWait);
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
