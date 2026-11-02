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

package org.apache.iggy.client.async.tcp;

import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

/**
 * How a close shuts down an event loop group that the client created for itself. Netty runs every task that
 * arrives during the quiet period, and each such task starts the period again. The timeout ends the wait even
 * when tasks keep arriving.
 *
 * <p>A value that the builder leaves unset comes from a system property in milliseconds, which changes it for every
 * client in the JVM. Without the property, the Netty default applies.
 */
record EventLoopShutdown(Duration quietPeriod, Duration timeout) {

    static final String QUIET_PERIOD_PROPERTY = "iggy.tcp.shutdownQuietPeriodMillis";
    static final String TIMEOUT_PROPERTY = "iggy.tcp.shutdownTimeoutMillis";
    // What Netty's shutdownGracefully() uses without arguments.
    static final Duration DEFAULT_QUIET_PERIOD = Duration.ofSeconds(2);
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);

    /**
     * Resolves the builder values against the JVM system properties.
     *
     * @param quietPeriod the builder quiet period, or {@code null} if unset
     * @param timeout the builder timeout, or {@code null} if unset
     * @throws IggyInvalidArgumentException if a value is negative, a property is not a whole number of
     *     milliseconds, or the timeout is shorter than the quiet period
     */
    static EventLoopShutdown resolve(Duration quietPeriod, Duration timeout) {
        return resolve(quietPeriod, timeout, System::getProperty);
    }

    static EventLoopShutdown resolve(Duration quietPeriod, Duration timeout, UnaryOperator<String> properties) {
        Duration resolvedQuietPeriod = quietPeriod != null
                ? quietPeriod
                : fromProperty(properties, QUIET_PERIOD_PROPERTY, DEFAULT_QUIET_PERIOD);
        Duration resolvedTimeout =
                timeout != null ? timeout : fromProperty(properties, TIMEOUT_PROPERTY, DEFAULT_TIMEOUT);
        if (resolvedQuietPeriod.isNegative()) {
            throw new IggyInvalidArgumentException("ShutdownQuietPeriod cannot be negative");
        }
        if (resolvedTimeout.isNegative()) {
            throw new IggyInvalidArgumentException("ShutdownTimeout cannot be negative");
        }
        if (resolvedTimeout.compareTo(resolvedQuietPeriod) < 0) {
            throw new IggyInvalidArgumentException("ShutdownTimeout cannot be shorter than ShutdownQuietPeriod");
        }
        return new EventLoopShutdown(resolvedQuietPeriod, resolvedTimeout);
    }

    Future<?> shutdown(EventExecutorGroup group) {
        return group.shutdownGracefully(saturatedNanos(quietPeriod), saturatedNanos(timeout), TimeUnit.NANOSECONDS);
    }

    private static Duration fromProperty(UnaryOperator<String> properties, String name, Duration fallback) {
        String value = properties.apply(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        long millis;
        try {
            millis = Long.parseLong(value.strip());
        } catch (NumberFormatException notANumber) {
            throw invalidProperty(name, value);
        }
        if (millis < 0) {
            throw invalidProperty(name, value);
        }
        return Duration.ofMillis(millis);
    }

    private static IggyInvalidArgumentException invalidProperty(String name, String value) {
        return new IggyInvalidArgumentException(
                "System property " + name + " must be a whole number of milliseconds, zero or more, but is: " + value);
    }

    private static long saturatedNanos(Duration value) {
        try {
            return value.toNanos();
        } catch (ArithmeticException tooLong) {
            return Long.MAX_VALUE;
        }
    }
}
