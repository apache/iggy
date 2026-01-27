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

package org.apache.iggy.config;

import java.time.Duration;

/**
 * Retry policy for client operations.
 *
 * <p>This class provides configuration for retry behavior including exponential backoff,
 * fixed delay, and no-retry strategies.
 */
public final class RetryPolicy {

    private final int maxRetries;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final double multiplier;

    private RetryPolicy(int maxRetries, Duration initialDelay, Duration maxDelay, double multiplier) {
        this.maxRetries = maxRetries;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.multiplier = multiplier;
    }

    /**
     * Creates a retry policy with exponential backoff using default parameters.
     *
     * <p>Default configuration:
     * <ul>
     *   <li>Max retries: 3</li>
     *   <li>Initial delay: 100ms</li>
     *   <li>Max delay: 5s</li>
     *   <li>Multiplier: 2.0</li>
     * </ul>
     *
     * @return a RetryPolicy with exponential backoff configuration
     */
    public static RetryPolicy exponentialBackoff() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
    }

    /**
     * Creates a retry policy with exponential backoff and custom parameters.
     *
     * @param maxRetries   the maximum number of retries
     * @param initialDelay the initial delay before the first retry
     * @param maxDelay     the maximum delay between retries
     * @param multiplier   the multiplier for exponential backoff
     * @return a RetryPolicy with custom exponential backoff configuration
     */
    public static RetryPolicy exponentialBackoff(
            int maxRetries, Duration initialDelay, Duration maxDelay, double multiplier) {
        return new RetryPolicy(maxRetries, initialDelay, maxDelay, multiplier);
    }

    /**
     * Creates a retry policy with fixed delay between retries.
     *
     * @param maxRetries the maximum number of retries
     * @param delay      the fixed delay between retries
     * @return a RetryPolicy with fixed delay configuration
     */
    public static RetryPolicy fixedDelay(int maxRetries, Duration delay) {
        return new RetryPolicy(maxRetries, delay, delay, 1.0);
    }

    /**
     * Creates a no-retry policy.
     *
     * @return a RetryPolicy that does not retry
     */
    public static RetryPolicy noRetry() {
        return new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 1.0);
    }

    /**
     * Gets the maximum number of retry attempts.
     *
     * @return the maximum number of retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Gets the initial delay before the first retry.
     *
     * @return the initial delay duration
     */
    public Duration getInitialDelay() {
        return initialDelay;
    }

    /**
     * Gets the maximum delay between retries.
     *
     * @return the maximum delay duration
     */
    public Duration getMaxDelay() {
        return maxDelay;
    }

    /**
     * Gets the multiplier for exponential backoff.
     *
     * @return the backoff multiplier
     */
    public double getMultiplier() {
        return multiplier;
    }
}
