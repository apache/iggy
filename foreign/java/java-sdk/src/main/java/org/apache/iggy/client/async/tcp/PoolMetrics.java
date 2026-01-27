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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for monitoring connection pool performance.
 * Tracks active connections, wait times, and errors.
 */
public class PoolMetrics {
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong totalWaitTimeNanos = new AtomicLong(0);
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong minWaitTimeNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxWaitTimeNanos = new AtomicLong(0);

    /**
     * Increments the count of active connections.
     */
    public void incrementActive() {
        activeConnections.incrementAndGet();
    }

    /**
     * Decrements the count of active connections.
     */
    public void decrementActive() {
        activeConnections.decrementAndGet();
    }

    /**
     * Records the wait time for acquiring a connection from the pool.
     *
     * @param waitTimeNanos wait time in nanoseconds
     */
    public void recordWaitTime(long waitTimeNanos) {
        totalRequests.incrementAndGet();
        totalWaitTimeNanos.addAndGet(waitTimeNanos);

        // Update min wait time
        long currentMin;
        do {
            currentMin = minWaitTimeNanos.get();
            if (waitTimeNanos >= currentMin) {
                break;
            }
        } while (!minWaitTimeNanos.compareAndSet(currentMin, waitTimeNanos));

        // Update max wait time
        long currentMax;
        do {
            currentMax = maxWaitTimeNanos.get();
            if (waitTimeNanos <= currentMax) {
                break;
            }
        } while (!maxWaitTimeNanos.compareAndSet(currentMax, waitTimeNanos));
    }

    /**
     * Records an error occurrence.
     */
    public void recordError() {
        totalErrors.incrementAndGet();
    }

    /**
     * Gets the current number of active connections.
     *
     * @return number of active connections
     */
    public int getActiveConnections() {
        return activeConnections.get();
    }

    /**
     * Gets the total number of requests made.
     *
     * @return total request count
     */
    public long getTotalRequests() {
        return totalRequests.get();
    }

    /**
     * Gets the total number of acquire requests (alias for getTotalRequests).
     *
     * @return total acquire request count
     */
    public long getTotalAcquireRequests() {
        return totalRequests.get();
    }

    /**
     * Gets the total number of errors encountered.
     *
     * @return total error count
     */
    public long getTotalErrors() {
        return totalErrors.get();
    }

    /**
     * Gets the average wait time in nanoseconds.
     *
     * @return average wait time, or 0 if no requests have been made
     */
    public long getAverageWaitTimeNanos() {
        long requests = totalRequests.get();
        return requests > 0 ? totalWaitTimeNanos.get() / requests : 0;
    }

    /**
     * Gets the average wait time in milliseconds.
     *
     * @return average wait time in milliseconds
     */
    public double getAverageWaitTimeMillis() {
        return getAverageWaitTimeNanos() / 1_000_000.0;
    }

    /**
     * Gets the minimum wait time in nanoseconds.
     *
     * @return minimum wait time, or 0 if no requests have been made
     */
    public long getMinWaitTimeNanos() {
        long min = minWaitTimeNanos.get();
        return min == Long.MAX_VALUE ? 0 : min;
    }

    /**
     * Gets the minimum wait time in milliseconds.
     *
     * @return minimum wait time in milliseconds
     */
    public double getMinWaitTimeMillis() {
        return getMinWaitTimeNanos() / 1_000_000.0;
    }

    /**
     * Gets the maximum wait time in nanoseconds.
     *
     * @return maximum wait time
     */
    public long getMaxWaitTimeNanos() {
        return maxWaitTimeNanos.get();
    }

    /**
     * Gets the maximum wait time in milliseconds.
     *
     * @return maximum wait time in milliseconds
     */
    public double getMaxWaitTimeMillis() {
        return getMaxWaitTimeNanos() / 1_000_000.0;
    }

    /**
     * Gets the error rate as a percentage.
     *
     * @return error rate (0-100), or 0 if no requests have been made
     */
    public double getErrorRate() {
        long requests = totalRequests.get();
        return requests > 0 ? (totalErrors.get() * 100.0) / requests : 0.0;
    }

    /**
     * Resets all metrics to their initial values.
     */
    public void reset() {
        activeConnections.set(0);
        totalWaitTimeNanos.set(0);
        totalRequests.set(0);
        totalErrors.set(0);
        minWaitTimeNanos.set(Long.MAX_VALUE);
        maxWaitTimeNanos.set(0);
    }

    @Override
    public String toString() {
        return String.format(
                "PoolMetrics{active=%d, requests=%d, errors=%d, avgWaitMs=%.2f, minWaitMs=%.2f, maxWaitMs=%.2f, errorRate=%.2f%%}",
                getActiveConnections(),
                getTotalRequests(),
                getTotalErrors(),
                getAverageWaitTimeMillis(),
                getMinWaitTimeMillis(),
                getMaxWaitTimeMillis(),
                getErrorRate());
    }
}
