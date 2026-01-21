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
import java.util.concurrent.atomic.LongAdder;

public class PoolMetrics {

    private final AtomicInteger activeConnections = new AtomicInteger();
    private final LongAdder totalErrors = new LongAdder();
    private final LongAdder totalWaitTimeNanos = new LongAdder();
    private final LongAdder totalAcquireRequests = new LongAdder();

    public int getActiveConnections() {
        return activeConnections.get();
    }

    public long getTotalErrors() {
        return totalErrors.sum();
    }

    public long getAverageWaitTimeMicros() {
        long requests = totalAcquireRequests.sum();
        return requests == 0 ? 0 : (totalWaitTimeNanos.sum() / requests) / 1000;
    }

    public long getTotalAcquireRequests() {
        return totalAcquireRequests.sum();
    }

    public void incrementActive() {
        activeConnections.incrementAndGet();
    }

    public void decrementActive() {
        activeConnections.decrementAndGet();
    }

    public void recordError() {
        totalErrors.increment();
    }

    public void recordWaitTime(long nanoTime) {
        totalWaitTimeNanos.add(nanoTime);
        totalAcquireRequests.increment();
    }
}
