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
