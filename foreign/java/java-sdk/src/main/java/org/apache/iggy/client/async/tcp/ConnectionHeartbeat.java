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

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** Pings a connection at a fixed interval on its event loop until the connection closes. */
final class ConnectionHeartbeat {
    private static final Logger log = LoggerFactory.getLogger(ConnectionHeartbeat.class);

    private final EventLoop eventLoop;
    private final long intervalNanos;
    private final Supplier<CompletableFuture<ByteBuf>> ping;
    private final BooleanSupplier closed;
    private final Object lock = new Object();
    private ScheduledFuture<?> task;
    private boolean running;
    private volatile Runnable listener = () -> {};

    ConnectionHeartbeat(
            EventLoop eventLoop,
            long intervalNanos,
            Supplier<CompletableFuture<ByteBuf>> ping,
            BooleanSupplier closed) {
        this.eventLoop = eventLoop;
        this.intervalNanos = intervalNanos;
        this.ping = ping;
        this.closed = closed;
    }

    void start() {
        synchronized (lock) {
            if (running || closed.getAsBoolean()) {
                return;
            }
            running = true;
            scheduleNext();
        }
    }

    private void scheduleNext() {
        synchronized (lock) {
            if (!running || closed.getAsBoolean()) {
                return;
            }
            try {
                task = eventLoop.schedule(this::send, intervalNanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException loopGone) {
                // Only a caller-owned group shuts down under a live connection.
                running = false;
                log.warn("Event loop rejected the heartbeat, stopping it: {}", loopGone.getMessage());
            }
        }
    }

    private void send() {
        synchronized (lock) {
            task = null;
            if (!running || closed.getAsBoolean()) {
                return;
            }
        }
        CompletableFuture<ByteBuf> heartbeat;
        try {
            heartbeat = ping.get();
        } catch (RuntimeException error) {
            log.warn("Failed to send heartbeat: {}", error.getMessage());
            scheduleNext();
            return;
        }
        heartbeat.whenComplete((response, error) -> {
            if (response != null) {
                response.release();
            }
            if (error != null && !closed.getAsBoolean()) {
                log.warn("Heartbeat failed: {}", error.getMessage());
            }
            scheduleNext();
            if (error == null) {
                listener.run();
            }
        });
    }

    /** Runs after each heartbeat that the server answered. */
    void onAnswered(Runnable answered) {
        listener = answered;
    }

    void stop() {
        synchronized (lock) {
            running = false;
            if (task != null) {
                task.cancel(false);
                task = null;
            }
        }
    }

    boolean scheduled() {
        synchronized (lock) {
            return task != null;
        }
    }
}
