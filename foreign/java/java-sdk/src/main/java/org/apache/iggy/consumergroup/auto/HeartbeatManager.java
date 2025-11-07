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

package org.apache.iggy.consumergroup.auto;

import org.apache.iggy.client.blocking.SystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages heartbeat sending for automatic consumer group management.
 */
public class HeartbeatManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatManager.class);
    
    private final ScheduledExecutorService scheduler;
    private final SystemClient systemClient;
    private final Duration heartbeatInterval;
    private final AtomicBoolean active;
    private ScheduledFuture<?> heartbeatTask;
    
    public HeartbeatManager(SystemClient systemClient, Duration heartbeatInterval) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Iggy-Heartbeat-Thread");
            t.setDaemon(true);
            return t;
        });
        this.systemClient = systemClient;
        this.heartbeatInterval = heartbeatInterval;
        this.active = new AtomicBoolean(false);
    }
    
    /**
     * Starts the heartbeat manager, sending periodic heartbeats.
     */
    public void start() {
        if (active.compareAndSet(false, true)) {
            logger.debug("Starting heartbeat manager with interval: {}", heartbeatInterval);
            heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeat,
                0,
                heartbeatInterval.toMillis(),
                TimeUnit.MILLISECONDS
            );
        }
    }
    
    /**
     * Stops the heartbeat manager.
     */
    public void stop() {
        if (active.compareAndSet(true, false)) {
            logger.debug("Stopping heartbeat manager");
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }
        }
    }
    
    /**
     * Sends a single heartbeat to the server.
     */
    private void sendHeartbeat() {
        if (!active.get()) {
            return;
        }
        
        try {
            logger.trace("Sending heartbeat");
            systemClient.ping();
            logger.trace("Heartbeat sent successfully");
        } catch (Exception e) {
            logger.warn("Failed to send heartbeat: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public void close() {
        stop();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}