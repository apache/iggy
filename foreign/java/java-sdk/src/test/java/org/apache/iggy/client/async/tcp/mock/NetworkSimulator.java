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

package org.apache.iggy.client.async.tcp.mock;

/**
 * Network simulator for testing various network conditions.
 * Provides utilities for simulating latency, packet loss, and bandwidth limitations.
 * 
 * Note: This is a placeholder implementation. In a real implementation, this would
 * integrate with network simulation libraries or use more advanced techniques.
 */
public class NetworkSimulator {
    
    // These would typically be implemented using network simulation libraries
    // or by wrapping the network connections with delay/loss injection
    
    private static double packetLossRate = 0.0;
    private static long latencyMs = 0;
    private static int bandwidthLimitBytesPerSecond = 0;

    /**
     * Adds latency to network operations.
     *
     * @param latencyMs the latency in milliseconds
     */
    public static void addLatency(long latencyMs) {
        NetworkSimulator.latencyMs = latencyMs;
    }

    /**
     * Sets the packet loss rate for network operations.
     *
     * @param rate the packet loss rate (0.0 to 1.0)
     */
    public static void setPacketLossRate(double rate) {
        if (rate < 0.0 || rate > 1.0) {
            throw new IllegalArgumentException("Packet loss rate must be between 0.0 and 1.0");
        }
        NetworkSimulator.packetLossRate = rate;
    }

    /**
     * Limits the bandwidth for network operations.
     *
     * @param bytesPerSecond the maximum bytes per second
     */
    public static void limitBandwidth(int bytesPerSecond) {
        if (bytesPerSecond < 0) {
            throw new IllegalArgumentException("Bandwidth limit must be non-negative");
        }
        NetworkSimulator.bandwidthLimitBytesPerSecond = bytesPerSecond;
    }

    /**
     * Gets the current packet loss rate.
     *
     * @return the packet loss rate
     */
    public static double getPacketLossRate() {
        return packetLossRate;
    }

    /**
     * Gets the current latency.
     *
     * @return the latency in milliseconds
     */
    public static long getLatencyMs() {
        return latencyMs;
    }

    /**
     * Gets the current bandwidth limit.
     *
     * @return the bandwidth limit in bytes per second
     */
    public static int getBandwidthLimitBytesPerSecond() {
        return bandwidthLimitBytesPerSecond;
    }

    /**
     * Resets all network simulation settings.
     */
    public static void reset() {
        packetLossRate = 0.0;
        latencyMs = 0;
        bandwidthLimitBytesPerSecond = 0;
    }
}