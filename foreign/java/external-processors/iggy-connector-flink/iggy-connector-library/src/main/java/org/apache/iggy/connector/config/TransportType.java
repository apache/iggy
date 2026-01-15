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

package org.apache.iggy.connector.config;

/**
 * Transport protocol type for connecting to Iggy server.
 *
 * <p>The Flink connector supports two transport protocols:
 * <ul>
 *   <li>{@link #HTTP} - HTTP/REST API transport (default, port 3000)</li>
 *   <li>{@link #TCP} - Binary TCP transport (port 8090)</li>
 * </ul>
 *
 * <p>TCP transport provides lower latency and persistent connections,
 * making it ideal for high-throughput streaming workloads. HTTP transport
 * offers broader compatibility and simpler debugging.
 *
 * <p>Example usage:
 * <pre>{@code
 * IggyConnectionConfig config = IggyConnectionConfig.builder()
 *     .serverAddress("localhost")
 *     .transportType(TransportType.TCP)
 *     .tcpPort(8090)
 *     .username("iggy")
 *     .password("iggy")
 *     .build();
 * }</pre>
 */
public enum TransportType {

    /**
     * HTTP/REST API transport.
     * <p>Default transport type. Uses the HTTP API on port 3000 by default.
     * Suitable for development, debugging, and environments with HTTP proxies.
     */
    HTTP("http", 3000),

    /**
     * Binary TCP transport.
     * <p>Lower latency transport using persistent TCP connections on port 8090 by default.
     * Recommended for production streaming workloads requiring high throughput.
     */
    TCP("tcp", 8090);

    private final String protocolName;
    private final int defaultPort;

    TransportType(String protocolName, int defaultPort) {
        this.protocolName = protocolName;
        this.defaultPort = defaultPort;
    }

    /**
     * Gets the protocol name.
     *
     * @return the protocol name (e.g., "http", "tcp")
     */
    public String getProtocolName() {
        return protocolName;
    }

    /**
     * Gets the default port for this transport type.
     *
     * @return the default port number
     */
    public int getDefaultPort() {
        return defaultPort;
    }

    /**
     * Parses a transport type from string, case-insensitive.
     *
     * @param value the string value to parse
     * @return the corresponding TransportType
     * @throws IllegalArgumentException if the value is not a valid transport type
     */
    public static TransportType fromString(String value) {
        if (value == null || value.isBlank()) {
            return HTTP; // Default to HTTP
        }
        String normalized = value.trim().toUpperCase();
        try {
            return TransportType.valueOf(normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid transport type: '" + value + "'. Valid values are: HTTP, TCP", e);
        }
    }
}
