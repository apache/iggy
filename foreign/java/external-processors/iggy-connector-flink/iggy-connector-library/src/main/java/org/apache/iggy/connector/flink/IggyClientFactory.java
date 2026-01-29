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

package org.apache.iggy.connector.flink;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.config.TransportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Factory for creating Iggy clients based on connection configuration.
 * <p>
 * Supports both HTTP and TCP transport protocols, providing a unified
 * interface for client creation across the Flink connector.
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
 *
 * IggyBaseClient client = IggyClientFactory.createClient(config);
 * }</pre>
 */
public final class IggyClientFactory implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(IggyClientFactory.class);

    private IggyClientFactory() {
        // Utility class
    }

    /**
     * Creates an Iggy client based on the connection configuration.
     * <p>
     * The client type (HTTP or TCP) is determined by the {@link TransportType}
     * configured in the connection config. The client is automatically connected
     * and authenticated.
     *
     * @param config the connection configuration
     * @return a connected and authenticated Iggy client
     * @throws RuntimeException if client creation or connection fails
     */
    public static IggyBaseClient createClient(IggyConnectionConfig config) {
        TransportType transportType = config.getTransportType();

        log.info("Creating Iggy client with transport type: {}", transportType);

        switch (transportType) {
            case TCP:
                return createTcpClient(config);
            case HTTP:
            default:
                return createHttpClient(config);
        }
    }

    /**
     * Creates an HTTP Iggy client.
     *
     * @param config the connection configuration
     * @return a connected and authenticated HTTP client
     */
    private static IggyBaseClient createHttpClient(IggyConnectionConfig config) {
        try {
            String host = extractHost(config.getServerAddress());
            int port = config.getHttpPort();

            String httpUrl = buildHttpUrl(host, port, config.isEnableTls());

            log.debug("Creating HTTP client for URL: {}", httpUrl);

            IggyHttpClient httpClient = new IggyHttpClient(httpUrl);
            httpClient.users().login(config.getUsername(), config.getPassword());

            log.info("Successfully created and authenticated HTTP client to {}", httpUrl);
            return httpClient;

        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create HTTP Iggy client: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a TCP Iggy client.
     *
     * @param config the connection configuration
     * @return a connected and authenticated TCP client
     */
    private static IggyBaseClient createTcpClient(IggyConnectionConfig config) {
        try {
            String host = extractHost(config.getServerAddress());
            int port = config.getTcpPort();

            log.debug("Creating TCP client for {}:{}", host, port);

            IggyTcpClient tcpClient = IggyTcpClient.builder()
                    .host(host)
                    .port(port)
                    .credentials(config.getUsername(), config.getPassword())
                    .tls(config.isEnableTls())
                    .build();

            log.info("Successfully created and authenticated TCP client to {}:{}", host, port);
            return tcpClient;

        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create TCP Iggy client: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the host from a server address.
     * <p>
     * Handles various address formats:
     * <ul>
     *   <li>Plain hostname: "localhost"</li>
     *   <li>Host with port: "localhost:8090"</li>
     *   <li>Full URL: "http://localhost:3000"</li>
     * </ul>
     *
     * @param serverAddress the server address
     * @return the extracted host
     */
    static String extractHost(String serverAddress) {
        if (serverAddress == null || serverAddress.isBlank()) {
            throw new IllegalArgumentException("serverAddress cannot be null or blank");
        }

        String address = serverAddress.trim();

        // If it contains a scheme, parse as URI
        if (address.contains("://")) {
            try {
                URI uri = new URI(address);
                String host = uri.getHost();
                if (host == null || host.isBlank()) {
                    throw new IllegalArgumentException("Cannot extract host from URI: " + serverAddress);
                }
                return host;
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid server address format: " + serverAddress, e);
            }
        }

        // If it contains a port separator, extract the host part
        // Handle IPv6 addresses (contain multiple colons) by splitting from the right
        if (address.contains(":")) {
            // For IPv6 addresses, the last colon separates the port
            // For IPv4 addresses, the last colon separates the port
            int lastColonIndex = address.lastIndexOf(':');
            if (lastColonIndex > 0) {
                String host = address.substring(0, lastColonIndex);
                // Remove brackets from IPv6 addresses if present
                if (host.startsWith("[") && host.endsWith("]")) {
                    host = host.substring(1, host.length() - 1);
                }
                if (!host.isBlank()) {
                    return host;
                }
            }
            throw new IllegalArgumentException("Cannot extract host from address: " + serverAddress);
        }

        // Plain hostname
        return address;
    }

    /**
     * Builds an HTTP URL from host and port.
     *
     * @param host      the host
     * @param port      the port
     * @param enableTls whether to use HTTPS
     * @return the HTTP URL
     */
    private static String buildHttpUrl(String host, int port, boolean enableTls) {
        String scheme = enableTls ? "https" : "http";
        return scheme + "://" + host + ":" + port;
    }
}
