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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Host and port of an Iggy TCP endpoint.
 *
 * @param host server host
 * @param port server TCP port
 */
public record TcpEndpoint(String host, int port) {

    public static final int DEFAULT_PORT = 8090;

    /**
     * Parses an Iggy TCP server address.
     *
     * @param serverAddress address in host, host:port, or tcp://host:port form
     * @return parsed TCP endpoint
     */
    public static TcpEndpoint parse(String serverAddress) {
        if (serverAddress == null || serverAddress.isBlank()) {
            throw new IllegalArgumentException("TCP server address cannot be null or blank");
        }

        URI uri = parseUri(serverAddress);
        validateScheme(uri);
        String host = extractHost(uri, serverAddress);
        validateComponents(uri, serverAddress);

        int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
        validatePort(port, serverAddress);
        return new TcpEndpoint(host, port);
    }

    private static URI parseUri(String serverAddress) {
        try {
            return serverAddress.contains("://") ? new URI(serverAddress) : new URI("tcp://" + serverAddress);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid TCP server address: " + serverAddress, e);
        }
    }

    private static void validateScheme(URI uri) {
        if (!"tcp".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Unsupported TCP server address scheme: " + uri.getScheme());
        }
    }

    private static String extractHost(URI uri, String serverAddress) {
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Cannot extract host from TCP server address: " + serverAddress);
        }
        return host;
    }

    private static void validateComponents(URI uri, String serverAddress) {
        if (uri.getUserInfo() != null
                || !uri.getPath().isEmpty()
                || uri.getQuery() != null
                || uri.getFragment() != null) {
            throw new IllegalArgumentException("Invalid TCP server address: " + serverAddress);
        }
    }

    private static void validatePort(int port, String serverAddress) {
        if (port == 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid TCP port in server address: " + serverAddress);
        }
    }
}
