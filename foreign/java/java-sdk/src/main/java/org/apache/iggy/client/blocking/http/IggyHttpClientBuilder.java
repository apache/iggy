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

package org.apache.iggy.client.blocking.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;

import java.io.File;
import java.time.Duration;

/**
 * Builder for creating configured IggyHttpClient instances.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Basic usage with explicit login
 * var client = IggyHttpClient.builder()
 *     .url("http://localhost:3000")
 *     .build();
 * client.users().login("iggy", "iggy");
 *
 * // Using host/port instead of full URL
 * var client = IggyHttpClient.builder()
 *     .host("localhost")
 *     .port(3000)
 *     .build();
 * client.users().login("iggy", "iggy");
 *
 * // Convenience method with auto-login
 * var client = IggyHttpClient.builder()
 *     .host("localhost")
 *     .port(3000)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin();
 *
 * // HTTPS with auto-login
 * var client = IggyHttpClient.builder()
 *     .host("iggy-server.example.com")
 *     .port(443)
 *     .enableTls()
 *     .credentials("admin", "secret")
 *     .buildAndLogin();
 * }</pre>
 *
 * @see IggyHttpClient#builder()
 */
public final class IggyHttpClientBuilder {
    private static final String HTTPS_PROTOCOL = "https";
    private static final String HTTP_PROTOCOL = "http";

    private String url;
    private String host = "localhost";
    private Integer port = IggyHttpClient.DEFAULT_HTTP_PORT;
    private boolean enableTls = false;
    private File tlsCertificate;
    private Duration connectionTimeout;
    private Duration requestTimeout;
    private String username;
    private String password;

    IggyHttpClientBuilder() {}

    /**
     * Sets the full URL for the Iggy server.
     * If set, this takes precedence over host/port/tls settings.
     *
     * @param url the full URL (e.g., "http://localhost:3000")
     * @return this builder
     */
    public IggyHttpClientBuilder url(String url) {
        this.url = url;
        return this;
    }

    /**
     * Sets the host address for the Iggy server.
     *
     * @param host the host address
     * @return this builder
     */
    public IggyHttpClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Sets the port for the Iggy server.
     *
     * @param port the port number
     * @return this builder
     */
    public IggyHttpClientBuilder port(Integer port) {
        this.port = port;
        return this;
    }

    /**
     * Enables TLS for the HTTP connection.
     *
     * @return this builder
     */
    public IggyHttpClientBuilder enableTls() {
        this.enableTls = true;
        return this;
    }

    /**
     * Enables or disables TLS for the HTTP connection.
     *
     * @param enableTls whether to enable TLS
     * @return this builder
     */
    public IggyHttpClientBuilder tls(boolean enableTls) {
        this.enableTls = enableTls;
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file) to validate the server certificate.
     *
     * @param tlsCertificate the PEM file containing the certificate or CA chain
     * @return this builder
     */
    public IggyHttpClientBuilder tlsCertificate(File tlsCertificate) {
        this.tlsCertificate = tlsCertificate;
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file path) to validate the server certificate.
     *
     * @param tlsCertificatePath the PEM file path containing the certificate or CA chain
     * @return this builder
     */
    public IggyHttpClientBuilder tlsCertificate(String tlsCertificatePath) {
        this.tlsCertificate = StringUtils.isBlank(tlsCertificatePath) ? null : new File(tlsCertificatePath);
        return this;
    }

    /**
     * Sets the connection timeout.
     *
     * @param connectionTimeout the connection timeout duration
     * @return this builder
     */
    public IggyHttpClientBuilder connectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * Sets the request timeout.
     *
     * @param requestTimeout the request timeout duration
     * @return this builder
     */
    public IggyHttpClientBuilder requestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Sets the credentials for authentication.
     * These credentials are stored and can be used with {@link IggyHttpClient#login()}.
     *
     * @param username the username
     * @param password the password
     * @return this builder
     */
    public IggyHttpClientBuilder credentials(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Builds and returns a configured IggyHttpClient instance.
     * The client will not be logged in.
     *
     * @return a new IggyHttpClient instance
     * @throws IggyInvalidArgumentException if the host is null or empty (when url is not set),
     *         or if the port is not positive (when url is not set)
     */
    public IggyHttpClient build() {
        String finalUrl;
        if (StringUtils.isNotBlank(url)) {
            finalUrl = url;
        } else {
            if (StringUtils.isBlank(host)) {
                throw new IggyInvalidArgumentException("Host cannot be null or empty");
            }
            if (port == null || port <= 0) {
                throw new IggyInvalidArgumentException("Port must be a positive integer");
            }
            String protocol = enableTls ? HTTPS_PROTOCOL : HTTP_PROTOCOL;
            finalUrl = protocol + "://" + host + ":" + port;
        }
        return new IggyHttpClient(finalUrl, username, password, connectionTimeout, requestTimeout, tlsCertificate);
    }

    /**
     * Builds and logs in using the provided credentials.
     * This is a convenience method equivalent to calling {@code build()} followed by {@code login()}.
     *
     * @return a new IggyHttpClient instance that is logged in
     * @throws IggyMissingCredentialsException if no credentials were provided
     * @throws IggyInvalidArgumentException if the host is null or empty (when url is not set),
     *         or if the port is not positive (when url is not set)
     */
    public IggyHttpClient buildAndLogin() {
        if (username == null || password == null) {
            throw new IggyMissingCredentialsException(
                    "Credentials must be provided to use buildAndLogin(). Use credentials(username, password).");
        }
        IggyHttpClient client = build();
        client.login();
        return client;
    }
}
