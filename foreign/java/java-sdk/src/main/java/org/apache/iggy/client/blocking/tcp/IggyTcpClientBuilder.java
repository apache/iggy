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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyMissingCredentialsException;

import java.io.File;
import java.time.Duration;

/**
 * Builder for creating configured IggyTcpClient instances.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Basic usage with explicit connect and login
 * var client = IggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .build();
 * client.connect();
 * client.users().login("iggy", "iggy");
 *
 * // Convenience method with auto-login
 * var client = IggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin();
 *
 * // With TLS enabled
 * var client = IggyTcpClient.builder()
 *     .host("iggy-server.example.com")
 *     .port(8090)
 *     .enableTls()
 *     .credentials("admin", "secret")
 *     .buildAndLogin();
 * }</pre>
 *
 * @see IggyTcpClient#builder()
 */
public final class IggyTcpClientBuilder {

    private final AsyncIggyTcpClientBuilder asyncBuilder = new AsyncIggyTcpClientBuilder();
    private String username;
    private String password;

    IggyTcpClientBuilder() {}

    /**
     * Sets the host address for the Iggy server.
     *
     * @param host the host address
     * @return this builder
     */
    public IggyTcpClientBuilder host(String host) {
        asyncBuilder.host(host);
        return this;
    }

    /**
     * Sets the port for the Iggy server.
     *
     * @param port the port number
     * @return this builder
     */
    public IggyTcpClientBuilder port(Integer port) {
        asyncBuilder.port(port);
        return this;
    }

    /**
     * Sets the credentials for authentication.
     * These credentials are stored and can be used with {@link IggyTcpClient#login()}.
     *
     * @param username the username
     * @param password the password
     * @return this builder
     */
    public IggyTcpClientBuilder credentials(String username, String password) {
        this.username = username;
        this.password = password;
        asyncBuilder.credentials(username, password);
        return this;
    }

    /**
     * Sets the connection timeout.
     *
     * @param connectionTimeout the connection timeout duration
     * @return this builder
     */
    public IggyTcpClientBuilder connectionTimeout(Duration connectionTimeout) {
        asyncBuilder.connectionTimeout(connectionTimeout);
        return this;
    }

    /**
     * Sets the request timeout.
     *
     * @param requestTimeout the request timeout duration
     * @return this builder
     */
    public IggyTcpClientBuilder requestTimeout(Duration requestTimeout) {
        asyncBuilder.requestTimeout(requestTimeout);
        return this;
    }

    /**
     * Sets the connection pool size.
     *
     * @param connectionPoolSize the size of the connection pool
     * @return this builder
     */
    public IggyTcpClientBuilder connectionPoolSize(Integer connectionPoolSize) {
        asyncBuilder.connectionPoolSize(connectionPoolSize);
        return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retryPolicy the retry policy to use
     * @return this builder
     */
    public IggyTcpClientBuilder retryPolicy(RetryPolicy retryPolicy) {
        asyncBuilder.retryPolicy(retryPolicy);
        return this;
    }

    /**
     * Enables or disables TLS for the TCP connection.
     *
     * @param enableTls whether to enable TLS
     * @return this builder
     */
    public IggyTcpClientBuilder tls(boolean enableTls) {
        asyncBuilder.tls(enableTls);
        return this;
    }

    /**
     * Enables TLS for the TCP connection.
     *
     * @return this builder
     */
    public IggyTcpClientBuilder enableTls() {
        asyncBuilder.enableTls();
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file) to validate the server certificate.
     *
     * @param tlsCertificate the PEM file containing the certificate or CA chain
     * @return this builder
     */
    public IggyTcpClientBuilder tlsCertificate(File tlsCertificate) {
        asyncBuilder.tlsCertificate(tlsCertificate);
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file path) to validate the server certificate.
     *
     * @param tlsCertificatePath the PEM file path containing the certificate or CA chain
     * @return this builder
     */
    public IggyTcpClientBuilder tlsCertificate(String tlsCertificatePath) {
        asyncBuilder.tlsCertificate(tlsCertificatePath);
        return this;
    }

    /**
     * Builds and returns a configured IggyTcpClient instance.
     * Note: You still need to call {@link IggyTcpClient#connect()} on the returned client.
     *
     * @return a new IggyTcpClient instance
     * @throws org.apache.iggy.exception.IggyInvalidArgumentException if the host is null or empty, or if the port is not positive
     */
    public IggyTcpClient build() {
        AsyncIggyTcpClient asyncClient = asyncBuilder.build();
        return new IggyTcpClient(asyncClient);
    }

    /**
     * Builds, connects, and logs in using the provided credentials.
     * This is a convenience method equivalent to calling {@code build()}, {@code connect()},
     * and {@code login()}.
     *
     * @return a new IggyTcpClient instance that is connected and logged in
     * @throws IggyMissingCredentialsException if no credentials were provided
     * @throws org.apache.iggy.exception.IggyInvalidArgumentException if the host is null or empty, or if the port is not positive
     */
    public IggyTcpClient buildAndLogin() {
        if (username == null || password == null) {
            throw new IggyMissingCredentialsException(
                    "Credentials must be provided to use buildAndLogin(). Use credentials(username, password).");
        }
        IggyTcpClient client = build();
        try {
            client.connect();
            client.login();
            return client;
        } catch (RuntimeException e) {
            try {
                client.close();
            } catch (RuntimeException closeEx) {
                e.addSuppressed(closeEx);
            }
            throw e;
        }
    }
}
