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

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;

import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Builder for creating configured AsyncIggyTcpClient instances.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Basic usage with explicit connect and login
 * var client = AsyncIggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .build();
 * client.connect().join();
 * client.users().login("iggy", "iggy").join();
 *
 * // Convenience method with auto-login
 * var client = AsyncIggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin()
 *     .join();
 *
 * // With TLS enabled
 * var client = AsyncIggyTcpClient.builder()
 *     .host("iggy-server.example.com")
 *     .port(8090)
 *     .enableTls()
 *     .credentials("admin", "secret")
 *     .buildAndLogin()
 *     .join();
 * }</pre>
 *
 * @see AsyncIggyTcpClient#builder()
 */
public final class AsyncIggyTcpClientBuilder {
    private String host = "localhost";
    private Integer port = 8090;
    private String username;
    private String password;
    private boolean enableTls = false;
    private File tlsCertificate;
    private Duration connectionTimeout;
    private Duration requestTimeout;
    private Integer connectionPoolSize;
    private RetryPolicy retryPolicy;
    private Duration acquireTimeout;

    public AsyncIggyTcpClientBuilder() {}

    /**
     * Sets the host address for the Iggy server.
     *
     * @param host the host address
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * Sets the port for the Iggy server.
     *
     * @param port the port number
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder port(Integer port) {
        this.port = port;
        return this;
    }

    /**
     * Sets the credentials for authentication.
     * These credentials are stored and can be used with {@link AsyncIggyTcpClient#login()}.
     *
     * @param username the username
     * @param password the password
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder credentials(String username, String password) {
        this.username = username;
        this.password = password;
        return this;
    }

    /**
     * Enables or disables TLS for the TCP connection.
     *
     * @param enableTls whether to enable TLS
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder tls(boolean enableTls) {
        this.enableTls = enableTls;
        return this;
    }

    /**
     * Enables TLS for the TCP connection.
     *
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder enableTls() {
        this.enableTls = true;
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file) to validate the server certificate.
     *
     * @param tlsCertificate the PEM file containing the certificate or CA chain
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder tlsCertificate(File tlsCertificate) {
        this.tlsCertificate = tlsCertificate;
        return this;
    }

    /**
     * Sets a custom trusted certificate (PEM file path) to validate the server certificate.
     *
     * @param tlsCertificatePath the PEM file path containing the certificate or CA chain
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder tlsCertificate(String tlsCertificatePath) {
        this.tlsCertificate = StringUtils.isBlank(tlsCertificatePath) ? null : new File(tlsCertificatePath);
        return this;
    }

    /**
     * Sets channel acquire timeout
     *
     * @param acquireTimeout the acquire timeout duration
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder acquireTimeout(Duration acquireTimeout) {
        this.acquireTimeout = acquireTimeout;
        return this;
    }

    /**
     * Sets the connection timeout.
     *
     * @param connectionTimeout the connection timeout duration
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder connectionTimeout(Duration connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    /**
     * Sets the request timeout.
     *
     * @param requestTimeout the request timeout duration
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder requestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Sets the connection pool size.
     *
     * @param connectionPoolSize the connection pool size
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder connectionPoolSize(Integer connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retryPolicy the retry policy
     * @return this builder
     */
    public AsyncIggyTcpClientBuilder retryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
        return this;
    }

    /**
     * Builds and returns a configured AsyncIggyTcpClient instance.
     * Note: You still need to call {@link AsyncIggyTcpClient#connect()} on the returned client.
     *
     * @return a new AsyncIggyTcpClient instance
     * @throws IggyInvalidArgumentException if the host is null or empty, or if the port is not positive
     */
    public AsyncIggyTcpClient build() {
        validateHost();
        validatePort();
        validateConnectionPoolSize();
        validateConnectionTimeout();
        validateAcquireTimeout();

        return new AsyncIggyTcpClient(
                host,
                port,
                username,
                password,
                connectionTimeout,
                acquireTimeout,
                requestTimeout,
                connectionPoolSize,
                retryPolicy,
                enableTls,
                Optional.ofNullable(tlsCertificate));
    }

    private void validateHost() {
        if (host == null || host.isEmpty()) {
            throw new IggyInvalidArgumentException("Host cannot be null or empty");
        }
    }

    private void validatePort() {
        if (port == null || port <= 0) {
            throw new IggyInvalidArgumentException("Port must be a positive integer");
        }
    }

    private void validateConnectionPoolSize() {
        if (connectionPoolSize != null && connectionPoolSize <= 0) {
            throw new IggyInvalidArgumentException("Connection pool size cannot by 0 or negative");
        }
    }

    private void validateConnectionTimeout() {
        if (connectionTimeout == null) {
            return;
        }
        if (connectionTimeout.equals(Duration.ZERO) || connectionTimeout.isNegative()) {
            throw new IggyInvalidArgumentException("ConnectionTimeout Cannot be 0 or Negative");
        }
        if (connectionTimeout.toMillis() > ((long) Integer.MAX_VALUE)) {
            throw new IggyInvalidArgumentException(
                    String.format("ConnectionTimeout Cannot be greater than %d", Integer.MAX_VALUE));
        }
    }

    private void validateAcquireTimeout() {
        if (acquireTimeout != null && (acquireTimeout.equals(Duration.ZERO) || acquireTimeout.isNegative())) {
            throw new IggyInvalidArgumentException("AcquireTimeout Cannot be 0 or Negative");
        }
    }

    /**
     * Builds, connects, and logs in using the provided credentials.
     * This is a convenience method equivalent to calling {@code build()}, {@code connect()},
     * and {@code login()}.
     *
     * @return a CompletableFuture that completes with the connected and logged in client
     * @throws IggyMissingCredentialsException if no credentials were provided
     * @throws IggyInvalidArgumentException if the host is null or empty, or if the port is not positive
     */
    public CompletableFuture<AsyncIggyTcpClient> buildAndLogin() {
        if (username == null || password == null) {
            throw new IggyMissingCredentialsException(
                    "Credentials must be provided to use buildAndLogin(). Use credentials(username, password).");
        }
        AsyncIggyTcpClient client = build();
        return client.connect()
                .thenCompose(v -> client.login())
                .thenApply(v -> client)
                .handle((result, ex) -> {
                    if (ex == null) {
                        return CompletableFuture.completedFuture(result);
                    }
                    return client.close().<AsyncIggyTcpClient>handle((ignored, closeEx) -> {
                        if (closeEx != null) {
                            ex.addSuppressed(closeEx);
                        }
                        throw (RuntimeException) ex;
                    });
                })
                .thenCompose(Function.identity());
    }
}
