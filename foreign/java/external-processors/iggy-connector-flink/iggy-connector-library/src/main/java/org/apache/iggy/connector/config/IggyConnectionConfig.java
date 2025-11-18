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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for connecting to an Iggy server.
 * This class is framework-agnostic and can be reused across different
 * stream processing engines (Flink, Spark, etc.).
 *
 * <p>Example usage:
 * <pre>{@code
 * IggyConnectionConfig config = IggyConnectionConfig.builder()
 *     .serverAddress("localhost:8080")
 *     .username("iggy")
 *     .password("iggy")
 *     .connectionTimeout(Duration.ofSeconds(30))
 *     .build();
 * }</pre>
 */
public final class IggyConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String serverAddress;
    private final String username;
    private final String password;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;
    private final int maxRetries;
    private final Duration retryBackoff;
    private final boolean enableTls;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private IggyConnectionConfig(
            String serverAddress,
            String username,
            String password,
            Duration connectionTimeout,
            Duration requestTimeout,
            int maxRetries,
            Duration retryBackoff,
            boolean enableTls) {
        this.serverAddress = Objects.requireNonNull(serverAddress, "serverAddress must not be null");
        this.username = Objects.requireNonNull(username, "username must not be null");
        this.password = Objects.requireNonNull(password, "password must not be null");
        this.connectionTimeout = Objects.requireNonNull(connectionTimeout, "connectionTimeout must not be null");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
        this.maxRetries = maxRetries;
        this.retryBackoff = Objects.requireNonNull(retryBackoff, "retryBackoff must not be null");
        this.enableTls = enableTls;

        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryBackoff() {
        return retryBackoff;
    }

    public boolean isEnableTls() {
        return enableTls;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IggyConnectionConfig that = (IggyConnectionConfig) o;

        return new EqualsBuilder()
                .append(maxRetries, that.maxRetries)
                .append(enableTls, that.enableTls)
                .append(serverAddress, that.serverAddress)
                .append(username, that.username)
                .append(password, that.password)
                .append(connectionTimeout, that.connectionTimeout)
                .append(requestTimeout, that.requestTimeout)
                .append(retryBackoff, that.retryBackoff)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(serverAddress)
                .append(username)
                .append(password)
                .append(connectionTimeout)
                .append(requestTimeout)
                .append(maxRetries)
                .append(retryBackoff)
                .append(enableTls)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "IggyConnectionConfig{"
                + "serverAddress='" + serverAddress + '\''
                + ", username='" + username + '\''
                + ", connectionTimeout=" + connectionTimeout
                + ", requestTimeout=" + requestTimeout
                + ", maxRetries=" + maxRetries
                + ", retryBackoff=" + retryBackoff
                + ", enableTls=" + enableTls
                + '}';
    }

    /**
     * Builder for {@link IggyConnectionConfig}.
     */
    public static final class Builder {
        private String serverAddress;
        private String username;
        private String password;
        private Duration connectionTimeout = Duration.ofSeconds(30);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private int maxRetries = 3;
        private Duration retryBackoff = Duration.ofMillis(100);
        private boolean enableTls = false;

        private Builder() {}

        public Builder serverAddress(String serverAddress) {
            this.serverAddress = serverAddress;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryBackoff(Duration retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder enableTls(boolean enableTls) {
            this.enableTls = enableTls;
            return this;
        }

        public IggyConnectionConfig build() {
            return new IggyConnectionConfig(
                    serverAddress,
                    username,
                    password,
                    connectionTimeout,
                    requestTimeout,
                    maxRetries,
                    retryBackoff,
                    enableTls);
        }
    }
}
