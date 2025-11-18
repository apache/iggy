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

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.client.blocking.ConsumerGroupsClient;
import org.apache.iggy.client.blocking.ConsumerOffsetsClient;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClient;
import org.apache.iggy.client.blocking.PartitionsClient;
import org.apache.iggy.client.blocking.PersonalAccessTokensClient;
import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.client.blocking.TopicsClient;
import org.apache.iggy.client.blocking.UsersClient;

import java.io.File;
import java.time.Duration;
import java.util.Optional;

public class IggyTcpClient implements IggyBaseClient {

    private final UsersTcpClient usersClient;
    private final StreamsTcpClient streamsClient;
    private final TopicsTcpClient topicsClient;
    private final PartitionsTcpClient partitionsClient;
    private final ConsumerGroupsTcpClient consumerGroupsClient;
    private final ConsumerOffsetTcpClient consumerOffsetsClient;
    private final MessagesTcpClient messagesClient;
    private final SystemTcpClient systemClient;
    private final PersonalAccessTokensTcpClient personalAccessTokensClient;
    private final String host;
    private final Integer port;
    private final Optional<String> username;
    private final Optional<String> password;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> requestTimeout;
    private final Optional<Integer> connectionPoolSize;
    private final Optional<RetryPolicy> retryPolicy;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;

    public IggyTcpClient(String host, Integer port) {
        this(host, port, null, null, null, null, null, null, false, Optional.empty());
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private IggyTcpClient(
            String host,
            Integer port,
            String username,
            String password,
            Duration connectionTimeout,
            Duration requestTimeout,
            Integer connectionPoolSize,
            RetryPolicy retryPolicy,
            boolean enableTls,
            Optional<File> tlsCertificate) {
        this.host = host;
        this.port = port;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
        this.connectionTimeout = Optional.ofNullable(connectionTimeout);
        this.requestTimeout = Optional.ofNullable(requestTimeout);
        this.connectionPoolSize = Optional.ofNullable(connectionPoolSize);
        this.retryPolicy = Optional.ofNullable(retryPolicy);
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;

        InternalTcpClient tcpClient = new InternalTcpClient(host, port, enableTls, tlsCertificate);
        tcpClient.connect();
        usersClient = new UsersTcpClient(tcpClient);
        streamsClient = new StreamsTcpClient(tcpClient);
        topicsClient = new TopicsTcpClient(tcpClient);
        partitionsClient = new PartitionsTcpClient(tcpClient);
        consumerGroupsClient = new ConsumerGroupsTcpClient(tcpClient);
        consumerOffsetsClient = new ConsumerOffsetTcpClient(tcpClient);
        messagesClient = new MessagesTcpClient(tcpClient);
        systemClient = new SystemTcpClient(tcpClient);
        personalAccessTokensClient = new PersonalAccessTokensTcpClient(tcpClient);

        // Auto-login if credentials are provided
        if (this.username.isPresent() && this.password.isPresent()) {
            usersClient.login(this.username.get(), this.password.get());
        }
    }

    /**
     * Creates a new builder for configuring IggyTcpClient.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public SystemClient system() {
        return systemClient;
    }

    @Override
    public StreamsClient streams() {
        return streamsClient;
    }

    @Override
    public UsersClient users() {
        return usersClient;
    }

    @Override
    public TopicsClient topics() {
        return topicsClient;
    }

    @Override
    public PartitionsClient partitions() {
        return partitionsClient;
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        return consumerGroupsClient;
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        return consumerOffsetsClient;
    }

    @Override
    public MessagesClient messages() {
        return messagesClient;
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        return personalAccessTokensClient;
    }

    /**
     * Builder for creating configured IggyTcpClient instances.
     */
    public static final class Builder {
        private String host = "localhost";
        private Integer port = 8090;
        private String username;
        private String password;
        private Duration connectionTimeout;
        private Duration requestTimeout;
        private Integer connectionPoolSize;
        private RetryPolicy retryPolicy;
        private boolean enableTls = false;
        private File tlsCertificate;

        private Builder() {}

        /**
         * Sets the host address for the Iggy server.
         *
         * @param host the host address
         * @return this builder
         */
        public Builder host(String host) {
            this.host = host;
            return this;
        }

        /**
         * Sets the port for the Iggy server.
         *
         * @param port the port number
         * @return this builder
         */
        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        /**
         * Sets the credentials for authentication.
         *
         * @param username the username
         * @param password the password
         * @return this builder
         */
        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Sets the connection timeout.
         *
         * @param connectionTimeout the connection timeout duration
         * @return this builder
         */
        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the request timeout.
         *
         * @param requestTimeout the request timeout duration
         * @return this builder
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Sets the connection pool size.
         *
         * @param connectionPoolSize the size of the connection pool
         * @return this builder
         */
        public Builder connectionPoolSize(Integer connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        /**
         * Sets the retry policy.
         *
         * @param retryPolicy the retry policy to use
         * @return this builder
         */
        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * Enables or disables TLS for the TCP connection.
         *
         * @param enableTls whether to enable TLS
         * @return this builder
         */
        public Builder tls(boolean enableTls) {
            this.enableTls = enableTls;
            return this;
        }

        /**
         * Enables TLS for the TCP connection.
         *
         * @return this builder
         */
        public Builder enableTls() {
            this.enableTls = true;
            return this;
        }

        /**
         * Sets a custom trusted certificate (PEM file) to validate the server certificate.
         *
         * @param tlsCertificate the PEM file containing the certificate or CA chain
         * @return this builder
         */
        public Builder tlsCertificate(File tlsCertificate) {
            this.tlsCertificate = tlsCertificate;
            return this;
        }

        /**
         * Sets a custom trusted certificate (PEM file path) to validate the server certificate.
         *
         * @param tlsCertificatePath the PEM file path containing the certificate or CA chain
         * @return this builder
         */
        public Builder tlsCertificate(String tlsCertificatePath) {
            this.tlsCertificate = StringUtils.isBlank(tlsCertificatePath) ? null : new File(tlsCertificatePath);
            return this;
        }

        /**
         * Builds and returns a configured IggyTcpClient instance.
         *
         * @return a new IggyTcpClient instance
         */
        public IggyTcpClient build() {
            if (host == null || host.isEmpty()) {
                throw new IllegalArgumentException("Host cannot be null or empty");
            }
            if (port == null || port <= 0) {
                throw new IllegalArgumentException("Port must be a positive integer");
            }
            return new IggyTcpClient(
                    host,
                    port,
                    username,
                    password,
                    connectionTimeout,
                    requestTimeout,
                    connectionPoolSize,
                    retryPolicy,
                    this.enableTls,
                    Optional.ofNullable(tlsCertificate));
        }
    }

    /**
     * Retry policy for client operations.
     */
    public static final class RetryPolicy {
        private final int maxRetries;
        private final Duration initialDelay;
        private final Duration maxDelay;
        private final double multiplier;

        private RetryPolicy(int maxRetries, Duration initialDelay, Duration maxDelay, double multiplier) {
            this.maxRetries = maxRetries;
            this.initialDelay = initialDelay;
            this.maxDelay = maxDelay;
            this.multiplier = multiplier;
        }

        /**
         * Creates a retry policy with exponential backoff.
         *
         * @return a RetryPolicy with exponential backoff configuration
         */
        public static RetryPolicy exponentialBackoff() {
            return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0);
        }

        /**
         * Creates a retry policy with exponential backoff and custom parameters.
         *
         * @param maxRetries   the maximum number of retries
         * @param initialDelay the initial delay before the first retry
         * @param maxDelay     the maximum delay between retries
         * @param multiplier   the multiplier for exponential backoff
         * @return a RetryPolicy with custom exponential backoff configuration
         */
        public static RetryPolicy exponentialBackoff(
                int maxRetries, Duration initialDelay, Duration maxDelay, double multiplier) {
            return new RetryPolicy(maxRetries, initialDelay, maxDelay, multiplier);
        }

        /**
         * Creates a retry policy with fixed delay.
         *
         * @param maxRetries the maximum number of retries
         * @param delay      the fixed delay between retries
         * @return a RetryPolicy with fixed delay configuration
         */
        public static RetryPolicy fixedDelay(int maxRetries, Duration delay) {
            return new RetryPolicy(maxRetries, delay, delay, 1.0);
        }

        /**
         * Creates a no-retry policy.
         *
         * @return a RetryPolicy that does not retry
         */
        public static RetryPolicy noRetry() {
            return new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 1.0);
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public double getMultiplier() {
            return multiplier;
        }
    }
}
