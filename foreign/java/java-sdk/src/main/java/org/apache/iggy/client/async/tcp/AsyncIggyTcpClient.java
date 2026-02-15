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

import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.ConsumerOffsetsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.PartitionsClient;
import org.apache.iggy.client.async.PersonalAccessTokensClient;
import org.apache.iggy.client.async.StreamsClient;
import org.apache.iggy.client.async.SystemClient;
import org.apache.iggy.client.async.TopicsClient;
import org.apache.iggy.client.async.UsersClient;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.user.IdentityInfo;

import java.io.File;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Async TCP client for Apache Iggy using Netty.
 * This is a true async implementation with non-blocking I/O.
 */
public class AsyncIggyTcpClient {

    private final String host;
    private final int port;
    private final Optional<String> username;
    private final Optional<String> password;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> requestTimeout;
    private final Optional<Integer> connectionPoolSize;
    private final Optional<RetryPolicy> retryPolicy;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;
    private AsyncTcpConnection connection;
    private MessagesClient messagesClient;
    private ConsumerGroupsClient consumerGroupsClient;
    private ConsumerOffsetsClient consumerOffsetsClient;
    private StreamsClient streamsClient;
    private TopicsClient topicsClient;
    private UsersClient usersClient;
    private SystemClient systemClient;
    private PersonalAccessTokensClient personalAccessTokensClient;
    private PartitionsClient partitionsClient;

    public AsyncIggyTcpClient(String host, int port) {
        this(host, port, null, null, null, null, null, null, false, Optional.empty());
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    AsyncIggyTcpClient(
            String host,
            int port,
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
    }

    /**
     * Creates a new builder for configuring AsyncIggyTcpClient.
     *
     * @return a new Builder instance
     */
    public static AsyncIggyTcpClientBuilder builder() {
        return new AsyncIggyTcpClientBuilder();
    }

    /**
     * Connects to the Iggy server asynchronously.
     *
     * @return a CompletableFuture that completes when connected
     */
    public CompletableFuture<Void> connect() {
        connection = new AsyncTcpConnection(host, port, enableTls, tlsCertificate);
        return connection.connect().thenRun(() -> {
            messagesClient = new MessagesTcpClient(connection);
            consumerGroupsClient = new ConsumerGroupsTcpClient(connection);
            consumerOffsetsClient = new ConsumerOffsetsTcpClient(connection);
            streamsClient = new StreamsTcpClient(connection);
            topicsClient = new TopicsTcpClient(connection);
            usersClient = new UsersTcpClient(connection);
            systemClient = new SystemTcpClient(connection);
            personalAccessTokensClient = new PersonalAccessTokensTcpClient(connection);
            partitionsClient = new PartitionsTcpClient(connection);
        });
    }

    /**
     * Logs in using the credentials provided during client construction.
     *
     * @return a CompletableFuture that completes when logged in
     * @throws IggyMissingCredentialsException if no credentials were provided
     * @throws IggyNotConnectedException if client is not connected
     */
    public CompletableFuture<IdentityInfo> login() {
        if (usersClient == null) {
            throw new IggyNotConnectedException();
        }
        if (username.isEmpty() || password.isEmpty()) {
            throw new IggyMissingCredentialsException();
        }
        return usersClient.login(username.get(), password.get());
    }

    /**
     * Gets the async users client.
     */
    public UsersClient users() {
        if (usersClient == null) {
            throw new IggyNotConnectedException();
        }
        return usersClient;
    }

    /**
     * Gets the async messages client.
     */
    public MessagesClient messages() {
        if (messagesClient == null) {
            throw new IggyNotConnectedException();
        }
        return messagesClient;
    }

    /**
     * Gets the async consumer groups client.
     */
    public ConsumerGroupsClient consumerGroups() {
        if (consumerGroupsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerGroupsClient;
    }

    /**
     * Gets the async streams client.
     */
    public StreamsClient streams() {
        if (streamsClient == null) {
            throw new IggyNotConnectedException();
        }
        return streamsClient;
    }

    /**
     * Gets the async topics client.
     */
    public TopicsClient topics() {
        if (topicsClient == null) {
            throw new IggyNotConnectedException();
        }
        return topicsClient;
    }

    /**
     * Gets the async system client.
     */
    public SystemClient system() {
        if (systemClient == null) {
            throw new IggyNotConnectedException();
        }
        return systemClient;
    }

    /**
     * Gets the async personal access tokens client.
     */
    public PersonalAccessTokensClient personalAccessTokens() {
        if (personalAccessTokensClient == null) {
            throw new IggyNotConnectedException();
        }
        return personalAccessTokensClient;
    }

    /**
     * Gets the async partitions client.
     */
    public PartitionsClient partitions() {
        if (partitionsClient == null) {
            throw new IggyNotConnectedException();
        }
        return partitionsClient;
    }

    /**
     * Gets the async consumer offsets client.
     */
    public ConsumerOffsetsClient consumerOffsets() {
        if (consumerOffsetsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerOffsetsClient;
    }

    /**
     * Closes the connection and releases resources.
     */
    public CompletableFuture<Void> close() {
        if (connection != null) {
            return connection.close();
        }
        return CompletableFuture.completedFuture(null);
    }
}
