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
 * Async TCP client for Apache Iggy message streaming, built on Netty.
 *
 * <p>This client provides fully non-blocking I/O for communicating with an Iggy server
 * over TCP using the binary protocol. All operations return {@link CompletableFuture}
 * instances, enabling efficient concurrent and reactive programming patterns.
 *
 * <h2>Lifecycle</h2>
 * <p>The client follows a three-phase lifecycle:
 * <ol>
 *   <li><strong>Build</strong> — configure the client via {@link #builder()} or
 *       {@link org.apache.iggy.Iggy#tcpClientBuilder()}</li>
 *   <li><strong>Connect</strong> — establish the TCP connection with {@link #connect()}</li>
 *   <li><strong>Login</strong> — authenticate with {@link #login()} or
 *       {@link UsersClient#login(String, String)}</li>
 * </ol>
 *
 * <h2>Quick Start</h2>
 * <pre>{@code
 * // One-liner: build, connect, and login
 * var client = AsyncIggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin()
 *     .join();
 *
 * // Send a message
 * client.messages().sendMessages(
 *         StreamId.of(1L), TopicId.of(1L),
 *         Partitioning.balanced(),
 *         List.of(Message.of("hello world")))
 *     .join();
 *
 * // Always close when done
 * client.close().join();
 * }</pre>
 *
 * <h2>Thread Safety</h2>
 * <p>This client is thread-safe. Multiple threads can invoke operations concurrently;
 * the underlying Netty event loop serializes writes to the TCP connection while
 * response handling is performed asynchronously.
 *
 * <h2>Resource Management</h2>
 * <p>Always call {@link #close()} when the client is no longer needed. This shuts down
 * the Netty event loop group and releases all associated resources.
 *
 * @see AsyncIggyTcpClientBuilder
 * @see org.apache.iggy.Iggy#tcpClientBuilder()
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

    /**
     * Creates a new async TCP client with default settings.
     *
     * <p>Prefer using {@link #builder()} for configuring the client.
     *
     * @param host the server hostname
     * @param port the server port
     */
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
     * Creates a new builder for configuring an {@code AsyncIggyTcpClient}.
     *
     * @return a new {@link AsyncIggyTcpClientBuilder} instance
     */
    public static AsyncIggyTcpClientBuilder builder() {
        return new AsyncIggyTcpClientBuilder();
    }

    /**
     * Connects to the Iggy server asynchronously.
     *
     * <p>This establishes the TCP connection using Netty's non-blocking I/O. After the
     * returned future completes, the sub-clients ({@link #messages()}, {@link #streams()},
     * etc.) become available. You must call this before performing any operations.
     *
     * @return a {@link CompletableFuture} that completes when the connection is established
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
     * <p>Credentials must have been set via
     * {@link AsyncIggyTcpClientBuilder#credentials(String, String)} when building
     * the client. For explicit credential handling, use
     * {@link UsersClient#login(String, String)} instead.
     *
     * @return a {@link CompletableFuture} that completes with the user's
     *         {@link IdentityInfo} on success
     * @throws IggyMissingCredentialsException if no credentials were provided at build time
     * @throws IggyNotConnectedException       if {@link #connect()} has not been called
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
     * Returns the async users client for authentication operations.
     *
     * @return the {@link UsersClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public UsersClient users() {
        if (usersClient == null) {
            throw new IggyNotConnectedException();
        }
        return usersClient;
    }

    /**
     * Returns the async messages client for producing and consuming messages.
     *
     * @return the {@link MessagesClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public MessagesClient messages() {
        if (messagesClient == null) {
            throw new IggyNotConnectedException();
        }
        return messagesClient;
    }

    /**
     * Returns the async consumer groups client for group membership management.
     *
     * @return the {@link ConsumerGroupsClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public ConsumerGroupsClient consumerGroups() {
        if (consumerGroupsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerGroupsClient;
    }

    /**
     * Returns the async streams client for stream management.
     *
     * @return the {@link StreamsClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public StreamsClient streams() {
        if (streamsClient == null) {
            throw new IggyNotConnectedException();
        }
        return streamsClient;
    }

    /**
     * Returns the async topics client for topic management.
     *
     * @return the {@link TopicsClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public TopicsClient topics() {
        if (topicsClient == null) {
            throw new IggyNotConnectedException();
        }
        return topicsClient;
    }

    /**
     * Returns the async system client for server system operations.
     *
     * @return the {@link SystemClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public SystemClient system() {
        if (systemClient == null) {
            throw new IggyNotConnectedException();
        }
        return systemClient;
    }

    /**
     * Returns the async personal access tokens client for token management.
     *
     * @return the {@link PersonalAccessTokensClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public PersonalAccessTokensClient personalAccessTokens() {
        if (personalAccessTokensClient == null) {
            throw new IggyNotConnectedException();
        }
        return personalAccessTokensClient;
    }

    /**
     * Returns the async partitions client for partition management.
     *
     * @return the {@link PartitionsClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public PartitionsClient partitions() {
        if (partitionsClient == null) {
            throw new IggyNotConnectedException();
        }
        return partitionsClient;
    }

    /**
     * Returns the async consumer offsets client for offset management.
     *
     * @return the {@link ConsumerOffsetsClient} instance
     * @throws IggyNotConnectedException if the client is not connected
     */
    public ConsumerOffsetsClient consumerOffsets() {
        if (consumerOffsetsClient == null) {
            throw new IggyNotConnectedException();
        }
        return consumerOffsetsClient;
    }

    /**
     * Closes the TCP connection and releases all Netty resources.
     *
     * <p>This shuts down the event loop group gracefully. After calling this method,
     * the client cannot be reused — create a new instance if needed.
     *
     * @return a {@link CompletableFuture} that completes when all resources are released
     */
    public CompletableFuture<Void> close() {
        if (connection != null) {
            return connection.close();
        }
        return CompletableFuture.completedFuture(null);
    }
}
