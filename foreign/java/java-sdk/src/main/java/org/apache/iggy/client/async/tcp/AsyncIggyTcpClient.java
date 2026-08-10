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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ConnectTimeoutException;
import org.apache.iggy.IggyVersion;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.ConsumerGroupsClient;
import org.apache.iggy.client.async.ConsumerOffsetsClient;
import org.apache.iggy.client.async.MessagesClient;
import org.apache.iggy.client.async.PartitionsClient;
import org.apache.iggy.client.async.PersonalAccessTokensClient;
import org.apache.iggy.client.async.StreamsClient;
import org.apache.iggy.client.async.SystemClient;
import org.apache.iggy.client.async.TopicsClient;
import org.apache.iggy.client.async.UsersClient;
import org.apache.iggy.client.async.tcp.AsyncTcpConnection.TcpConnectionPoolConfig;
import org.apache.iggy.client.async.tcp.LeaderAwareness.LeaderRedirectionState;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

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

    private static final int INVALID_COMMAND_ERROR_CODE = 3;
    private static final Logger log = LoggerFactory.getLogger(AsyncIggyTcpClient.class);
    private static final RetryPolicy DEFAULT_RECONNECT_POLICY = RetryPolicy.fixedDelay(12, Duration.ofSeconds(5));

    private final ConnectionInfo seedConnectionInfo;
    private final AtomicBoolean reconnecting = new AtomicBoolean();
    private final Optional<String> username;
    private final Optional<String> password;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> acquireTimeout;
    private final Optional<Duration> requestTimeout;
    private final Duration heartbeatInterval;
    private final int maxVsrFrameSize;
    private final Optional<RetryPolicy> retryPolicy;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;
    private final TcpConnectionPoolConfig poolConfig;
    private final ClientRoutingState routingState = new ClientRoutingState();
    private final AtomicReference<AsyncTcpConnection> connection = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> loginChain =
            new AtomicReference<>(CompletableFuture.completedFuture(null));
    private volatile ConnectionInfo connectionInfo;
    private volatile boolean closed;
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
        this(
                host,
                port,
                null,
                null,
                null,
                null,
                null,
                Duration.ofSeconds(5),
                VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                null,
                false,
                Optional.empty());
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    AsyncIggyTcpClient(
            String host,
            int port,
            String username,
            String password,
            Duration connectionTimeout,
            Duration acquireTimeout,
            Duration requestTimeout,
            Duration heartbeatInterval,
            int maxVsrFrameSize,
            RetryPolicy retryPolicy,
            boolean enableTls,
            Optional<File> tlsCertificate) {
        this.connectionInfo = new ConnectionInfo(host, port);
        this.seedConnectionInfo = this.connectionInfo;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
        this.connectionTimeout = Optional.ofNullable(connectionTimeout);
        this.acquireTimeout = Optional.ofNullable(acquireTimeout);
        this.requestTimeout = Optional.ofNullable(requestTimeout);
        this.heartbeatInterval = heartbeatInterval;
        this.maxVsrFrameSize = maxVsrFrameSize;
        this.retryPolicy = Optional.ofNullable(retryPolicy);
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;

        var poolConfigBuilder = TcpConnectionPoolConfig.builder();
        this.acquireTimeout.ifPresent(timeout -> poolConfigBuilder.setAcquireTimeoutMillis(timeout.toMillis()));
        this.poolConfig = poolConfigBuilder.build();
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
        closed = false;
        ConnectionInfo target = connectionInfo;
        AsyncTcpConnection newConnection = openConnection(target);
        AsyncTcpConnection previousConnection = connection.getAndSet(newConnection);
        if (previousConnection != null) {
            previousConnection.close();
        }
        routingState.clearAssignments();
        Supplier<AsyncTcpConnection> currentConnection = connection::get;
        return newConnection.connect().thenRun(() -> {
            log.debug("Connected to {} | {}", target.serverAddress(), IggyVersion.getInstance());
            messagesClient = new MessagesTcpClient(currentConnection, routingState);
            consumerGroupsClient = new ConsumerGroupsTcpClient(currentConnection);
            consumerOffsetsClient = new ConsumerOffsetsTcpClient(currentConnection);
            streamsClient = new StreamsTcpClient(currentConnection);
            topicsClient = new TopicsTcpClient(currentConnection);
            usersClient = new UsersTcpClient(currentConnection, this::loginOnLeader);
            systemClient = new SystemTcpClient(currentConnection);
            personalAccessTokensClient = new PersonalAccessTokensTcpClient(currentConnection, this::loginOnLeader);
            partitionsClient = new PartitionsTcpClient(currentConnection);
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
     * {@link IdentityInfo} on success
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
     * Sends a command code and payload and returns the raw response payload.
     *
     * <p>Session-control codes complete the returned future with an invalid-command error.
     *
     * @param code the command code
     * @param payload the command payload
     * @return a future containing the raw response payload
     * @throws IggyNotConnectedException if {@link #connect()} has not been called
     */
    public CompletableFuture<byte[]> sendBinaryRequest(int code, byte[] payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        if (isSessionControlCode(code)) {
            return CompletableFuture.failedFuture(
                    IggyServerException.fromTcpResponse(INVALID_COMMAND_ERROR_CODE, new byte[0]));
        }
        AsyncTcpConnection currentConnection = connection.get();
        if (currentConnection == null) {
            throw new IggyNotConnectedException();
        }

        return currentConnection.send(code, Unpooled.copiedBuffer(payload)).thenApply(response -> {
            try {
                if (response.readableBytes() <= 1) {
                    return new byte[0];
                }
                byte[] responsePayload = new byte[response.readableBytes()];
                response.readBytes(responsePayload);
                return responsePayload;
            } finally {
                response.release();
            }
        });
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
        closed = true;
        AsyncTcpConnection currentConnection = connection.get();
        if (currentConnection != null) {
            return currentConnection.close();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns the server address this client currently targets. Pre-login
     * leader discovery can change it, so it may differ from the address the
     * client was built with.
     *
     * @return the current {@link ConnectionInfo}
     */
    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    private AsyncTcpConnection openConnection(ConnectionInfo target) {
        return new AsyncTcpConnection(
                target.host(),
                target.port(),
                enableTls,
                tlsCertificate,
                poolConfig,
                connectionTimeout,
                requestTimeout,
                heartbeatInterval,
                maxVsrFrameSize,
                this::retryTransientOnLeader,
                routingState::clearAssignments,
                this::onConnectionFailure);
    }

    /**
     * A not-accepted request was never admitted, so it is safe to recheck the
     * leader, restore authentication on a new connection, and retry it within
     * the original request deadline.
     */
    private CompletableFuture<ByteBuf> retryTransientOnLeader(
            AsyncTcpConnection source,
            int commandCode,
            ByteBuf payload,
            long requestDeadlineNanos,
            IggyServerException rejection) {
        AtomicReference<ByteBuf> requestPayload = new AtomicReference<>(payload);
        Optional<AsyncTcpConnection.AuthenticationSnapshot> authentication = source.authenticationSnapshot();
        AtomicReference<ByteBuf> authenticationPayload = new AtomicReference<>(authentication
                .map(AsyncTcpConnection.AuthenticationSnapshot::payload)
                .orElse(null));

        CompletableFuture<Void> ready = prepareTransientFailover(
                source, authentication, authenticationPayload, requestDeadlineNanos, rejection);
        CompletableFuture<ByteBuf> retried = ready.thenCompose(ignored -> {
            if (requestDeadlineNanos - System.nanoTime() <= 0) {
                return CompletableFuture.failedFuture(rejection);
            }
            AsyncTcpConnection currentConnection = connection.get();
            if (currentConnection == null) {
                return CompletableFuture.failedFuture(new IggyNotConnectedException());
            }
            return currentConnection.send(commandCode, takePayload(requestPayload), requestDeadlineNanos);
        });
        return retried.whenComplete((response, error) -> {
            releasePayload(requestPayload);
            releasePayload(authenticationPayload);
        });
    }

    private CompletableFuture<Void> prepareTransientFailover(
            AsyncTcpConnection source,
            Optional<AsyncTcpConnection.AuthenticationSnapshot> authentication,
            AtomicReference<ByteBuf> authenticationPayload,
            long requestDeadlineNanos,
            IggyServerException rejection) {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        CompletableFuture<Void> previous = loginChain.getAndSet(gate);
        LeaderRedirectionState redirectionState = new LeaderRedirectionState();
        CompletableFuture<Void> transaction = previous.thenCompose(ignored -> {
            if (closed) {
                return CompletableFuture.failedFuture(new IggyNotConnectedException());
            }
            if (requestDeadlineNanos - System.nanoTime() <= 0) {
                return CompletableFuture.failedFuture(rejection);
            }
            if (connection.get() != source) {
                return CompletableFuture.completedFuture(null);
            }
            return redirectToLeader(redirectionState).thenCompose(redirected -> {
                AsyncTcpConnection currentConnection = connection.get();
                if (currentConnection == null || currentConnection == source || authentication.isEmpty()) {
                    return CompletableFuture.completedFuture(null);
                }
                if (requestDeadlineNanos - System.nanoTime() <= 0) {
                    return CompletableFuture.failedFuture(rejection);
                }
                return currentConnection
                        .send(
                                authentication.orElseThrow().commandCode(),
                                takePayload(authenticationPayload),
                                requestDeadlineNanos)
                        .thenAccept(ByteBuf::release);
            });
        });
        transaction.whenComplete((ignored, error) -> gate.complete(null));
        return transaction;
    }

    private static ByteBuf takePayload(AtomicReference<ByteBuf> payload) {
        ByteBuf owned = payload.getAndSet(null);
        if (owned == null) {
            throw new IllegalStateException("Request payload ownership was already transferred");
        }
        return owned;
    }

    private static void releasePayload(AtomicReference<ByteBuf> payload) {
        ByteBuf owned = payload.getAndSet(null);
        if (owned != null) {
            owned.release();
        }
    }

    /**
     * Entry point of the background redial after a pool acquire failure or an
     * expired reply. Requests that were in flight stay failed (their outcome
     * is unknown); the redial only restores the client for subsequent calls.
     * Alternates the current endpoint with the seed, paced by the configured
     * retry policy, and replays the builder credentials on the restored
     * connection. Personal-access-token logins cannot be replayed here; those
     * clients must log in again themselves.
     */
    private void onConnectionFailure(Throwable cause) {
        if (closed || !isConnectionLoss(cause)) {
            return;
        }
        if (!reconnecting.compareAndSet(false, true)) {
            return;
        }
        log.warn("Connection to {} lost ({}), starting redial", connectionInfo.serverAddress(), cause.getMessage());
        RetryPolicy policy = retryPolicy.orElse(DEFAULT_RECONNECT_POLICY);
        redialAttempt(1, policy).whenComplete((ignored, error) -> reconnecting.set(false));
    }

    private static boolean isConnectionLoss(Throwable cause) {
        return cause instanceof ConnectTimeoutException
                || cause instanceof IOException
                || cause instanceof IggyTimeoutException;
    }

    private CompletableFuture<Void> redialAttempt(int attempt, RetryPolicy policy) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        if (attempt > policy.getMaxRetries()) {
            log.error("Redial gave up after {} attempts, next request will fail fast", policy.getMaxRetries());
            return CompletableFuture.completedFuture(null);
        }
        ConnectionInfo target = ReconnectPlan.target(connectionInfo, seedConnectionInfo, attempt);
        Duration delay = ReconnectPlan.delay(policy, attempt);
        Executor delayedExecutor = CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS);
        return CompletableFuture.supplyAsync(() -> null, delayedExecutor).thenCompose(ignored -> {
            if (closed) {
                return CompletableFuture.completedFuture(null);
            }
            log.info("Redial attempt {}/{} to {}", attempt, policy.getMaxRetries(), target.serverAddress());
            return retarget(target)
                    .thenCompose(retargeted -> replayLogin())
                    .handle((ok, error) -> {
                        if (error == null) {
                            log.info("Reconnected to {}", target.serverAddress());
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        log.warn(
                                "Redial attempt {} to {} failed: {}",
                                attempt,
                                target.serverAddress(),
                                error.getMessage());
                        return redialAttempt(attempt + 1, policy);
                    })
                    .thenCompose(Function.identity());
        });
    }

    /**
     * Replays the builder credentials on the freshly published connection.
     * The login runs through the users client, so leader discovery retargets
     * again before Register when the redialed node is not the leader.
     */
    private CompletableFuture<Void> replayLogin() {
        if (username.isEmpty() || password.isEmpty() || usersClient == null) {
            return CompletableFuture.completedFuture(null);
        }
        return usersClient.login(username.get(), password.get()).thenApply(identity -> null);
    }

    /**
     * Serializes pre-login leader discovery and Register across concurrent
     * logins. A queued login waits for the entire in-flight transaction, then
     * checks the roster from the connection that transaction published. Each
     * transaction has a fresh redirection budget, so hitting the cap affects
     * only that login. Metadata and retargeting failures retain best-effort
     * behavior and let Register run against the current target.
     */
    CompletableFuture<IdentityInfo> loginOnLeader(Supplier<CompletableFuture<IdentityInfo>> loginAttempt) {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        CompletableFuture<Void> previous = loginChain.getAndSet(gate);
        LeaderRedirectionState redirectionState = new LeaderRedirectionState();
        CompletableFuture<IdentityInfo> transaction = previous.thenCompose(
                        ignored -> redirectToLeader(redirectionState))
                .thenCompose(ignored -> loginAttempt.get());
        CompletableFuture<IdentityInfo> callerFuture = new CompletableFuture<>();
        transaction.whenComplete((identity, error) -> {
            gate.complete(null);
            if (error != null) {
                callerFuture.completeExceptionally(error);
            } else {
                callerFuture.complete(identity);
            }
        });
        return callerFuture;
    }

    /**
     * One authentication-independent discovery hop. When the roster names a
     * healthy leader elsewhere, reconnect to it and re-check from the new
     * node, since mid-election metadata can point at a node that is itself not
     * the leader. Register is sent only after this bounded process settles.
     */
    private CompletableFuture<Void> redirectToLeader(LeaderRedirectionState redirectionState) {
        ConnectionInfo currentTarget = connectionInfo;
        return findLeaderElsewhere(currentTarget).thenCompose(leaderTarget -> {
            if (leaderTarget.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            if (!redirectionState.canRedirect()) {
                log.warn(
                        "Maximum leader redirections ({}) reached, connection will continue on server node {}",
                        LeaderAwareness.MAX_LEADER_REDIRECTS,
                        currentTarget.serverAddress());
                return CompletableFuture.completedFuture(null);
            }
            return retarget(leaderTarget.get())
                    .handle((ignored, error) -> {
                        if (error != null) {
                            log.warn(
                                    "Failed to reconnect to leader at {}: {}, connection will continue"
                                            + " on server node {}",
                                    leaderTarget.get().serverAddress(),
                                    error.getMessage(),
                                    currentTarget.serverAddress());
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        redirectionState.recordRedirect();
                        return redirectToLeader(redirectionState);
                    })
                    .thenCompose(Function.identity());
        });
    }

    /**
     * Picks a healthy leader living elsewhere from the roster, waiting out a
     * transiently leaderless election. Resolves to empty on any failure
     * (metadata fetch, malformed roster) so the redirection path never fails
     * the login that triggered it.
     */
    CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget) {
        SystemClient currentSystemClient = systemClient;
        if (currentSystemClient == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return LeaderAwareness.findLeaderElsewhere(currentSystemClient::getClusterMetadata, currentTarget);
    }

    CompletableFuture<Void> retarget(ConnectionInfo newTarget) {
        AsyncTcpConnection oldConnection = connection.get();
        AsyncTcpConnection newConnection;
        try {
            newConnection = openConnection(newTarget);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        AsyncTcpConnection openedConnection = newConnection;
        return newConnection
                .connect()
                .thenCompose(ignored -> publishConnection(oldConnection, openedConnection, newTarget))
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        openedConnection.close();
                    }
                });
    }

    private CompletableFuture<Void> publishConnection(
            AsyncTcpConnection oldConnection, AsyncTcpConnection newConnection, ConnectionInfo newTarget) {
        if (!connection.compareAndSet(oldConnection, newConnection)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Connection replaced concurrently during leader redirection"));
        }
        if (closed) {
            oldConnection.close();
            return CompletableFuture.failedFuture(
                    new IggyNotConnectedException("Client closed during leader redirection"));
        }
        connectionInfo = newTarget;
        routingState.clearAssignments();
        oldConnection.close().whenComplete((ignored, closeError) -> {
            if (closeError != null) {
                log.warn("Failed to close previous connection: {}", closeError.getMessage());
            }
        });
        return CompletableFuture.completedFuture(null);
    }

    private static boolean isSessionControlCode(int code) {
        return code == CommandCode.User.LOGIN.getValue()
                || code == CommandCode.User.LOGOUT.getValue()
                || code == CommandCode.User.LOGIN_REGISTER.getValue()
                || code == CommandCode.PersonalAccessToken.LOGIN.getValue()
                || code == CommandCode.PersonalAccessToken.LOGIN_REGISTER.getValue();
    }
}
