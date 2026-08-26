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
import org.apache.iggy.exception.IggyErrorCode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
    private final LoginRoutingHook loginRoutingHook = new LoginRoutingHook() {

        @Override
        public CompletableFuture<IdentityInfo> loginOnLeader(Supplier<CompletableFuture<IdentityInfo>> loginAttempt) {
            return AsyncIggyTcpClient.this.loginOnLeader(loginAttempt);
        }

        @Override
        public void forgetLogin() {
            rememberedLogin = null;
        }
    };
    private final AtomicReference<AsyncTcpConnection> connection = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> loginChain =
            new AtomicReference<>(CompletableFuture.completedFuture(null));
    private volatile ConnectionInfo connectionInfo;
    /**
     * Every node the roster named on the last leader check, kept as redial
     * candidates. A node dies together with its address, and the roster is
     * unreachable exactly when it is needed, so it has to have been
     * remembered while the connection was still healthy.
     */
    private volatile List<ConnectionInfo> rosterTargets = List.of();
    /**
     * The login a successful sign-in ran, replayed after a redial so the
     * session is re-established on whichever node answers. The supplier
     * already carries the credentials it signed in with, so nothing new is
     * stored. Cleared on an explicit sign-out, which leaves no session to
     * restore.
     */
    private volatile Supplier<CompletableFuture<IdentityInfo>> rememberedLogin;

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
            usersClient = new UsersTcpClient(currentConnection, loginRoutingHook);
            systemClient = new SystemTcpClient(currentConnection);
            personalAccessTokensClient = new PersonalAccessTokensTcpClient(currentConnection, loginRoutingHook);
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
        // Closing is caller intent, like a logout: connect() clears `closed`
        // again, and a session the caller ended must not come back with the
        // credentials the earlier sign-in used.
        rememberedLogin = null;
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
                this::onSessionReset,
                this::onConnectionFailure);
    }

    /**
     * A server-side eviction reached this client. The routing state it cached
     * belonged to the evicted session, so it goes.
     *
     * The sign-in stays. A stale-client eviction is not caller intent: the
     * server's heartbeat verifier sends it after a gc pause or a laptop sleep,
     * and a client that signed in by hand has to recover from it exactly like
     * one whose credentials were configured. The connection re-authenticates
     * the replacement channel from the login it captured, which is the same
     * sign-in a redial would replay. Same rule in every SDK; only an explicit
     * sign-out or close ends a session for good.
     */
    private void onSessionReset(int errorCode) {
        routingState.clearAssignments();
        if (errorCode == IggyErrorCode.STALE_CLIENT.getCode()) {
            log.debug("The server evicted this session as stale; the next request re-establishes it");
        }
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
     * Each rotation sweeps every endpoint the client knows -- where it was,
     * the configured seed, then the roster it learned -- and only a rotation
     * that reaches none of them waits out the retry policy's delay. The
     * sign-in is replayed on whichever endpoint answers, whether it was
     * configured on the builder or run by the caller, personal access tokens
     * included.
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
        List<ConnectionInfo> candidates = redialCandidates();
        // The retry budget bounds the rotations, not the endpoints. A policy of
        // zero retries with several endpoints known still gets one rotation:
        // those endpoints - the address the client was configured with, the
        // nodes the roster named - were made known in order to be tried, and
        // the other SDKs sweep them once too. With one endpoint known, zero
        // retries redials nothing, which is what it asked for.
        boolean sweepOnce = attempt == 1 && candidates.size() > 1;
        if (attempt > policy.getMaxRetries() && !sweepOnce) {
            log.error("Redial gave up after {} attempts, next request will fail fast", policy.getMaxRetries());
            return CompletableFuture.completedFuture(null);
        }
        // The delay paces rotations, not dials. The first rotation runs at once
        // when there is somewhere else to go: pausing before dialing a survivor
        // only pushes the failover past the window the caller waits in, and the
        // node just lost may be gone for good.
        Duration delay = attempt == 1 && candidates.size() > 1 ? Duration.ZERO : ReconnectPlan.delay(policy, attempt);
        Executor delayedExecutor = CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS);
        return CompletableFuture.supplyAsync(() -> null, delayedExecutor)
                .thenCompose(ignored -> sweepCandidates(candidates, 0, attempt, policy));
    }

    /**
     * Dials one endpoint of a rotation and, if it does not come up, the next
     * one. Every endpoint gets its turn inside one attempt, so a full pass over
     * the cluster costs one retry rather than one per endpoint: with the
     * default policy, rotating one endpoint per attempt would first dial a
     * two-node survivor two delays in.
     */
    private CompletableFuture<Void> sweepCandidates(
            List<ConnectionInfo> candidates, int index, int attempt, RetryPolicy policy) {
        if (closed) {
            return CompletableFuture.completedFuture(null);
        }
        if (index >= candidates.size()) {
            return redialAttempt(attempt + 1, policy);
        }
        ConnectionInfo target = candidates.get(index);
        log.info(
                "Redial attempt {}/{} to {} ({}/{})",
                attempt,
                policy.getMaxRetries(),
                target.serverAddress(),
                index + 1,
                candidates.size());
        return retarget(target)
                .handle((retargeted, dialError) -> {
                    if (dialError != null) {
                        log.warn("Redial to {} failed: {}", target.serverAddress(), dialError.getMessage());
                        return sweepCandidates(candidates, index + 1, attempt, policy);
                    }
                    return replaySignInOn(target, candidates, index, attempt, policy);
                })
                .thenCompose(Function.identity());
    }

    /**
     * Re-establishes the session on an endpoint that just came up.
     *
     * A sign-in the server rejected -- a rotated password, an expired token --
     * ends the redial: the connection is up, no other endpoint would answer
     * differently, and retrying would tear the working connection down on the
     * next rotation and leave the client connected but unauthenticated anyway.
     * The rejected credentials are dropped so nothing replays them.
     */
    private CompletableFuture<Void> replaySignInOn(
            ConnectionInfo target, List<ConnectionInfo> candidates, int index, int attempt, RetryPolicy policy) {
        return replayLogin()
                .handle((ok, loginError) -> {
                    if (loginError == null) {
                        log.info("Reconnected to {}", target.serverAddress());
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    if (!isSignInRejection(unwrap(loginError))) {
                        log.warn(
                                "The sign-in on {} did not complete: {}",
                                target.serverAddress(),
                                loginError.getMessage());
                        return sweepCandidates(candidates, index + 1, attempt, policy);
                    }
                    log.error(
                            "Reconnected to {} but the sign-in was rejected: {}. The connection stands"
                                    + " unauthenticated until the caller signs in again.",
                            target.serverAddress(),
                            loginError.getMessage());
                    rememberedLogin = null;
                    return CompletableFuture.<Void>completedFuture(null);
                })
                .thenCompose(Function.identity());
    }

    /**
     * Whether the server answered the sign-in with a verdict no other endpoint
     * would change: a rotated password, an expired token.
     *
     * Only that ends a redial. Everything else -- the channel closing before
     * the reply, a timeout, a transient refusal from a node that is not the
     * primary -- says nothing about the credentials, and treating it as a
     * rejection would drop them and leave the client published on a node that
     * is already gone, with every later call failing "not authenticated".
     */
    static boolean isSignInRejection(Throwable error) {
        if (!(error instanceof IggyServerException serverError)) {
            return false;
        }
        int code = serverError.getRawErrorCode();
        return code != AsyncTcpConnection.TRANSIENT_NOT_ACCEPTED && code != AsyncTcpConnection.TRANSIENT_NOT_COMMITTED;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * Re-establishes the session on the freshly published connection with the
     * sign-in that last succeeded, falling back to the credentials configured
     * on the builder when no login has run yet.
     *
     * The last sign-in rather than the configured one, which is where this
     * differs from the Rust and Go SDKs: the connection re-authenticates a
     * replacement channel from the login payload it captured, which is that
     * same last sign-in. Replaying a different user here would make the same
     * eviction land on a different session depending on whether the channel
     * or the redial got there first. A client that only ever used its
     * configured credentials remembers exactly those, so nothing changes for
     * it.
     *
     * The login runs through the users client, so leader discovery retargets
     * again before Register when the redialed node is not the leader.
     */
    private CompletableFuture<Void> replayLogin() {
        Supplier<CompletableFuture<IdentityInfo>> replay = rememberedLogin;
        if (replay != null) {
            // Runs through loginOnLeader, so a redial that landed on a backup
            // still settles on the leader before the session is used.
            return loginOnLeader(replay).thenApply(identity -> null);
        }
        if (username.isPresent() && password.isPresent() && usersClient != null) {
            return usersClient.login(username.get(), password.get()).thenApply(identity -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Serializes Register and authenticated leader settlement across concurrent
     * logins. A queued login waits for the entire in-flight transaction. Each
     * redirect drops the bound session, so the login is replayed before the next
     * roster read. Each transaction has a fresh redirection budget.
     */
    CompletableFuture<IdentityInfo> loginOnLeader(Supplier<CompletableFuture<IdentityInfo>> loginAttempt) {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        CompletableFuture<Void> previous = loginChain.getAndSet(gate);
        LeaderRedirectionState redirectionState = new LeaderRedirectionState();
        CompletableFuture<IdentityInfo> transaction =
                previous.thenCompose(ignored -> loginAndSettleOnLeader(loginAttempt, redirectionState));
        CompletableFuture<IdentityInfo> callerFuture = new CompletableFuture<>();
        transaction.whenComplete((identity, error) -> {
            gate.complete(null);
            // Not after a close: a login still in flight when `close()` cleared
            // this would set it again, and `connect()` clears `closed`, so the
            // next loss would replay a sign-in the caller had ended.
            if (error == null && !closed) {
                rememberedLogin = loginAttempt;
            }
            if (error != null) {
                callerFuture.completeExceptionally(error);
            } else {
                callerFuture.complete(identity);
            }
        });
        return callerFuture;
    }

    private CompletableFuture<IdentityInfo> loginAndSettleOnLeader(
            Supplier<CompletableFuture<IdentityInfo>> loginAttempt, LeaderRedirectionState redirectionState) {
        return loginAttempt.get().thenCompose(identity -> {
            ConnectionInfo currentTarget = connectionInfo;
            return findLeaderElsewhere(currentTarget).thenCompose(leaderTarget -> {
                if (leaderTarget.isEmpty()) {
                    return CompletableFuture.completedFuture(identity);
                }
                if (!redirectionState.canRedirect()) {
                    log.warn(
                            "Maximum leader redirections ({}) reached, connection will continue on server node {}",
                            LeaderAwareness.MAX_LEADER_REDIRECTS,
                            currentTarget.serverAddress());
                    return CompletableFuture.completedFuture(identity);
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
                                return CompletableFuture.completedFuture(identity);
                            }
                            redirectionState.recordRedirect();
                            return loginAndSettleOnLeader(loginAttempt, redirectionState);
                        })
                        .thenCompose(Function.identity());
            });
        });
    }

    /**
     * One discovery hop for transient failover. When the roster names a healthy leader elsewhere,
     * reconnect to it and re-check from the new node, since mid-election
     * metadata can point at a node that is itself not the leader. Register is
     * sent only after this bounded process settles. Reading the roster needs a
     * bound session, so a connection that has none fails the fetch locally and
     * stays where it is. Login settlement uses {@link #loginAndSettleOnLeader}
     * so every redirected connection binds before its next roster read.
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
        return LeaderAwareness.findLeaderElsewhere(currentSystemClient::getClusterMetadata, currentTarget)
                .thenApply(lookup -> {
                    rememberRoster(lookup);
                    return lookup.redirect();
                });
    }

    /**
     * Keeps what a leader check learned about where the cluster's nodes are.
     *
     * Replaced wholesale rather than merged: the roster is the cluster's own
     * answer, so a node it dropped stops being dialed. The configured seed is
     * kept separately and outlives it.
     *
     * An inconclusive check -- an unreadable roster, a metadata read that
     * failed -- names no endpoint, and that must leave the last roster
     * standing: assigning it anyway would empty the redial candidates exactly
     * when the cluster is unreachable, which is when they are needed.
     */
    void rememberRoster(LeaderAwareness.LeaderLookup lookup) {
        if (!lookup.endpoints().isEmpty()) {
            rosterTargets = lookup.endpoints();
        }
    }

    /** The roster this client would redial, for tests in this package. */
    List<ConnectionInfo> rosterTargets() {
        return rosterTargets;
    }

    /** Whether a sign-in is remembered for replay, for tests in this package. */
    boolean hasRememberedLogin() {
        return rememberedLogin != null;
    }

    /** The live connection, for tests in this package. */
    AsyncTcpConnection currentConnection() {
        return connection.get();
    }

    /**
     * Endpoints a redial rotates through, likeliest first: where the client
     * currently is, the address it was configured with, then the roster it
     * learned while connected. Duplicates are dropped by spelling, so an
     * endpoint the roster merely writes differently does not earn a second
     * attempt. Spelling only: this runs on the Netty event loop, where a
     * resolver lookup per candidate pair would block it.
     */
    private List<ConnectionInfo> redialCandidates() {
        List<ConnectionInfo> candidates = new ArrayList<>();
        candidates.add(connectionInfo);
        Stream.concat(Stream.of(seedConnectionInfo), rosterTargets.stream())
                .filter(endpoint ->
                        candidates.stream().noneMatch(candidate -> LeaderAwareness.isSameSpelling(candidate, endpoint)))
                .forEach(candidates::add);
        return List.copyOf(candidates);
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
