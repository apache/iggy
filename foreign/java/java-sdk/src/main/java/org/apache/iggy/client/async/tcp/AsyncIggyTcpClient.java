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
import io.netty.channel.IoEventLoopGroup;
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
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.config.RetryPolicy;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMissingCredentialsException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.IdentityInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongFunction;
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
 * <h2>Callbacks</h2>
 * <p>The futures of {@link MessagesClient}, {@link ConsumerOffsetsClient},
 * {@link #sendBinaryRequest(int, byte[])} and {@link #resolveOperation(long, Duration)}
 * complete in a task of the client event loop. No lock of the client is held there, so
 * their callbacks can call any method of the client, such as {@link #login()} or
 * {@link #close()}. A callback runs on the event loop, so it must not block.
 * <p>A cancel can win over an outcome that has not reached the event loop yet. So a
 * cancelled send or offset store can still have committed. After the event loop shuts
 * down, these futures complete in the thread that ends the call. That thread can hold a
 * lock of the client, so the rule above no longer holds. A future whose outcome arrives
 * during the final shutdown can also stay incomplete, because Netty can drop a task that
 * it accepts then.
 *
 * <h2>Resource Management</h2>
 * <p>When the client is no longer needed, call {@link #close()}. This closes the
 * connection and shuts down the event loop group that the client created for itself. A
 * group supplied through {@link AsyncIggyTcpClientBuilder#eventLoopGroup(IoEventLoopGroup)}
 * stays up. The caller owns that group. After every client on the group is closed, the
 * caller shuts the group down. Do not block in a completion callback. Callbacks run on
 * that group's loops, so a blocked callback stalls every client that shares the group.
 *
 * @see AsyncIggyTcpClientBuilder
 * @see org.apache.iggy.Iggy#tcpClientBuilder()
 */
public class AsyncIggyTcpClient {

    /**
     * Bound on one dial while other endpoints are queued behind it, matching
     * the Rust, Go, C# and Node SDKs. Netty's own connect timeout would
     * otherwise let a node whose syns are dropped hold the whole rotation.
     *
     * Package-private: the tests pin the bound against the other SDKs'.
     */
    static final Duration FAILOVER_DIAL_TIMEOUT = Duration.ofSeconds(2);

    private static final int INVALID_COMMAND_ERROR_CODE = 3;
    private static final Logger log = LoggerFactory.getLogger(AsyncIggyTcpClient.class);
    private static final RetryPolicy DEFAULT_RECONNECT_POLICY = RetryPolicy.fixedDelay(12, Duration.ofSeconds(5));
    private static final Set<Integer> SIGN_IN_REJECTIONS = Set.of(
            IggyErrorCode.INVALID_FORMAT.getCode(),
            IggyErrorCode.UNAUTHENTICATED.getCode(),
            IggyErrorCode.INVALID_CREDENTIALS.getCode(),
            IggyErrorCode.INVALID_USERNAME.getCode(),
            IggyErrorCode.INVALID_PASSWORD.getCode(),
            IggyErrorCode.USER_INACTIVE.getCode(),
            IggyErrorCode.INVALID_PERSONAL_ACCESS_TOKEN.getCode(),
            IggyErrorCode.PERSONAL_ACCESS_TOKEN_EXPIRED.getCode());

    private final ConnectionInfo seedConnectionInfo;
    private final AtomicReference<Object> reconnecting = new AtomicReference<>();
    private volatile Optional<String> username;
    private volatile Optional<String> password;
    private final Optional<Duration> connectionTimeout;
    private final Optional<Duration> acquireTimeout;
    private final Optional<Duration> requestTimeout;
    private final Duration heartbeatInterval;
    private final int maxVsrFrameSize;
    private final Optional<RetryPolicy> retryPolicy;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;
    private final Optional<IoEventLoopGroup> sharedEventLoopGroup;
    private IoEventLoopGroup eventLoopGroup;
    private final org.apache.iggy.client.async.tcp.vsr.ConsensusSession session =
            new org.apache.iggy.client.async.tcp.vsr.ConsensusSession();
    private final int ioThreads;
    private final TcpConnectionPoolConfig poolConfig;
    private final ClientRoutingState routingState = new ClientRoutingState();
    private final LoginRoutingHook loginRoutingHook = new LoginRoutingHook() {

        @Override
        public CompletableFuture<IdentityInfo> loginOnLeader(
                LongFunction<CompletableFuture<IdentityInfo>> loginAttempt) {
            return AsyncIggyTcpClient.this.loginOnLeader(loginAttempt);
        }

        @Override
        public Runnable logoutCompletion() {
            synchronized (AsyncIggyTcpClient.this) {
                long generation = explicitLoginGeneration;
                return () -> forgetLogin(generation);
            }
        }
    };
    private final AtomicReference<AsyncTcpConnection> connection = new AtomicReference<>();
    private final PollRouter pollRouter;
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
     * The login the last explicit sign-in ran, replayed after a redial and by
     * a heartbeat that finds the session lost, so the session is
     * re-established on whichever node answers. The supplier already carries
     * the credentials it signed in with, so nothing new is stored. A sign-out,
     * a close and a rejected replay clear it, because they leave no session to
     * restore, and so does an explicit sign-in as it starts, because it
     * replaces the session. A committed credential change replaces the login
     * inside it, not the holder, so a replay that already runs stays the
     * remembered one and sends the new credentials.
     */
    private volatile RememberedLogin rememberedLogin;

    private long explicitLoginGeneration;
    private AsyncTcpConnection registeringConnection;

    /**
     * The Register of the last automatic sign-in and the connection that
     * carries it. While it runs, it holds the sign-in gate, and an explicit
     * sign-in ends it on that connection instead of waiting for its reply.
     * Guarded by this client.
     */
    private PendingReplay pendingReplay;

    /**
     * The transient failovers that hold or wait for the sign-in gate and that no explicit sign-in ended. A walk can
     * hold the gate until the request deadline, so an explicit sign-in fails them instead of waiting. Guarded by
     * this client.
     */
    private final Set<CompletableFuture<Void>> pendingFailovers = new HashSet<>();

    private volatile boolean closed;
    private MessagesClient messagesClient;
    private ConsumerGroupsClient consumerGroupsClient;
    private ConsumerOffsetsClient consumerOffsetsClient;
    private StreamsClient streamsClient;
    private TopicsClient topicsClient;
    private UsersTcpClient usersClient;
    private SystemClient systemClient;
    private PersonalAccessTokensClient personalAccessTokensClient;
    private PartitionsClient partitionsClient;
    private RawPartitionRequests rawRequests;

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
                Optional.empty(),
                Optional.empty(),
                AsyncTcpConnection.DEFAULT_IO_THREADS);
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
            Optional<File> tlsCertificate,
            Optional<IoEventLoopGroup> sharedEventLoopGroup,
            int ioThreads) {
        this.connectionInfo = new ConnectionInfo(host, port);
        this.seedConnectionInfo = this.connectionInfo;
        this.username = Optional.ofNullable(username);
        this.password = Optional.ofNullable(password);
        this.connectionTimeout = Optional.ofNullable(connectionTimeout);
        this.acquireTimeout = Optional.ofNullable(acquireTimeout);
        this.requestTimeout = Optional.ofNullable(requestTimeout);
        this.pollRouter = new PollRouter(
                connection::get, this::openPollConnection, this.requestTimeout.orElse(PollRouter.POLL_TIMEOUT));
        this.pollRouter.coordinatorEndpoint(() -> connectionInfo);
        this.pollRouter.configuredEndpoint(seedConnectionInfo);
        this.heartbeatInterval = heartbeatInterval;
        this.maxVsrFrameSize = maxVsrFrameSize;
        this.retryPolicy = Optional.ofNullable(retryPolicy);
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;
        this.sharedEventLoopGroup = sharedEventLoopGroup;
        this.ioThreads = ioThreads;

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
        session.endLineage();
        session.reset();
        pollRouter.clearSession();
        ConnectionInfo target = connectionInfo;
        AsyncTcpConnection newConnection = openConnection(target);
        AsyncTcpConnection previousConnection;
        // Under the lock of replay(), so a replay records the connection that carries its Register.
        synchronized (this) {
            pendingReplay = null;
            previousConnection = connection.getAndSet(newConnection);
        }
        if (previousConnection != null) {
            previousConnection.close();
        }
        routingState.clearAssignments();
        Supplier<AsyncTcpConnection> currentConnection = connection::get;
        return newConnection.connect().thenRun(() -> {
            log.debug("Connected to {} | {}", target.serverAddress(), IggyVersion.getInstance());
            var messages = new MessagesTcpClient(currentConnection, routingState, pollRouter);
            var offsets = new ConsumerOffsetsTcpClient(currentConnection, pollRouter);
            messagesClient = messages;
            consumerGroupsClient = new ConsumerGroupsTcpClient(currentConnection);
            consumerOffsetsClient = offsets;
            rawRequests = new RawPartitionRequests(currentConnection, pollRouter, messages, offsets);
            // One partition can be cached under its name and under its id, so
            // this client's own topology changes drop every send context.
            streamsClient = new StreamsTcpClient(currentConnection, pollRouter::clear);
            topicsClient = new TopicsTcpClient(currentConnection, pollRouter::clear);
            usersClient = new UsersTcpClient(currentConnection, loginRoutingHook);
            systemClient = new SystemTcpClient(currentConnection);
            personalAccessTokensClient = new PersonalAccessTokensTcpClient(currentConnection, loginRoutingHook);
            partitionsClient = new PartitionsTcpClient(currentConnection, pollRouter::clear);
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
    public synchronized CompletableFuture<IdentityInfo> login() {
        long generation = beginExplicitSignIn();
        try {
            if (usersClient == null) {
                throw new IggyNotConnectedException();
            }
            if (username.isEmpty() || password.isEmpty()) {
                throw new IggyMissingCredentialsException();
            }
            return loginOnLeader(
                    deadline -> usersClient.loginWithoutRedirect(username.get(), password.get(), deadline),
                    true,
                    generation);
        } catch (RuntimeException failure) {
            discardExplicitSignIn(generation);
            throw failure;
        }
    }

    /**
     * Sends a command code and payload and returns the raw response payload.
     *
     * <p>Session-control codes complete the returned future with an invalid-command error.
     *
     * <p>SendMessages, PollMessages, StoreConsumerOffset and DeleteConsumerOffset
     * carry the partition context the server fences them by, which the client
     * captures before it sends them. A SendMessages payload must name an explicit
     * partition id: balanced and messages-key partitioning complete the future
     * with a feature-unavailable error, because only the typed
     * {@link MessagesClient#sendMessages} resolves them to a partition. A payload
     * of these commands that does not follow their binary layout completes the
     * future with an {@link org.apache.iggy.exception.IggyInvalidArgumentException}.
     * Neither failure sends anything.
     *
     * <p>PollMessages takes the routed path of a typed poll, and StoreConsumerOffset
     * and DeleteConsumerOffset that of a typed offset write. Each uses the context
     * its route reports, so after another client deleted and recreated the
     * partition, one call can fail with code 87 (5009 for a consumer group). The
     * failed route is dropped and the next call routes again.
     *
     * <p>The future completes on the client event loop, as the callbacks section of this class says.
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
        RawPartitionRequests raw = rawRequests;
        if (connection.get() == null || raw == null) {
            throw new IggyNotConnectedException();
        }

        return pollRouter.onClientLoop(AsyncTcpConnection.mapResponse(raw.send(code, payload), response -> {
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
        }));
    }

    /**
     * Replays a retained mutation with its original identity, context and bytes. While another resolve of it
     * replays, this one waits for that replay within {@code timeout}. A wait that runs out fails with
     * {@code OPERATION_UNCERTAIN}.
     *
     * <p>The client keeps the proved outcomes of its last {@code count} replays of
     * {@code pendingOperations(count, bytes)}, also across a session reset. A later resolve of the operation
     * returns the same response body or committed rejection without a request. {@code OPERATION_NOT_RETAINED}
     * means that the outcome is unknown and this client can no longer recover it. A session reset or an abandon
     * dropped the operation before a replay proved its outcome, or that outcome aged out.
     *
     * <p>The future completes on the client event loop, as the callbacks section of this class says.
     */
    public CompletableFuture<byte[]> resolveOperation(long operationId, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IggyInvalidArgumentException("Resolution timeout must be positive");
        }
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException ignored) {
            timeoutNanos = Long.MAX_VALUE;
        }
        if (connection.get() == null) {
            throw new IggyNotConnectedException();
        }
        try {
            return pollRouter.onClientLoop(AsyncTcpConnection.mapResponse(
                    pollRouter.resolve(operationId, Duration.ofNanos(timeoutNanos)), response -> {
                        try {
                            byte[] body = new byte[response.readableBytes()];
                            response.readBytes(body);
                            return body;
                        } finally {
                            response.release();
                        }
                    }));
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    /**
     * Releases local recovery state and frees the pending operation slot at once. This does not prove that the
     * operation was uncommitted. A replay of the operation that is already running keeps its lane until the replay
     * ends, so a later mutation of that lane waits for it. That replay still keeps the outcome it proves for
     * {@link #resolveOperation}.
     */
    public CompletableFuture<Void> abandonOperation(long operationId) {
        if (connection.get() == null) {
            throw new IggyNotConnectedException();
        }
        try {
            connection.get().sessionOwner().abandon(operationId);
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    void configurePendingOperations(int count, long bytes) {
        if (count <= 0 || bytes <= 0) {
            throw new IllegalArgumentException("Pending operation limits must be positive");
        }
        session.configurePendingOperations(count, bytes);
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
     * Closes the TCP connection and releases the Netty resources that this client owns.
     *
     * <p>If the client created its own event loop group, it shuts that group down gracefully.
     * The client does not shut down a group supplied through
     * {@link AsyncIggyTcpClientBuilder#eventLoopGroup(IoEventLoopGroup)}. The caller shuts
     * that group down. After you call this method, you cannot reuse the client. If you need
     * a client again, create a new instance.
     *
     * @return a {@link CompletableFuture} that completes when all resources are released
     */
    public CompletableFuture<Void> close() {
        // Closing is caller intent, like a logout: connect() clears `closed`
        // again, and a session the caller ended must not come back with the
        // credentials the earlier sign-in used.
        synchronized (this) {
            closed = true;
            explicitLoginGeneration++;
            rememberedLogin = null;
        }
        AsyncTcpConnection currentConnection = connection.get();
        session.endLineage();
        CompletableFuture<Void> dataClosed = pollRouter.clearSession();
        session.reset();
        CompletableFuture<Void> linksClosed =
                currentConnection == null ? dataClosed : dataClosed.thenCompose(ignored -> currentConnection.close());
        return linksClosed.thenCompose(ignored -> {
            IoEventLoopGroup owned;
            synchronized (this) {
                owned = sharedEventLoopGroup.isEmpty() ? eventLoopGroup : null;
                if (owned != null) {
                    eventLoopGroup = null;
                }
            }
            if (owned == null) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            owned.shutdownGracefully().addListener(done -> {
                if (done.isSuccess()) {
                    stopped.complete(null);
                } else {
                    stopped.completeExceptionally(done.cause());
                }
            });
            return stopped;
        });
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

    private synchronized IoEventLoopGroup eventLoopGroup() {
        if (eventLoopGroup == null) {
            eventLoopGroup = sharedEventLoopGroup.orElseGet(() -> AsyncTcpConnection.newEventLoopGroup(ioThreads));
        }
        return eventLoopGroup;
    }

    private AsyncTcpConnection openConnection(ConnectionInfo target) {
        AsyncTcpConnection opened = new AsyncTcpConnection(
                target.host(),
                target.port(),
                enableTls,
                tlsCertificate,
                poolConfig,
                Optional.of(eventLoopGroup()),
                ioThreads,
                dialTimeout(),
                requestTimeout,
                heartbeatInterval,
                maxVsrFrameSize,
                this::recheckLeader,
                this::onSessionReset,
                this::onLinkFailure);
        opened.followSession(session);
        opened.onHeartbeat(
                () -> signInIfOwed(opened),
                () -> !closed
                        && rememberedLogin != null
                        && (reconnecting.get() != null
                                || retryPolicy.orElse(DEFAULT_RECONNECT_POLICY).getMaxRetries() != 0));
        opened.onCredentialsRefreshed(this::refreshLogin);
        opened.onSignInRefused(this::forgetRefusedLogin);
        return opened;
    }

    private AsyncTcpConnection openPollConnection(ConnectionInfo target) {
        AsyncTcpConnection opened = new AsyncTcpConnection(
                target.host(),
                target.port(),
                enableTls,
                tlsCertificate,
                poolConfig,
                Optional.of(connection.get().eventLoopGroup()),
                ioThreads,
                dialTimeout(),
                requestTimeout,
                heartbeatInterval,
                maxVsrFrameSize,
                this::recheckLeader,
                this::onSessionReset,
                this::onLinkFailure);
        // A poll connection becomes the coordinator when the leader moves to its node.
        opened.onCredentialsRefreshed(this::refreshLogin);
        opened.onSignInRefused(this::forgetRefusedLogin);
        return opened;
    }

    /**
     * Redials only for the connection the client uses. Closing a replaced connection, or a poll link that the router
     * drops, fails its pending requests as lost links, and a redial for them would dial and sign in again behind a
     * connection that still works.
     *
     * <p>The Rust SDK differs: each replica link redials itself with backoff when it drops. Here a poll link opens a
     * new channel only when the next request needs one, and the client redials only when the requests or the heartbeat
     * of the connection it uses find the node dead.
     *
     * <p>A link reports its failure inline, also on the event loop while that thread holds a poll monitor that a
     * caller under the lock of this client can wait for, so this listener takes no lock of this client.
     */
    private void onLinkFailure(AsyncTcpConnection source, Throwable cause) {
        if (connection.get() == source) {
            onConnectionFailure(cause);
        }
    }

    /**
     * How long one dial may take.
     *
     * With other endpoints queued behind this one, a node whose syns are
     * dropped must not hold the rotation, so the wait is capped at
     * {@link #FAILOVER_DIAL_TIMEOUT} - the bound the other SDKs use. A
     * configured connection timeout is capped too rather than exempted: it says
     * how long one endpoint may take, and a rotation that spends it on every
     * endpoint reaches the survivor long after the caller gave up. A client that
     * knows one endpoint keeps whatever it configured, since there is nothing
     * queued behind that dial.
     */
    Optional<Duration> dialTimeout() {
        if (redialCandidates().size() < 2) {
            return connectionTimeout;
        }
        return Optional.of(connectionTimeout
                .filter(configured -> configured.compareTo(FAILOVER_DIAL_TIMEOUT) < 0)
                .orElse(FAILOVER_DIAL_TIMEOUT));
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
        pollRouter.clearBindings();
        if (!connection.get().sessionOwner().isBound()) {
            routingState.clearAssignments();
            pollRouter.clear();
        }
        if (errorCode == IggyErrorCode.STALE_CLIENT.getCode()) {
            log.debug("The server evicted this session as stale; the next request re-establishes it");
        }
    }

    /**
     * Rechecks the leader after non-admission or connection loss and returns the connection that carries the next
     * attempt. Retained mutation state preserves the frozen identity and any earlier uncertainty.
     */
    private CompletableFuture<AsyncTcpConnection> recheckLeader(
            AsyncTcpConnection source,
            long requestDeadlineNanos,
            Throwable rejection,
            AsyncTcpConnection.TransientFailoverState failoverState) {
        return prepareTransientFailover(source, requestDeadlineNanos, rejection, failoverState)
                .thenCompose(ignored -> {
                    if (requestDeadlineNanos - System.nanoTime() <= 0) {
                        return CompletableFuture.failedFuture(rejection);
                    }
                    AsyncTcpConnection current = connection.get();
                    return current == null
                            ? CompletableFuture.failedFuture(new IggyNotConnectedException())
                            : CompletableFuture.completedFuture(current);
                });
    }

    /**
     * Holds the sign-in gate while the request moves to another node. An explicit sign-in fails the returned future
     * instead of waiting for the walk, and the walk then moves the client nowhere, see {@link #endAutomaticSignIn()}.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<Void> prepareTransientFailover(
            AsyncTcpConnection source,
            long requestDeadlineNanos,
            Throwable rejection,
            AsyncTcpConnection.TransientFailoverState failoverState) {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        CompletableFuture<Void> failover = new CompletableFuture<>();
        CompletableFuture<Void> previous;
        synchronized (this) {
            if (failoverState.signedOut()) {
                return CompletableFuture.failedFuture(
                        new IggyNotConnectedException(AsyncTcpConnection.SIGN_IN_REPLACED));
            }
            previous = loginChain.getAndSet(gate);
            pendingFailovers.add(failover);
        }
        BooleanSupplier pending = () -> isPending(failover);
        CompletableFuture<Void> queued = previous.thenApply(ignored -> null);
        queued.orTimeout(Math.max(0, requestDeadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
        queued.thenCompose(ignored -> {
                    if (closed) {
                        return CompletableFuture.failedFuture(new IggyNotConnectedException());
                    }
                    if (requestDeadlineNanos - System.nanoTime() <= 0) {
                        return CompletableFuture.failedFuture(rejection);
                    }
                    if (connection.get() != source || !pending.getAsBoolean()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    CompletableFuture<Void> recheck = rejection instanceof IggyServerException serverError
                                    && serverError.getRawErrorCode() == AsyncTcpConnection.NOT_PRIMARY
                            ? findLeaderElsewhere(connectionInfo, requestDeadlineNanos)
                                    .thenCompose(target -> target.isPresent()
                                            ? retarget(target.get(), requestDeadlineNanos, pending)
                                            : CompletableFuture.completedFuture(null))
                            : walkRoster(requestDeadlineNanos, failoverState, pending);
                    return recheck.thenCompose(
                            ignoredWalk -> signInAfterFailover(source, failover, requestDeadlineNanos, rejection));
                })
                .whenComplete((ignored, error) -> {
                    // An explicit sign-in that ended this failover completes it.
                    synchronized (this) {
                        if (!pendingFailovers.remove(failover)) {
                            return;
                        }
                    }
                    if (error == null) {
                        failover.complete(null);
                    } else {
                        failover.completeExceptionally(error);
                    }
                });
        failover.whenComplete((ignored, error) -> previous.whenComplete((prior, priorError) -> gate.complete(null)));
        return failover;
    }

    /**
     * Signs the session in on the connection that the failover moved to, with the login that {@code source} holds
     * at that moment: a credential change can commit during the walk. This Register is an automatic sign-in, so an
     * explicit sign-in ends it like a replay.
     */
    private CompletableFuture<Void> signInAfterFailover(
            AsyncTcpConnection source,
            CompletableFuture<Void> failover,
            long requestDeadlineNanos,
            Throwable rejection) {
        AsyncTcpConnection current = connection.get();
        if (current == null || current == source || current.isAuthenticated()) {
            return CompletableFuture.completedFuture(null);
        }
        if (requestDeadlineNanos - System.nanoTime() <= 0) {
            return CompletableFuture.failedFuture(rejection);
        }
        long generation = session.generation();
        CompletableFuture<Void> signIn = new CompletableFuture<>();
        synchronized (this) {
            if (!pendingFailovers.contains(failover)) {
                return CompletableFuture.completedFuture(null);
            }
            pendingReplay = new PendingReplay(current, signIn);
        }
        try {
            // On the loop that abortSignIn runs on, so an ended sign-in either finds this Register on a channel that
            // it closes or keeps it from being sent.
            current.eventLoop().execute(() -> sendSignIn(source, current, signIn, requestDeadlineNanos));
        } catch (RejectedExecutionException loopGone) {
            signIn.completeExceptionally(new IggyNotConnectedException());
        }
        // Before the failover ends, so the call that waits for it sees the lineage end.
        return signIn.whenComplete((ignored, error) -> {
            synchronized (this) {
                if (isSignInRejection(unwrap(error)) && session.refuseSignIn(generation, error)) {
                    log.warn(
                            "The server refused the sign-in ({}). The client forgets the credentials.",
                            unwrap(error).getMessage());
                    rememberedLogin = null;
                }
            }
        });
    }

    /** Sends the login that {@code source} holds now on {@code target}, unless {@code signIn} already ended. */
    private synchronized void sendSignIn(
            AsyncTcpConnection source,
            AsyncTcpConnection target,
            CompletableFuture<Void> signIn,
            long requestDeadlineNanos) {
        if (signIn.isDone()) {
            return;
        }
        Optional<AsyncTcpConnection.AuthenticationSnapshot> authentication = source.authenticationSnapshot();
        if (authentication.isEmpty()) {
            signIn.complete(null);
            return;
        }
        long lineage = source.sessionOwner().lineage();
        CompletableFuture<ByteBuf> sent = target.send(
                authentication.get().commandCode(), authentication.get().payload(), requestDeadlineNanos);
        signIn.whenComplete((ignored, error) -> {
            if (signIn.isCancelled()) {
                sent.cancel(false);
            }
        });
        sent.whenComplete((response, error) -> {
            AsyncTcpConnection.releaseIfPresent(response);
            synchronized (this) {
                if (signIn.isDone()) {
                    return;
                }
                if (source.sessionOwner().lineage() != lineage) {
                    signIn.completeExceptionally(source.sessionOwner().lineageEnded(lineage, error));
                } else if (error == null) {
                    target.followLogin(source.sessionLogin());
                    signIn.complete(null);
                } else if (isSignInRejection(unwrap(error))
                        && source.isAuthenticated()
                        && source.sessionLogin().version()
                                != authentication.get().version()) {
                    sendSignIn(source, target, signIn, requestDeadlineNanos);
                } else {
                    signIn.completeExceptionally(error);
                }
            }
        });
    }

    private synchronized boolean isPending(CompletableFuture<Void> failover) {
        return pendingFailovers.contains(failover);
    }

    /**
     * Entry point of the background redial after a pool acquire failure or an
     * expired reply. Requests that were in flight stay failed (their outcome
     * is unknown); the redial only restores the client for subsequent calls.
     * Each rotation sweeps every endpoint the client knows -- where it was,
     * the metadata primary, the configured seed, then the roster it learned.
     * Only a rotation that reaches none of them waits out the retry policy's delay. The
     * sign-in that last succeeded is replayed on whichever endpoint answers,
     * personal access tokens included.
     */
    private void onConnectionFailure(Throwable cause) {
        if (closed || !isConnectionLoss(cause)) {
            return;
        }
        Object recovery = new Object();
        if (!reconnecting.compareAndSet(null, recovery)) {
            return;
        }
        log.warn("Connection to {} lost ({}), starting redial", connectionInfo.serverAddress(), cause.getMessage());
        RetryPolicy policy = retryPolicy.orElse(DEFAULT_RECONNECT_POLICY);
        redialAttempt(1, policy).whenComplete((ignored, error) -> reconnecting.compareAndSet(recovery, null));
    }

    /**
     * Signs in again on a connection that answers heartbeats but carries no
     * sign-in while one is remembered. A burst leaves that state when the
     * session was lost and every sign-in it replayed lost its link, and
     * nothing else restores the session: a ping needs no session, and a
     * request without one fails before it is sent.
     *
     * The sign-in on the live connection is the first step of a new burst
     * with a fresh budget, so a budget bounds one burst, not the session. A
     * policy without retries restarts nothing.
     */
    private synchronized void signInIfOwed(AsyncTcpConnection source) {
        RetryPolicy policy = retryPolicy.orElse(DEFAULT_RECONNECT_POLICY);
        if (closed
                || policy.getMaxRetries() == 0
                || connection.get() != source
                || source.isAuthenticated()
                || rememberedLogin == null) {
            return;
        }
        Object recovery = new Object();
        if (!reconnecting.compareAndSet(null, recovery)) {
            return;
        }
        log.warn("The session on {} was lost, signing in again", connectionInfo.serverAddress());
        replaySignInOn(connectionInfo, redialCandidates(), 0, 1, policy)
                .whenComplete((ignored, error) -> reconnecting.compareAndSet(recovery, null));
    }

    private static boolean isConnectionLoss(Throwable cause) {
        return cause instanceof ConnectTimeoutException
                || cause instanceof IggyConnectionException
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
            // The rotations actually run, not the configured budget: a policy of
            // zero retries still sweeps once when it knows several endpoints.
            log.error(
                    "Redial gave up after {} rotations. The next request or heartbeat dials again, and a failed"
                            + " dial starts a new redial",
                    attempt - 1);
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
        // A policy of zero retries that knows several endpoints still gets the
        // one rotation they were made known for, so the budget shown here is
        // what will actually run rather than what was configured.
        int rotations = Math.max(policy.getMaxRetries(), candidates.size() > 1 ? 1 : 0);
        log.info(
                "Redial attempt {}/{} to {} ({}/{})",
                attempt,
                rotations,
                target.serverAddress(),
                index + 1,
                candidates.size());
        boolean bound = session.isBound();
        return retarget(target)
                .handle((retargeted, dialError) -> {
                    if (dialError != null) {
                        log.warn("Redial to {} failed: {}", target.serverAddress(), dialError.getMessage());
                        // A refused bind ended the session on a node that is up, so the node gets one more try,
                        // which signs in instead of binding. If that dial fails too, the connection the client
                        // keeps still counts as signed in, so no heartbeat signs in again. The next request on it
                        // signs in before it is sent.
                        int next = bound && !session.isBound() ? index : index + 1;
                        return sweepCandidates(candidates, next, attempt, policy);
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
        Object recovery = reconnecting.get();
        long generation = session.generation();
        RememberedLogin replay = rememberedLogin;
        LongFunction<CompletableFuture<IdentityInfo>> credentials = replay == null ? null : replay.login;
        CompletableFuture<Void> restored =
                connection.get().isAuthenticated() ? CompletableFuture.completedFuture(null) : replayLogin(replay);
        return restored.handle((ok, loginError) -> {
                    if (loginError == null) {
                        // Roster lookup tolerates a lost link. The burst retires before it checks the link, since a
                        // loss that the failure listener saw first found this burst running and started none. A loss
                        // after the retire starts its own burst, and this one stops.
                        reconnecting.compareAndSet(recovery, null);
                        if (rememberedLogin == replay
                                && !connection.get().hasOpenChannel()
                                && reconnecting.compareAndSet(null, recovery)) {
                            return sweepCandidates(candidates, index + 1, attempt, policy);
                        }
                        log.info("Reconnected to {}", target.serverAddress());
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    // An explicit sign-in, a sign-out or a close ended this sign-in, so nothing is left to restore.
                    if (rememberedLogin != replay) {
                        log.info("The caller ended the sign-in on {}, so the redial stops", target.serverAddress());
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    // A rejection of credentials that a refresh replaced meanwhile says nothing about the new ones.
                    if (!isSignInRejection(unwrap(loginError)) || replay.login != credentials) {
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
                    session.refuseSignIn(generation, loginError);
                    return CompletableFuture.<Void>completedFuture(null);
                })
                .thenCompose(Function.identity());
    }

    /**
     * Whether the server answered the sign-in with a verdict no other endpoint
     * would change: a rotated password, an expired token. The codes are those
     * of the Rust SDK.
     *
     * Only that ends a redial. Everything else -- the channel closing before
     * the reply, a timeout, a transient refusal from a node that is not the
     * primary -- says nothing about the credentials, and treating it as a
     * rejection would drop them and leave the client published on a node that
     * is already gone, with every later call failing "not authenticated".
     */
    static boolean isSignInRejection(Throwable error) {
        return error instanceof IggyServerException serverError
                && SIGN_IN_REJECTIONS.contains(serverError.getRawErrorCode());
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    /**
     * Re-establishes the session on the freshly published connection with the
     * sign-in that last succeeded, the same rule as in every other SDK: a
     * client is whoever it last signed in as. The connection also
     * re-authenticates a replacement channel from the login payload it
     * captured, which is that same last sign-in, so replaying a different user
     * here would make one eviction land on a different session depending on
     * whether the channel or the redial got there first.
     *
     * The credentials configured on the builder are no fallback. Without a
     * remembered sign-in, the caller signed out, closed the client, had the
     * sign-in rejected or never signed in, and a sign-in here would undo that.
     */
    private CompletableFuture<Void> replayLogin(LongFunction<CompletableFuture<IdentityInfo>> replay) {
        if (replay != null) {
            // Runs through loginOnLeader, so a redial that landed on a backup
            // still settles on the leader before the session is used.
            return loginOnLeader(replay, false).thenApply(identity -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Serializes Register and authenticated leader settlement across concurrent
     * logins. Queueing, Register, and roster settlement share one deadline.
     * A leader change attaches the authenticated session to the managed link.
     */
    CompletableFuture<IdentityInfo> loginOnLeader(LongFunction<CompletableFuture<IdentityInfo>> loginAttempt) {
        return loginOnLeader(loginAttempt, true);
    }

    /**
     * An explicit sign-in takes ownership before queueing or validating credentials. Its failure leaves
     * the client signed out, and an older completion cannot restore or clear its state. A replay runs only
     * while it is still the remembered sign-in. The gate covers Register and leader settlement, including
     * after caller cancellation, so two Registers cannot overlap on the shared connection.
     */
    private CompletableFuture<IdentityInfo> loginOnLeader(
            LongFunction<CompletableFuture<IdentityInfo>> loginAttempt, boolean explicit) {
        return loginOnLeader(loginAttempt, explicit, null);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<IdentityInfo> loginOnLeader(
            LongFunction<CompletableFuture<IdentityInfo>> loginAttempt, boolean explicit, Long claimedGeneration) {
        long deadline = System.nanoTime()
                + requestTimeout.orElse(PollRouter.POLL_TIMEOUT).toNanos();
        CompletableFuture<Void> gate = new CompletableFuture<>();
        CompletableFuture<Void> previous;
        long generation;
        synchronized (this) {
            previous = loginChain.getAndSet(gate);
            // Join the gate before aborting recovery, so its completion cannot overtake this owner.
            generation = claimedGeneration != null
                    ? claimedGeneration
                    : explicit ? beginExplicitSignIn() : explicitLoginGeneration;
        }
        CompletableFuture<IdentityInfo> callerFuture = new CompletableFuture<>();
        CompletableFuture<Void> queued = previous.thenApply(ignored -> null);
        queued.orTimeout(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        CompletableFuture<IdentityInfo> transaction = queued.thenCompose(ignored -> {
            if (!takeTurn(loginAttempt, explicit, generation)) {
                return explicit
                        ? CompletableFuture.failedFuture(
                                new IggyNotConnectedException(AsyncTcpConnection.SIGN_IN_REPLACED))
                        : CompletableFuture.completedFuture(null);
            }
            // The server keeps the user of each client id, also after its
            // session ended, and refuses another user on it. A sign-in that a
            // lost session owed can register this client id without its reply
            // arriving. Over a live sign-in, the logout before Register
            // replaces the session instead.
            AsyncTcpConnection current = connection.get();
            if (explicit && (current == null || !current.isAuthenticated())) {
                session.reset();
            }
            routingState.clearAssignments();
            return pollRouter
                    .clearSession(current)
                    .thenCompose(cleared -> loginAndSettleOnLeader(
                            explicit ? loginAttempt : replayDeadline -> replay(loginAttempt, replayDeadline),
                            deadline,
                            () -> ownsSignIn(generation, callerFuture)));
        });
        callerFuture.whenComplete((identity, error) -> {
            if (explicit && callerFuture.isCancelled()) {
                discardExplicitSignIn(generation);
            }
        });
        transaction.whenComplete((identity, error) -> {
            // Not after a close: a login still in flight when `close()` cleared
            // this would set it again, and `connect()` clears `closed`, so the
            // next loss would replay a sign-in the caller had ended.
            synchronized (this) {
                if (explicit && error == null && ownsSignIn(generation, callerFuture)) {
                    rememberedLogin =
                            new RememberedLogin(loginAttempt, connection.get().sessionLogin());
                    session.finishSignIn();
                }
            }
            if (explicit && error != null && !callerFuture.isCancelled()) {
                discardExplicitSignIn(generation);
            }
            previous.whenComplete((prior, priorError) -> gate.complete(null));
            if (error != null) {
                Throwable failure = unwrap(error);
                callerFuture.completeExceptionally(
                        explicit && (failure instanceof IggyTimeoutException || failure instanceof TimeoutException)
                                ? AsyncTcpConnection.notAccepted()
                                : error);
            } else {
                callerFuture.complete(identity);
            }
        });
        return callerFuture;
    }

    private synchronized boolean takeTurn(
            LongFunction<CompletableFuture<IdentityInfo>> loginAttempt, boolean explicit, long generation) {
        return explicit ? generation == explicitLoginGeneration && !closed : rememberedLogin == loginAttempt;
    }

    private synchronized long beginExplicitSignIn() {
        explicitLoginGeneration++;
        session.beginSignIn();
        endAutomaticSignIn();
        AsyncTcpConnection current = connection.get();
        if (current != null && !session.isBound() && (current.isAuthenticated() || registeringConnection == current)) {
            current.discardAuthentication();
            session.reset();
        }
        return explicitLoginGeneration;
    }

    private synchronized boolean ownsSignIn(long generation, CompletableFuture<?> caller) {
        return generation == explicitLoginGeneration && !closed && !caller.isCancelled();
    }

    private synchronized void discardExplicitSignIn(long generation) {
        if (generation != explicitLoginGeneration) {
            return;
        }
        rememberedLogin = null;
        AsyncTcpConnection current = connection.get();
        if (current != null) {
            current.discardAuthentication();
        }
        session.endLineage();
        session.reset();
        routingState.clearAssignments();
        pollRouter.clearSession(current);
    }

    /**
     * An automatic sign-in can hold the gate until the deadline of its
     * Register, and its reply can be lost. An explicit sign-in ends it
     * instead of waiting behind it. The late reply binds nothing, because
     * the explicit sign-in registers a new client id.
     *
     * A failover holds the gate too, and its walk can wait for a node until the request deadline. The failover fails
     * as not connected, and only after the abort of the Register that it can send: its gate opens as it fails, and
     * no automatic Register may reach a channel after the turn of the explicit sign-in started.
     */
    private void endAutomaticSignIn() {
        PendingReplay replay;
        List<CompletableFuture<Void>> failovers;
        synchronized (this) {
            rememberedLogin = null;
            replay = pendingReplay;
            failovers = List.copyOf(pendingFailovers);
            pendingFailovers.clear();
        }
        CompletableFuture<?> aborted = CompletableFuture.completedFuture(null);
        if (replay != null && replay.connection() != null) {
            replay.connection().abortSignIn(replay.signIn());
            aborted = replay.signIn();
        }
        aborted.whenComplete((ignored, error) -> failovers.forEach(failover ->
                failover.completeExceptionally(new IggyNotConnectedException(AsyncTcpConnection.SIGN_IN_REPLACED))));
    }

    /**
     * Sends the Register of an automatic sign-in while it is still the
     * remembered one. The check and the send share the lock with
     * {@link #endAutomaticSignIn()}, so an explicit sign-in either finds
     * this Register or keeps it from being sent.
     */
    private synchronized CompletableFuture<IdentityInfo> replay(
            LongFunction<CompletableFuture<IdentityInfo>> loginAttempt, long deadline) {
        if (rememberedLogin != loginAttempt) {
            return CompletableFuture.failedFuture(
                    new CancellationException("This sign-in is no longer the one to replay"));
        }
        AsyncTcpConnection carrier = connection.get();
        CompletableFuture<IdentityInfo> signIn = loginAttempt.apply(deadline);
        pendingReplay = new PendingReplay(carrier, signIn);
        return signIn.thenApply(identity -> {
            synchronized (this) {
                if (rememberedLogin == loginAttempt) {
                    carrier.followLogin(rememberedLogin.credentials);
                }
            }
            return identity;
        });
    }

    private CompletableFuture<IdentityInfo> loginAndSettleOnLeader(
            LongFunction<CompletableFuture<IdentityInfo>> loginAttempt, long deadline, BooleanSupplier wanted) {
        CompletableFuture<IdentityInfo> registration;
        synchronized (this) {
            if (!wanted.getAsBoolean()) {
                return CompletableFuture.failedFuture(
                        new IggyNotConnectedException(AsyncTcpConnection.SIGN_IN_REPLACED));
            }
            AsyncTcpConnection carrier = connection.get();
            registration = loginAttempt.apply(deadline);
            registeringConnection = carrier;
            registration.whenComplete((identity, error) -> {
                synchronized (this) {
                    if (registeringConnection == carrier) {
                        registeringConnection = null;
                    }
                }
            });
        }
        return registration.thenCompose(identity -> {
            if (!wanted.getAsBoolean()) {
                return CompletableFuture.completedFuture(identity);
            }
            ConnectionInfo currentTarget = connectionInfo;
            return findLeaderElsewhere(currentTarget, deadline).thenCompose(leaderTarget -> {
                if (leaderTarget.isEmpty() || !wanted.getAsBoolean()) {
                    return CompletableFuture.completedFuture(identity);
                }
                return retarget(leaderTarget.get(), deadline, wanted).handle((ignored, error) -> {
                    if (error != null) {
                        log.warn(
                                "Failed to reconnect to leader at {}: {}, connection will continue on server node {}",
                                leaderTarget.get().serverAddress(),
                                error.getMessage(),
                                currentTarget.serverAddress());
                    }
                    return identity;
                });
            });
        });
    }

    /**
     * Walks the known roster once after a lost link or a never-admitted
     * refusal. Routed requests never come here, so the walk tries the cached
     * metadata primary first and the other nodes in roster order after it.
     * Failed dials are skipped within the same request deadline, and the walk
     * stops once {@code wanted} turns false.
     */
    private CompletableFuture<Void> walkRoster(
            long requestDeadlineNanos,
            AsyncTcpConnection.TransientFailoverState failoverState,
            BooleanSupplier wanted) {
        ConnectionInfo currentTarget = connectionInfo;
        failoverState.visitedTargets().add(currentTarget);
        List<ConnectionInfo> targets = new ArrayList<>(
                LeaderAwareness.rosterWalkTargets(rosterTargets, currentTarget, failoverState.visitedTargets()));
        ConnectionInfo metadataPrimary = pollRouter.metadataPrimary();
        if (metadataPrimary != null
                && failoverState.visitedTargets().stream()
                        .noneMatch(visited -> LeaderAwareness.isSameAddress(visited, metadataPrimary))) {
            targets.removeIf(target -> LeaderAwareness.isSameAddress(target, metadataPrimary));
            targets.add(0, metadataPrimary);
        }
        return walkRoster(targets, 0, currentTarget, requestDeadlineNanos, failoverState, wanted);
    }

    private CompletableFuture<Void> walkRoster(
            List<ConnectionInfo> targets,
            int index,
            ConnectionInfo originalTarget,
            long requestDeadlineNanos,
            AsyncTcpConnection.TransientFailoverState failoverState,
            BooleanSupplier wanted) {
        if (index >= targets.size() || requestDeadlineNanos - System.nanoTime() <= 0 || !wanted.getAsBoolean()) {
            return CompletableFuture.completedFuture(null);
        }
        ConnectionInfo target = targets.get(index);
        failoverState.visitedTargets().add(target);
        log.info(
                "The request was refused on {}, walking the roster to {}",
                originalTarget.serverAddress(),
                target.serverAddress());
        return retarget(target, requestDeadlineNanos, wanted)
                .handle((ignored, error) -> {
                    if (error == null) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    log.warn("Roster walk to {} failed: {}", target.serverAddress(), error.getMessage());
                    return walkRoster(targets, index + 1, originalTarget, requestDeadlineNanos, failoverState, wanted);
                })
                .thenCompose(Function.identity());
    }

    /**
     * Picks a healthy leader living elsewhere from the roster, waiting out a
     * transiently leaderless election. Resolves to empty on any failure
     * (metadata fetch, malformed roster) so the redirection path never fails
     * the login that triggered it.
     */
    CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget) {
        return findLeaderElsewhere(
                currentTarget,
                System.nanoTime()
                        + requestTimeout.orElse(PollRouter.POLL_TIMEOUT).toNanos());
    }

    CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget, long deadline) {
        if (systemClient == null || deadline - System.nanoTime() <= 0) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        Duration remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
        Duration pause = remaining.compareTo(LeaderAwareness.LEADERLESS_POLL_INTERVAL) < 0
                ? remaining
                : LeaderAwareness.LEADERLESS_POLL_INTERVAL;
        return LeaderAwareness.findLeaderElsewhere(
                        () -> AsyncTcpConnection.mapResponse(
                                connection
                                        .get()
                                        .send(
                                                CommandCode.System.GET_CLUSTER_METADATA.getValue(),
                                                Unpooled.EMPTY_BUFFER,
                                                deadline),
                                response -> {
                                    try {
                                        return observeTopology(BytesDeserializer.readClusterMetadata(response));
                                    } finally {
                                        response.release();
                                    }
                                }),
                        currentTarget,
                        remaining,
                        pause)
                .thenApply(LeaderAwareness.LeaderLookup::redirect);
    }

    private ClusterMetadata observeTopology(ClusterMetadata metadata) {
        if (!metadata.nodes().isEmpty()) {
            rememberRoster(new LeaderAwareness.LeaderLookup(
                    Optional.empty(), LeaderAwareness.nodeTargets(metadata, seedConnectionInfo)));
            pollRouter.observeTopology(metadata);
        }
        return metadata;
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

    /**
     * Endpoints a redial rotates through, likeliest first: where the client
     * currently is, the last known metadata primary, the address it was
     * configured with, then the roster it learned while connected. Duplicates
     * are dropped by spelling, so an
     * endpoint the roster merely writes differently does not earn a second
     * attempt. Spelling only: this runs on the Netty event loop, where a
     * resolver lookup per candidate pair would block it.
     */
    private List<ConnectionInfo> redialCandidates() {
        List<ConnectionInfo> candidates = new ArrayList<>();
        candidates.add(connectionInfo);
        Stream.concat(Stream.of(pollRouter.metadataPrimary(), seedConnectionInfo), rosterTargets.stream())
                .filter(Objects::nonNull)
                .filter(endpoint ->
                        candidates.stream().noneMatch(candidate -> LeaderAwareness.isSameSpelling(candidate, endpoint)))
                .forEach(candidates::add);
        return List.copyOf(candidates);
    }

    CompletableFuture<Void> retarget(ConnectionInfo newTarget) {
        return retarget(
                newTarget,
                System.nanoTime()
                        + requestTimeout.orElse(PollRouter.POLL_TIMEOUT).toNanos());
    }

    CompletableFuture<Void> retarget(ConnectionInfo newTarget, long deadline) {
        return retarget(newTarget, deadline, () -> true);
    }

    /** Moves to {@code newTarget} unless {@code wanted} turned false before the move. */
    CompletableFuture<Void> retarget(ConnectionInfo newTarget, long deadline, BooleanSupplier wanted) {
        AsyncTcpConnection oldConnection = connection.get();
        if (oldConnection != null && oldConnection.sessionOwner().isBound()) {
            return pollRouter
                    .connectionFor(newTarget, deadline)
                    .thenCompose(managed -> publishConnection(oldConnection, managed, newTarget, wanted));
        }
        AsyncTcpConnection newConnection;
        try {
            newConnection = openConnection(newTarget);
        } catch (RuntimeException error) {
            return CompletableFuture.failedFuture(error);
        }
        AsyncTcpConnection openedConnection = newConnection;
        return newConnection
                .connect()
                .thenCompose(ignored -> publishConnection(oldConnection, openedConnection, newTarget, wanted))
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        openedConnection.close();
                    }
                });
    }

    private CompletableFuture<Void> publishConnection(
            AsyncTcpConnection oldConnection,
            AsyncTcpConnection newConnection,
            ConnectionInfo newTarget,
            BooleanSupplier wanted) {
        // Under the lock of endAutomaticSignIn(), so a failover that an explicit sign-in ended moves nothing.
        synchronized (this) {
            if (!wanted.getAsBoolean()) {
                return CompletableFuture.failedFuture(
                        new IggyNotConnectedException(AsyncTcpConnection.SIGN_IN_REPLACED));
            }
            // Every routed call reads the coordinator, so it must never become null.
            if (newConnection == null) {
                return CompletableFuture.failedFuture(AsyncTcpConnection.notAccepted());
            }
            if (!connection.compareAndSet(oldConnection, newConnection)) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("Connection replaced concurrently during leader redirection"));
            }
        }
        if (closed) {
            oldConnection.close();
            return CompletableFuture.failedFuture(
                    new IggyNotConnectedException("Client closed during leader redirection"));
        }
        connectionInfo = newTarget;
        if (oldConnection == newConnection || pollRouter.ownsConnection(oldConnection)) {
            return CompletableFuture.completedFuture(null);
        }
        if (oldConnection.sessionOwner() != newConnection.sessionOwner()) {
            routingState.clearAssignments();
            pollRouter.clearSession();
        }
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

    /**
     * Follows a committed password change or rename of the signed-in user: the configured credentials of that user
     * and the remembered sign-in take the new values.
     */
    void refreshLogin(
            AsyncTcpConnection source,
            String oldUsername,
            Optional<String> nextUsername,
            Optional<String> nextPassword) {
        if (username.filter(oldUsername::equals).isPresent()) {
            nextUsername.ifPresent(value -> username = Optional.of(value));
            nextPassword.ifPresent(value -> password = Optional.of(value));
        }
        rememberLoginOf(source);
    }

    /**
     * Replays the login of {@code source} from now on, also in a reconnect burst that already runs. Only a sign-in
     * that is still remembered follows it, so an explicit sign-in that took its turn, a sign-out and a close stay in
     * effect.
     */
    private synchronized void rememberLoginOf(AsyncTcpConnection source) {
        RememberedLogin remembered = rememberedLogin;
        if (closed || remembered == null) {
            return;
        }
        var snapshot = source.authenticationSnapshot();
        if (snapshot.isEmpty()) {
            return;
        }
        var authentication = snapshot.get();
        byte[] payload;
        try {
            payload = new byte[authentication.payload().readableBytes()];
            authentication.payload().readBytes(payload);
        } finally {
            authentication.payload().release();
        }
        remembered.credentials = source.sessionLogin();
        // mapResponse passes a cancel on to the send, which endAutomaticSignIn relies on.
        remembered.login = deadline -> AsyncTcpConnection.mapResponse(
                connection.get().send(authentication.commandCode(), Unpooled.wrappedBuffer(payload), deadline),
                response -> {
                    try {
                        return new IdentityInfo(response.readUnsignedIntLE(), Optional.empty());
                    } finally {
                        response.release();
                    }
                });
    }

    private synchronized void forgetLogin(long generation) {
        if (generation == explicitLoginGeneration) {
            rememberedLogin = null;
            routingState.clearAssignments();
            pollRouter.clearSession(connection.get());
        }
    }

    private synchronized void forgetRefusedLogin(long endedLineage) {
        if (session.lineage() == endedLineage) {
            rememberedLogin = null;
        }
    }

    private record PendingReplay(AsyncTcpConnection connection, CompletableFuture<?> signIn) {}

    /** The login that automatic sign-ins replay. Its identity marks one explicit sign-in, its login can change. */
    private static final class RememberedLogin implements LongFunction<CompletableFuture<IdentityInfo>> {
        private volatile LongFunction<CompletableFuture<IdentityInfo>> login;
        private SessionLogin credentials;

        private RememberedLogin(LongFunction<CompletableFuture<IdentityInfo>> login, SessionLogin credentials) {
            this.login = login;
            this.credentials = credentials;
        }

        @Override
        public CompletableFuture<IdentityInfo> apply(long deadline) {
            return login.apply(deadline);
        }
    }
}
