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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.client.async.tcp.vsr.VsrRequestEncoder;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyEmptyResponseException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.exception.IggyTlsException;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

/**
 * Async TCP connection using Netty for non-blocking I/O.
 * Manages the connection lifecycle and request/response correlation.
 */
public class AsyncTcpConnection {
    // Transient VSR denials (not-committed / not-accepted) are replayed with
    // the same encoded frame so the server's dedup sees the same request id.
    // A not-committed outcome is unknown, so it replays for the whole budget.
    // A not-accepted deny was refused outright (typically a demoted primary),
    // so after a short same-node retry it is handed to the owning client for
    // a leader recheck and safe replay.
    //
    // Package-private: the client classifies a failed sign-in by these codes,
    // and a transient one is not a rejected credential.
    static final int TRANSIENT_NOT_COMMITTED = 57;
    static final int TRANSIENT_NOT_ACCEPTED = 58;
    static final int NOT_PRIMARY = 89;
    static final long LIFECYCLE_RETRY_MAX_INTERVAL_MS = 1000;
    // The pool holds one channel, and one channel lives on one loop.
    static final int DEFAULT_IO_THREADS = 1;

    private static final int STALE_CLIENT = 30;
    private static final int UNAUTHENTICATED = 40;
    private static final int LIFECYCLE_BUSY = 88;
    private static final int REGISTER_RESPONSE_MIN_BYTES = 17;
    // Flags the optional username of an UpdateUser body.
    private static final int USERNAME_PRESENT = 1;
    private static final Logger log = LoggerFactory.getLogger(AsyncTcpConnection.class);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofMillis(3000);
    // A missing reply must not hold the single VSR-pinned channel forever.
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final long TRANSIENT_RETRY_INTERVAL_MS = 50;
    // Each lifecycle retry commits another refusal, so its pause doubles from
    // the transient interval up to this cap; mirrors LIFECYCLE_RETRY_MAX_INTERVAL
    // in core/sdk/src/vsr.rs.
    private static final Duration TRANSIENT_RETRY_BUDGET = Duration.ofSeconds(30);
    private static final Duration NOT_ACCEPTED_RETRY_BUDGET = Duration.ofSeconds(2);
    private static final String EVENT_LOOP_THREAD_PREFIX = "iggy-tcp-io";

    private final IoEventLoopGroup eventLoopGroup;
    private final boolean ownsEventLoopGroup;
    private final EventLoop eventLoop;
    private final FixedChannelPool channelPool;
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE, true);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong authGeneration = new AtomicLong(0);
    private final VsrRequestEncoder vsrEncoder;
    private final ConsensusSession consensusSession;
    private volatile boolean sharedSession;
    private final TransientFailoverHandler transientFailoverHandler;
    private final IntConsumer sessionResetListener;
    private final BiConsumer<AsyncTcpConnection, Throwable> connectionFailureListener;
    private final long requestTimeoutNanos;
    private final long heartbeatIntervalNanos;
    private final Object heartbeatLock = new Object();
    // Shared with the connections that bind this session, see bindSharedSession.
    private volatile SessionLogin login = new SessionLogin();
    private ScheduledFuture<?> heartbeatTask;
    private boolean heartbeatRunning;
    private volatile Runnable heartbeatListener = () -> {};
    private volatile CredentialsListener credentialsListener = (source, previousUsername, username, password) -> {};

    private volatile boolean authenticated = false;
    private volatile long authenticatedUserId;

    public AsyncTcpConnection(
            String host,
            int port,
            boolean enableTls,
            Optional<File> tlsCertificate,
            TcpConnectionPoolConfig poolConfig,
            Optional<Duration> connectionTimeout) {
        this(
                host,
                port,
                enableTls,
                tlsCertificate,
                poolConfig,
                Optional.empty(),
                DEFAULT_IO_THREADS,
                connectionTimeout,
                Optional.empty(),
                Duration.ofSeconds(5),
                VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                null,
                errorCode -> {},
                (source, cause) -> {});
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    AsyncTcpConnection(
            String host,
            int port,
            boolean enableTls,
            Optional<File> tlsCertificate,
            TcpConnectionPoolConfig poolConfig,
            Optional<IoEventLoopGroup> sharedEventLoopGroup,
            int ioThreads,
            Optional<Duration> connectionTimeout,
            Optional<Duration> requestTimeout,
            Duration heartbeatInterval,
            int maxVsrFrameSize,
            TransientFailoverHandler transientFailoverHandler,
            IntConsumer sessionResetListener,
            BiConsumer<AsyncTcpConnection, Throwable> connectionFailureListener) {
        this.transientFailoverHandler = transientFailoverHandler;
        this.sessionResetListener = sessionResetListener;
        this.connectionFailureListener = connectionFailureListener;
        this.requestTimeoutNanos = toTimeoutNanos(requestTimeout.orElse(DEFAULT_REQUEST_TIMEOUT));
        this.heartbeatIntervalNanos = toTimeoutNanos(heartbeatInterval);
        SslContext sslContext = null;
        if (enableTls) {
            try {
                SslContextBuilder sslBuilder = SslContextBuilder.forClient();
                tlsCertificate.ifPresent(sslBuilder::trustManager);
                sslContext = sslBuilder.build();
            } catch (SSLException e) {
                throw new IggyTlsException("Failed to build SSL context for AsyncTcpConnection", e);
            }
        }

        this.consensusSession = new ConsensusSession();
        this.vsrEncoder = new VsrRequestEncoder(consensusSession);
        this.ownsEventLoopGroup = sharedEventLoopGroup.isEmpty();
        this.eventLoopGroup = sharedEventLoopGroup.orElseGet(() -> newEventLoopGroup(ioThreads));
        this.eventLoop = eventLoopGroup.next();

        long dialTimeoutMillis =
                connectionTimeout.orElse(DEFAULT_CONNECTION_TIMEOUT).toMillis();
        var bootstrap = new Bootstrap()
                .group(eventLoop)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) dialTimeoutMillis)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(host, port);

        // A replica link owns one multiplexed channel; the logical session
        // supplies request IDs across every replica link.
        this.channelPool = new FixedChannelPool(
                bootstrap,
                new PoolChannelHandler(
                        host,
                        port,
                        enableTls,
                        sslContext,
                        dialTimeoutMillis,
                        consensusSession,
                        maxVsrFrameSize,
                        this::onSessionEvicted,
                        channels::add),
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                poolConfig.getAcquireTimeoutMillis(),
                1,
                poolConfig.getMaxPendingAcquires());

        log.info("Connection pool initialized with one multiplexed replica channel");
    }

    /**
     * Validates server reachability by eagerly acquiring and releasing one connection.
     */
    public CompletableFuture<Void> connect() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        channelPool.acquire().addListener((FutureListener<Channel>) f -> {
            if (f.isSuccess()) {
                channelPool.release(f.getNow()).addListener(release -> {
                    if (release.isSuccess()) {
                        startHeartbeat();
                        future.complete(null);
                    } else {
                        future.completeExceptionally(release.cause());
                    }
                });
            } else {
                Throwable cause = f.cause();
                if (cause instanceof ConnectTimeoutException) {
                    future.completeExceptionally(new IggyConnectionException("Connection timeout", cause));
                } else {
                    future.completeExceptionally(cause);
                }
            }
        });
        return future;
    }

    private void startHeartbeat() {
        synchronized (heartbeatLock) {
            if (heartbeatRunning || isClosed.get()) {
                return;
            }
            heartbeatRunning = true;
            scheduleNextHeartbeat();
        }
    }

    private void scheduleNextHeartbeat() {
        synchronized (heartbeatLock) {
            if (!heartbeatRunning || isClosed.get()) {
                return;
            }
            try {
                heartbeatTask = eventLoop.schedule(this::sendHeartbeat, heartbeatIntervalNanos, TimeUnit.NANOSECONDS);
            } catch (RejectedExecutionException loopGone) {
                // Only a caller-owned group shuts down under a live connection.
                heartbeatRunning = false;
                log.warn("Event loop rejected the heartbeat, stopping it: {}", loopGone.getMessage());
            }
        }
    }

    private void sendHeartbeat() {
        synchronized (heartbeatLock) {
            heartbeatTask = null;
            if (!heartbeatRunning || isClosed.get()) {
                return;
            }
        }
        CompletableFuture<ByteBuf> heartbeat;
        try {
            TransientFailoverState state = new TransientFailoverState();
            state.heartbeat = true;
            heartbeat = send(CommandCode.System.PING.getValue(), Unpooled.EMPTY_BUFFER, 0, state);
        } catch (RuntimeException error) {
            log.warn("Failed to send heartbeat: {}", error.getMessage());
            scheduleNextHeartbeat();
            return;
        }
        heartbeat.whenComplete((response, error) -> {
            if (response != null) {
                response.release();
            }
            if (error != null && !isClosed.get()) {
                log.warn("Heartbeat failed: {}", error.getMessage());
            }
            scheduleNextHeartbeat();
            if (error == null) {
                heartbeatListener.run();
            }
        });
    }

    /** Runs after each heartbeat that the server answered. */
    void onHeartbeat(Runnable listener) {
        heartbeatListener = listener;
    }

    /**
     * Ends {@code signIn} while it still runs. On the event loop of this connection its channel closes first, so a
     * Register that waits for its reply frees the channel at once, and that late reply cannot answer the next
     * Register, which has the same request id. Requests that the channel still carries fail as not connected, not
     * as a lost link, so they start no redial.
     */
    void abortSignIn(CompletableFuture<?> signIn) {
        try {
            eventLoop.execute(() -> {
                if (signIn.isDone()) {
                    return;
                }
                IggyNotConnectedException replaced = new IggyNotConnectedException("A new sign-in replaced this one");
                for (Channel channel : channels) {
                    VsrResponseHandler handler = channel.pipeline().get(VsrResponseHandler.class);
                    if (handler != null) {
                        handler.closeChannel(channel, replaced);
                    } else {
                        channel.close();
                    }
                }
                signIn.cancel(false);
            });
        } catch (RejectedExecutionException loopGone) {
            signIn.cancel(false);
        }
    }

    /** Runs after a committed password change or rename of the signed-in user replaced the login of this connection. */
    void onCredentialsRefreshed(CredentialsListener listener) {
        credentialsListener = listener;
    }

    private void stopHeartbeat() {
        synchronized (heartbeatLock) {
            heartbeatRunning = false;
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }
        }
    }

    boolean heartbeatScheduled() {
        synchronized (heartbeatLock) {
            return heartbeatTask != null;
        }
    }

    EventLoop eventLoop() {
        return eventLoop;
    }

    IoEventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    static IoEventLoopGroup newEventLoopGroup(int ioThreads) {
        return new MultiThreadIoEventLoopGroup(
                ioThreads,
                new DefaultThreadFactory(EVENT_LOOP_THREAD_PREFIX, false, Thread.MAX_PRIORITY),
                NioIoHandler.newFactory());
    }

    void followSession(ConsensusSession owner) {
        consensusSession.follow(owner);
    }

    ConsensusSession sessionOwner() {
        return consensusSession.owner();
    }

    long metadataWatermark() {
        return consensusSession.metadataWatermark();
    }

    long[] sessionIdentity() {
        return consensusSession.identity();
    }

    byte[] bindSecret() {
        return consensusSession.bindSecret();
    }

    byte[] bindSecret(long clientLow, long clientHigh, long epoch) {
        return consensusSession.bindSecret(clientLow, clientHigh, epoch);
    }

    long sessionGeneration() {
        return consensusSession.generation();
    }

    void bindSharedSession(AsyncTcpConnection parent, long clientLow, long clientHigh, long epoch, byte[] secret) {
        consensusSession.bindShared(parent.consensusSession, clientLow, clientHigh, epoch, secret);
        sharedSession = true;
        login = parent.login;
        parent.authenticationSnapshot().ifPresent(authentication -> {
            authentication.payload().release();
            authenticated = true;
            authenticatedUserId = parent.authenticatedUserId;
        });
    }

    boolean isAuthenticated() {
        return authenticated;
    }

    public <T> CompletableFuture<T> exchangeForEntity(
            CommandCode commandCode, ByteBuf payload, Function<ByteBuf, T> func) {
        return mapResponse(send(commandCode, payload), response -> {
            try {
                if (!response.isReadable()) {
                    throw new IggyEmptyResponseException(commandCode.toString());
                }
                return func.apply(response);
            } finally {
                response.release();
            }
        });
    }

    public <T> CompletableFuture<List<T>> exchangeForList(
            CommandCode commandCode, ByteBuf payload, Function<ByteBuf, T> func) {
        return mapResponse(send(commandCode, payload), response -> {
            try {
                var result = new ArrayList<T>();
                while (response.isReadable()) {
                    result.add(func.apply(response));
                }
                return result;
            } finally {
                response.release();
            }
        });
    }

    public <T> CompletableFuture<Optional<T>> exchangeForOptional(
            CommandCode commandCode, ByteBuf payload, Function<ByteBuf, T> func) {
        return mapResponse(send(commandCode, payload), response -> {
            try {
                if (response.isReadable()) {
                    return Optional.of(func.apply(response));
                }
                return Optional.empty();
            } finally {
                response.release();
            }
        });
    }

    public CompletableFuture<Void> sendAndRelease(CommandCode commandCode, ByteBuf payload) {
        return mapResponse(send(commandCode, payload), response -> {
            response.release();
            return null;
        });
    }

    static <T> CompletableFuture<T> mapResponse(CompletableFuture<ByteBuf> input, Function<ByteBuf, T> decode) {
        CompletableFuture<T> result = new CompletableFuture<>();
        result.whenComplete((response, error) -> {
            if (result.isCancelled()) {
                input.cancel(false);
            }
        });
        input.whenComplete((response, error) -> {
            if (error != null) {
                result.completeExceptionally(error);
            } else if (result.isDone()) {
                response.release();
            } else {
                try {
                    result.complete(decode.apply(response));
                } catch (RuntimeException decodeError) {
                    result.completeExceptionally(decodeError);
                }
            }
        });
        return result;
    }

    public CompletableFuture<ByteBuf> send(CommandCode commandCode, ByteBuf payload) {
        return send(commandCode.getValue(), payload);
    }

    public CompletableFuture<ByteBuf> send(int commandCode, ByteBuf payload) {
        return send(commandCode, payload, 0);
    }

    CompletableFuture<ByteBuf> send(int commandCode, ByteBuf payload, long requestDeadlineNanos) {
        return send(commandCode, payload, requestDeadlineNanos, new TransientFailoverState());
    }

    CompletableFuture<ByteBuf> send(
            int commandCode, ByteBuf payload, long requestDeadlineNanos, TransientFailoverState failoverState) {
        return send(commandCode, payload, requestDeadlineNanos, failoverState, 0);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private CompletableFuture<ByteBuf> send(
            int commandCode,
            ByteBuf payload,
            long requestDeadlineNanos,
            TransientFailoverState failoverState,
            long requiredSessionGeneration) {
        failoverState.linkFailed = false;
        failoverState.detachedSession = false;
        long deadline = requestDeadlineNanos == 0 ? System.nanoTime() + requestTimeoutNanos : requestDeadlineNanos;
        if (isLoginCode(commandCode) && authenticated) {
            return logoutThenLogin(commandCode, payload, deadline);
        }
        captureLoginPayloadIfNeeded(commandCode, payload);
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();
        CompletableFuture<ByteBuf> callerFuture = new CompletableFuture<>();
        ByteBuf failoverPayload = transientFailoverHandler != null
                        && !failoverState.routed
                        && !isLoginCode(commandCode)
                        && !isPollRoutingCode(commandCode)
                ? payload.retainedDuplicate()
                : null;

        var acquiring = channelPool.acquire();
        var acquisitionTimeout = eventLoop.schedule(
                () -> {
                    Throwable failure = failoverState.ticket != null && failoverState.uncertain
                            ? failoverState.retainTicket()
                            : responseTimeout(commandCode);
                    callerFuture.completeExceptionally(failure);
                    acquiring.cancel(false);
                },
                Math.max(0, deadline - System.nanoTime()),
                TimeUnit.NANOSECONDS);
        callerFuture.whenComplete((response, error) -> {
            if (callerFuture.isCancelled()) {
                acquiring.cancel(false);
            }
        });
        acquiring.addListener((FutureListener<Channel>) f -> {
            acquisitionTimeout.cancel(false);
            if (!f.isSuccess()) {
                payload.release();
                releaseIfPresent(failoverPayload);
                notifyConnectionFailure(f.cause());
                callerFuture.completeExceptionally(mapAcquireException(f.cause()));
                return;
            }
            if (callerFuture.isDone()) {
                payload.release();
                releaseIfPresent(failoverPayload);
                releaseChannel(f.getNow());
                return;
            }
            dispatchAcquiredChannel(
                    f.getNow(),
                    commandCode,
                    payload,
                    failoverPayload,
                    responseFuture,
                    callerFuture,
                    deadline,
                    failoverState,
                    requiredSessionGeneration);
        });

        return callerFuture;
    }

    CompletableFuture<ByteBuf> sendRouted(
            int commandCode, ByteBuf payload, long sessionGeneration, long deadline, TransientFailoverState state) {
        state.routed = true;
        return send(commandCode, payload, deadline, state, sessionGeneration);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void dispatchAcquiredChannel(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            ByteBuf failoverPayload,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long requestDeadlineNanos,
            TransientFailoverState failoverState,
            long requiredSessionGeneration) {
        Runnable dispatch = () -> dispatchOnChannel(
                channel,
                commandCode,
                payload,
                failoverPayload,
                responseFuture,
                callerFuture,
                requestDeadlineNanos,
                failoverState,
                requiredSessionGeneration);
        if (channel.eventLoop().inEventLoop()) {
            dispatch.run();
            return;
        }
        try {
            channel.eventLoop().execute(dispatch);
        } catch (RejectedExecutionException error) {
            payload.release();
            releaseIfPresent(failoverPayload);
            releaseChannel(channel);
            callerFuture.completeExceptionally(error);
        }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void dispatchOnChannel(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            ByteBuf failoverPayload,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long inheritedRequestDeadlineNanos,
            TransientFailoverState failoverState,
            long requiredSessionGeneration) {
        boolean isLoginCommand = isLoginCode(commandCode);
        boolean holdLeaseUntilResponse = mutatesSessionState(commandCode);
        long requestDeadlineNanos = inheritedRequestDeadlineNanos == 0
                ? System.nanoTime() + requestTimeoutNanos
                : inheritedRequestDeadlineNanos;

        responseFuture.whenComplete((response, error) -> completeResponse(
                channel,
                commandCode,
                isLoginCommand,
                failoverPayload,
                requestDeadlineNanos,
                holdLeaseUntilResponse,
                callerFuture,
                failoverState,
                response,
                error));
        authenticationStep(channel, commandCode, requestDeadlineNanos)
                .whenComplete((ignored, authError) -> completeAuthenticationStep(
                        channel,
                        commandCode,
                        payload,
                        responseFuture,
                        callerFuture,
                        requestDeadlineNanos,
                        holdLeaseUntilResponse,
                        requiredSessionGeneration,
                        failoverState,
                        authError));
    }

    private CompletableFuture<Void> authenticationStep(Channel channel, int commandCode, long requestDeadlineNanos) {
        if (isLoginCode(commandCode) || !requiresAuthentication(commandCode)) {
            return CompletableFuture.completedFuture(null);
        }
        Optional<AuthenticationSnapshot> authentication = authenticationSnapshot();
        if (authentication.isEmpty()) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        int loginCode = authentication.get().commandCode();
        return IggyAuthenticator.ensureAuthenticated(
                channel,
                authentication.get().payload(),
                authGeneration,
                payloadToLogin -> sendAuthenticationFrame(channel, payloadToLogin, loginCode, requestDeadlineNanos));
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void completeAuthenticationStep(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long requestDeadlineNanos,
            boolean holdLeaseUntilResponse,
            long requiredSessionGeneration,
            TransientFailoverState failoverState,
            Throwable authError) {
        try {
            if (authError != null || callerFuture.isDone()) {
                payload.release();
                responseFuture.completeExceptionally(authError == null ? responseTimeout(commandCode) : authError);
                return;
            }
            if (requiredSessionGeneration != 0 && requiredSessionGeneration != sessionGeneration()) {
                // A replacement channel or eviction reset invalidates the parent attachment.
                payload.release();
                responseFuture.completeExceptionally(
                        IggyServerException.fromTcpResponse(TRANSIENT_NOT_ACCEPTED, new byte[0]));
                return;
            }
            sendFrame(channel, payload, commandCode, responseFuture, callerFuture, requestDeadlineNanos, failoverState);
        } finally {
            if (!holdLeaseUntilResponse) {
                releaseChannel(channel);
            }
        }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void completeResponse(
            Channel channel,
            int commandCode,
            boolean isLoginCommand,
            ByteBuf failoverPayload,
            long requestDeadlineNanos,
            boolean holdLeaseUntilResponse,
            CompletableFuture<ByteBuf> callerFuture,
            TransientFailoverState failoverState,
            ByteBuf response,
            Throwable error) {
        try {
            completeRequest(
                    channel,
                    commandCode,
                    isLoginCommand,
                    failoverPayload,
                    requestDeadlineNanos,
                    callerFuture,
                    failoverState,
                    response,
                    error);
        } finally {
            if (holdLeaseUntilResponse) {
                releaseChannel(channel);
            }
        }
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void completeRequest(
            Channel channel,
            int commandCode,
            boolean isLoginCommand,
            ByteBuf failoverPayload,
            long requestDeadlineNanos,
            CompletableFuture<ByteBuf> callerFuture,
            TransientFailoverState failoverState,
            ByteBuf response,
            Throwable error) {
        try {
            handlePostResponse(channel, commandCode, isLoginCommand, response, error);
        } catch (RuntimeException bookkeepingError) {
            releaseIfPresent(response);
            releaseIfPresent(failoverPayload);
            callerFuture.completeExceptionally(bookkeepingError);
            return;
        }
        if (error == null) {
            releaseIfPresent(failoverPayload);
            completeWithResponse(callerFuture, response);
            return;
        }
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        if (failoverState.heartbeat && cause instanceof IggyTimeoutException) {
            channel.close();
            notifyConnectionFailure(new IggyConnectionException("The heartbeat reply timed out", cause));
        }
        completeFailedRequest(commandCode, failoverPayload, requestDeadlineNanos, callerFuture, failoverState, error);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void completeFailedRequest(
            int commandCode,
            ByteBuf failoverPayload,
            long requestDeadlineNanos,
            CompletableFuture<ByteBuf> callerFuture,
            TransientFailoverState failoverState,
            Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        if (cause instanceof IggyConnectionException || cause instanceof java.io.IOException) {
            notifyConnectionFailure(cause);
        }
        if (failoverState.routed) {
            releaseIfPresent(failoverPayload);
            callerFuture.completeExceptionally(failoverState.linkFailed ? new LinkFailure(error) : error);
            return;
        }
        IggyServerException serverError = findServerError(error);
        if (canReplayLostLink(failoverState, cause, failoverPayload, requestDeadlineNanos)
                || shouldRecheckLeader(serverError, failoverPayload, requestDeadlineNanos)) {
            retryAfterLeaderRecheck(
                    commandCode, failoverPayload, requestDeadlineNanos, cause, callerFuture, failoverState);
            return;
        }
        releaseIfPresent(failoverPayload);
        if (failoverState.ticket != null) {
            if (failoverState.uncertain || callerFuture.isCancelled()) {
                error = failoverState.retainTicket();
            } else {
                failoverState.ticket.complete();
                failoverState.ticket = null;
            }
        }
        callerFuture.completeExceptionally(error);
    }

    private static boolean canReplayLostLink(
            TransientFailoverState state, Throwable error, ByteBuf payload, long deadline) {
        return state.uncertain && lostLink(state, error) && payload != null && deadline - System.nanoTime() > 0;
    }

    /**
     * Whether the request lost the link that carried it. A detached-session
     * eviction counts too: the server can drop the link but keep the session,
     * so an attempt it admitted before can still commit. A reply with that
     * code answered the attempt, so it is no lost link.
     */
    private static boolean lostLink(TransientFailoverState state, Throwable error) {
        return error instanceof IggyConnectionException
                || error instanceof java.io.IOException
                || (state.linkFailed && state.detachedSession);
    }

    private static boolean shouldRecheckLeader(
            IggyServerException serverError, ByteBuf failoverPayload, long requestDeadlineNanos) {
        return serverError != null
                && (serverError.getRawErrorCode() == TRANSIENT_NOT_ACCEPTED
                        || serverError.getRawErrorCode() == NOT_PRIMARY)
                && failoverPayload != null
                && requestDeadlineNanos - System.nanoTime() > 0;
    }

    private void retryAfterLeaderRecheck(
            int commandCode,
            ByteBuf payload,
            long requestDeadlineNanos,
            Throwable rejection,
            CompletableFuture<ByteBuf> callerFuture,
            TransientFailoverState failoverState) {
        CompletableFuture<ByteBuf> retry;
        try {
            retry = transientFailoverHandler.retry(
                    this, commandCode, payload, requestDeadlineNanos, rejection, failoverState);
        } catch (RuntimeException retryError) {
            payload.release();
            completeFailedRequest(commandCode, null, requestDeadlineNanos, callerFuture, failoverState, retryError);
            return;
        }
        callerFuture.whenComplete((response, error) -> {
            if (callerFuture.isCancelled()) {
                retry.cancel(false);
            }
        });
        retry.whenComplete((response, error) -> {
            if (error == null) {
                completeWithResponse(callerFuture, response);
            } else {
                completeFailedRequest(commandCode, null, requestDeadlineNanos, callerFuture, failoverState, error);
            }
        });
    }

    private CompletableFuture<ByteBuf> sendAuthenticationFrame(
            Channel channel, ByteBuf payload, int commandCode, long requestDeadlineNanos) {
        if (consensusSession.isBound()) {
            long[] identity = consensusSession.identity();
            long expectedSession = identity[2];
            ByteBuf binding = vsrEncoder.bindSession(channel.alloc());
            CompletableFuture<ByteBuf> bindingFuture = new CompletableFuture<>();
            TransientFailoverState bindState = new TransientFailoverState();
            sendFrame(
                    channel,
                    binding,
                    CommandCode.System.BIND_SESSION.getValue(),
                    bindingFuture,
                    bindingFuture,
                    requestDeadlineNanos,
                    bindState);
            return bindingFuture
                    .handle((response, error) -> {
                        if (error == null) {
                            payload.release();
                            if (response.readableBytes() < REGISTER_RESPONSE_MIN_BYTES
                                    || response.getLongLE(response.readerIndex() + Integer.BYTES) != expectedSession) {
                                response.release();
                                throw new IggyNotConnectedException("BindSession returned a different session epoch");
                            }
                            return CompletableFuture.completedFuture(response);
                        }
                        IggyServerException serverError = findServerError(error);
                        if (serverError != null) {
                            // A stale client eviction drops the link and keeps the session, so the request
                            // binds it again on a new link.
                            if (serverError.getRawErrorCode() == STALE_CLIENT && lostLink(bindState, serverError)) {
                                payload.release();
                                return CompletableFuture.<ByteBuf>failedFuture(new IggyConnectionException(
                                        "The server dropped the link during BindSession", serverError));
                            }
                            if (serverError.getRawErrorCode() == UNAUTHENTICATED
                                    || serverError.getRawErrorCode() == STALE_CLIENT) {
                                resetRefusedSession(identity[0], identity[1], serverError.getRawErrorCode());
                                return sendAuthenticationFrame(channel, payload, commandCode, requestDeadlineNanos);
                            }
                        }
                        payload.release();
                        return CompletableFuture.<ByteBuf>failedFuture(error);
                    })
                    .thenCompose(Function.identity());
        }
        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        sendFrame(
                channel,
                payload,
                commandCode,
                loginFuture,
                loginFuture,
                requestDeadlineNanos,
                new TransientFailoverState());
        return loginFuture;
    }

    /**
     * Pool acquire failures and expired replies both make the current target
     * unusable. The listener lets the owning client run its redial strategy
     * while the failed request surfaces to its caller.
     */
    private void notifyConnectionFailure(Throwable cause) {
        try {
            connectionFailureListener.accept(this, cause);
        } catch (RuntimeException listenerError) {
            log.warn("Connection failure listener threw: {}", listenerError.getMessage());
        }
    }

    private static Throwable mapAcquireException(Throwable cause) {
        if (cause instanceof IllegalStateException) {
            return new IggyNotConnectedException("Connection pool is closed");
        }
        if (cause instanceof TimeoutException) {
            return new IggyTimeoutException("Timed out acquiring a connection from the pool", cause);
        }
        return cause;
    }

    /**
     * An already-bound connection replays its original Register response.
     * Logout first so explicit login can establish the requested identity.
     */
    private CompletableFuture<ByteBuf> logoutThenLogin(int commandCode, ByteBuf payload, long deadline) {
        return send(CommandCode.User.LOGOUT.getValue(), Unpooled.EMPTY_BUFFER, deadline)
                .handle((logoutResponse, logoutError) -> {
                    if (logoutError != null) {
                        payload.release();
                        throw new CompletionException(logoutError);
                    }
                    logoutResponse.release();
                    return null;
                })
                .thenCompose(ignored -> send(commandCode, payload, deadline));
    }

    private static boolean isLoginCode(int commandCode) {
        return commandCode == CommandCode.User.LOGIN.getValue()
                || commandCode == CommandCode.PersonalAccessToken.LOGIN.getValue();
    }

    private static boolean isPollRoutingCode(int commandCode) {
        return commandCode == CommandCode.System.GET_CLUSTER_METADATA.getValue()
                || commandCode == CommandCode.System.BIND_SESSION.getValue()
                || commandCode == CommandCode.Messages.GET_PARTITION_ROUTE.getValue();
    }

    private boolean isRoutedExchange(int commandCode) {
        return isPollRoutingCode(commandCode) || (sharedSession && isPartitionCommand(commandCode));
    }

    private static boolean isPartitionCommand(int code) {
        return code == CommandCode.Messages.SEND.getValue()
                || code == CommandCode.Messages.POLL.getValue()
                || code == CommandCode.ConsumerOffset.GET.getValue()
                || isOffsetWriteCode(code);
    }

    private static boolean mutatesSessionState(int commandCode) {
        return isLoginCode(commandCode) || commandCode == CommandCode.User.LOGOUT.getValue();
    }

    /**
     * BindSession authenticates with the parent's proof. Primary polls and offset
     * writes use that binding, so none of them sends a password login on auxiliary
     * channels.
     */
    private boolean requiresAuthentication(int commandCode) {
        return !isAllowedBeforeAuthentication(commandCode);
    }

    private boolean isAllowedBeforeAuthentication(int commandCode) {
        return commandCode == CommandCode.System.PING.getValue()
                || (sharedSession
                        && (commandCode == CommandCode.System.BIND_SESSION.getValue()
                                || isPartitionCommand(commandCode)));
    }

    private static boolean isOffsetWriteCode(int commandCode) {
        return commandCode == CommandCode.ConsumerOffset.STORE.getValue()
                || commandCode == CommandCode.ConsumerOffset.DELETE.getValue();
    }

    /**
     * Writes the first attempt of a request. {@code callerFuture} is the future its caller holds,
     * and an authentication step holds the response future itself.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void sendFrame(
            Channel channel,
            ByteBuf payload,
            int commandCode,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long deadline,
            TransientFailoverState state) {
        int operation = VsrOperation.operationForCode(commandCode);
        boolean mutation = operation != VsrOperation.NON_REPLICATED && !mutatesSessionState(commandCode);
        if (state.ticket == null && (mutation || state.lane != null)) {
            ConsensusSession.LaneKey lane = state.lane == null ? ConsensusSession.LaneKey.METADATA : state.lane;
            CompletableFuture<ConsensusSession.Ticket> admission = consensusSession.acquire(
                    lane,
                    mutation,
                    (long) VsrHeaders.HEADER_SIZE + payload.readableBytes(),
                    deadline,
                    channel.eventLoop());
            callerFuture.whenComplete((response, error) -> {
                if (callerFuture.isCancelled()) {
                    admission.cancel(false);
                }
            });
            admission.whenComplete((ticket, error) -> {
                if (error != null || callerFuture.isDone()) {
                    payload.release();
                    if (ticket != null) {
                        ticket.complete();
                    }
                    responseFuture.completeExceptionally(error == null ? responseTimeout(commandCode) : error);
                    return;
                }
                state.ticket = ticket;
                sendAdmittedFrame(channel, payload, commandCode, responseFuture, callerFuture, deadline, state);
            });
            return;
        }
        sendAdmittedFrame(channel, payload, commandCode, responseFuture, callerFuture, deadline, state);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void sendAdmittedFrame(
            Channel channel,
            ByteBuf payload,
            int commandCode,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long requestDeadlineNanos,
            TransientFailoverState failoverState) {
        try {
            VsrResponseHandler handler = channel.pipeline().get(VsrResponseHandler.class);
            if (handler == null) {
                throw new IggyClientException("Channel missing VsrResponseHandler");
            }

            ByteBuf frame;
            if (failoverState.ticket != null && failoverState.ticket.hasFrame()) {
                frame = failoverState.ticket.frame();
                try {
                    if (failoverState.requestId == 0 && !failoverState.ticket.renewRequestId(frame)) {
                        throw new IggyServerException(TRANSIENT_NOT_ACCEPTED);
                    }
                } catch (RuntimeException error) {
                    frame.release();
                    failoverState.ticket.finishWrite();
                    throw error;
                }
            } else {
                frame = vsrEncoder.encode(
                        channel.alloc(), commandCode, payload, failoverState.context, failoverState.requestId);
                if (failoverState.ticket != null) {
                    try {
                        failoverState.ticket.freeze(frame, commandCode, failoverState.routingBody);
                    } catch (RuntimeException error) {
                        frame.release();
                        throw error;
                    }
                }
            }
            failoverState.requestId = VsrHeaders.readRequestId(frame);
            long nowNanos = System.nanoTime();
            long deadlineNanos =
                    requestDeadlineNanos == 0 ? nowNanos + TRANSIENT_RETRY_BUDGET.toNanos() : requestDeadlineNanos;
            long notAcceptedDeadlineNanos =
                    isLoginCode(commandCode) ? deadlineNanos : nowNanos + NOT_ACCEPTED_RETRY_BUDGET.toNanos();
            writeVsrFrame(
                    new VsrExchange(
                            channel,
                            handler,
                            responseFuture,
                            callerFuture,
                            commandCode,
                            failoverState,
                            requestDeadlineNanos,
                            deadlineNanos,
                            notAcceptedDeadlineNanos),
                    frame,
                    false,
                    TRANSIENT_RETRY_INTERVAL_MS);
        } catch (RuntimeException e) {
            responseFuture.completeExceptionally(e);
        } finally {
            payload.release();
        }
    }

    /**
     * One VSR write attempt. A transient denial (the cluster could not commit
     * or accept yet) replays the SAME encoded frame so the server's dedup
     * sees the same request id. A lifecycle refusal was committed under that
     * id, so it is retried as a new request; everything else resolves the
     * caller.
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    private void writeVsrFrame(VsrExchange exchange, ByteBuf frame, boolean uncertain, long lifecycleRetryIntervalMs) {
        Channel channel = exchange.channel();
        VsrResponseHandler handler = exchange.handler();
        CompletableFuture<ByteBuf> responseFuture = exchange.responseFuture();
        if (exchange.requestDeadlineNanos() - System.nanoTime() <= 0) {
            IggyTimeoutException timeout = responseTimeout(exchange.commandCode());
            releaseFrame(exchange, frame);
            responseFuture.completeExceptionally(preserveUncertainOutcome(timeout, uncertain));
            return;
        }
        CompletableFuture<ByteBuf> attempt = new CompletableFuture<>();
        VsrResponseHandler.PendingRequest pending;
        try {
            pending = handler.registerRequest(
                    channel, frame, attempt, exchange.requestDeadlineNanos(), exchange.commandCode());
        } catch (RuntimeException error) {
            releaseFrame(exchange, frame);
            responseFuture.completeExceptionally(error);
            return;
        }
        attempt.whenComplete((response, error) ->
                completeAttempt(exchange, frame, uncertain, pending, lifecycleRetryIntervalMs, response, error));
        exchange.callerFuture().whenComplete((response, error) -> {
            if (exchange.callerFuture().isCancelled()) {
                attempt.cancel(false);
            }
        });
        if (attempt.isDone()) {
            return;
        }
        ConsensusSession.Ticket writerTicket = exchange.failoverState().ticket;
        if (writerTicket != null) {
            try {
                writerTicket.startWrite();
            } catch (RuntimeException error) {
                attempt.completeExceptionally(error);
                return;
            }
        }
        channel.writeAndFlush(frame.retainedDuplicate()).addListener((ChannelFutureListener) future -> {
            if (writerTicket != null) {
                writerTicket.finishWrite();
            }
            if (!future.isSuccess()) {
                log.error("Failed to send frame: {}", future.cause().getMessage());
                // A failed write leaves framing undefined. Closing removes and
                // fails every pending entry before the channel can be reused.
                handler.closeChannel(channel, future.cause());
            }
        });
    }

    private static void releaseFrame(VsrExchange exchange, ByteBuf frame) {
        frame.release();
        if (exchange.failoverState().ticket != null) {
            exchange.failoverState().ticket.finishWrite();
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void completeAttempt(
            VsrExchange exchange,
            ByteBuf frame,
            boolean uncertain,
            VsrResponseHandler.PendingRequest pending,
            long lifecycleRetryIntervalMs,
            ByteBuf response,
            Throwable error) {
        Channel channel = exchange.channel();
        IggyServerException serverError = findServerError(error);
        // A cancel can end an attempt after its reply was marked committed, and then nothing holds the outcome.
        // The operation stays uncertain, as in Rust, so a resolve can still prove it.
        boolean committed = pending.isCommitted() && (error == null || serverError != null);
        exchange.failoverState().committed = committed;
        exchange.failoverState().linkFailed = pending.isFailedByLink();
        exchange.failoverState().detachedSession = pending.isSessionDetached();
        closeIfDetached(exchange, pending, serverError);
        boolean wasUncertain = uncertain || exchange.failoverState().uncertain;
        // An eviction fails every request on its link and names none of them, so the server can have admitted
        // this one, unlike a reply that refuses it with the same code.
        boolean nextUncertain = !committed
                && (wasUncertain
                        || isUncertainOutcome(serverError)
                        || (error != null
                                && (serverError == null || pending.isFailedByLink())
                                && exchange.failoverState().ticket != null));
        exchange.failoverState().uncertain = nextUncertain;
        exchange.failoverState().everUncertain |= wasUncertain || nextUncertain;
        if (shouldRetryTransient(
                        exchange.commandCode(),
                        error,
                        exchange.deadlineNanos(),
                        exchange.notAcceptedDeadlineNanos(),
                        exchange.failoverState().routed)
                && channel.isActive()) {
            try {
                channel.eventLoop()
                        .schedule(
                                () -> writeVsrFrame(exchange, frame, nextUncertain, lifecycleRetryIntervalMs),
                                TRANSIENT_RETRY_INTERVAL_MS,
                                TimeUnit.MILLISECONDS)
                        .addListener(retry -> {
                            if (retry.isCancelled()) {
                                releaseFrame(exchange, frame);
                                exchange.responseFuture()
                                        .completeExceptionally(preserveUncertainOutcome(error, nextUncertain));
                            }
                        });
                return;
            } catch (RejectedExecutionException retryRejected) {
                log.warn("Event loop rejected a VSR retry, failing the request: {}", retryRejected.getMessage());
            }
        }
        if (!wasUncertain
                && !exchange.failoverState().routed
                && scheduleLifecycleRetry(exchange, frame, serverError, nextUncertain, lifecycleRetryIntervalMs)) {
            return;
        }
        if (error == null && !nextUncertain) {
            refreshCredentials(exchange.commandCode(), frame);
        }
        releaseFrame(exchange, frame);
        ConsensusSession.Ticket ticket = exchange.failoverState().ticket;
        if (ticket != null && nextUncertain) {
            releaseIfPresent(response);
            exchange.responseFuture()
                    .completeExceptionally(
                            error != null ? error : exchange.failoverState().retainTicket());
        } else {
            // Recorded while the reply is alive: a routed replay completes its ticket after the callbacks of its
            // caller, which release the reply.
            if (ticket != null && committed) {
                ticket.recordOutcome(response, serverError);
            }
            if (ticket != null && !exchange.failoverState().routed && (committed || error == null)) {
                ticket.complete();
                exchange.failoverState().ticket = null;
            }
            if (error != null) {
                exchange.responseFuture().completeExceptionally(preserveUncertainOutcome(error, nextUncertain));
            } else {
                completeWithResponse(exchange.responseFuture(), response);
            }
        }
    }

    /**
     * A reply with a stale client means that the server no longer attaches
     * the link to the session, for example because the session ended. The
     * link closes before the caller sees the reply, so the next request binds
     * again on a new link. That bind resumes a session the server still knows
     * and is refused for an ended one, which signs in again. A refused bind
     * already resets the session on its own link.
     */
    private static void closeIfDetached(
            VsrExchange exchange, VsrResponseHandler.PendingRequest pending, IggyServerException serverError) {
        if (serverError != null
                && serverError.getRawErrorCode() == STALE_CLIENT
                && !pending.isFailedByLink()
                && exchange.commandCode() != CommandCode.System.BIND_SESSION.getValue()) {
            exchange.handler()
                    .closeChannel(
                            exchange.channel(),
                            new IggyConnectionException("The server detached this connection from its session"));
        }
    }

    /**
     * Only ordinary requests retry a lifecycle refusal, and only when the next
     * attempt starts before the request deadline, as in Rust's TCP client. Routed
     * exchanges hand it to their router. Sign-in and sign-out hold the
     * channel lease until answered, so a pause there would stall every other
     * request; the server never refuses them for a lifecycle change.
     */
    private boolean scheduleLifecycleRetry(
            VsrExchange exchange,
            ByteBuf refusedFrame,
            IggyServerException refusal,
            boolean uncertain,
            long lifecycleRetryIntervalMs) {
        int commandCode = exchange.commandCode();
        if (uncertain
                || refusal == null
                || refusal.getRawErrorCode() != LIFECYCLE_BUSY
                || isRoutedExchange(commandCode)
                || mutatesSessionState(commandCode)
                || exchange.requestDeadlineNanos() - System.nanoTime()
                        <= TimeUnit.MILLISECONDS.toNanos(lifecycleRetryIntervalMs)
                || !exchange.channel().isActive()) {
            return false;
        }
        try {
            exchange.channel()
                    .eventLoop()
                    .schedule(
                            () -> retryLifecycleBusy(
                                    exchange, refusedFrame, refusal, uncertain, lifecycleRetryIntervalMs),
                            lifecycleRetryIntervalMs,
                            TimeUnit.MILLISECONDS)
                    .addListener(retry -> {
                        // Shutting down the event loop drops the pending retry unrun.
                        if (retry.isCancelled()) {
                            releaseFrame(exchange, refusedFrame);
                            exchange.responseFuture().completeExceptionally(refusal);
                        }
                    });
            return true;
        } catch (RejectedExecutionException retryRejected) {
            log.warn("Event loop rejected a lifecycle retry, failing the request: {}", retryRejected.getMessage());
            return false;
        }
    }

    /**
     * Sends the refused request again under a fresh request id with the same
     * payload and context, because the server's dedup answers the old id with
     * its committed refusal. The frame is encoded after the pause so its id
     * stays in order with the requests other callers sent meanwhile.
     */
    private void retryLifecycleBusy(
            VsrExchange exchange,
            ByteBuf refusedFrame,
            IggyServerException refusal,
            boolean uncertain,
            long lifecycleRetryIntervalMs) {
        Channel channel = exchange.channel();
        if (exchange.requestDeadlineNanos() - System.nanoTime() <= 0
                || !channel.isActive()
                || exchange.callerFuture().isDone()) {
            releaseFrame(exchange, refusedFrame);
            exchange.responseFuture().completeExceptionally(refusal);
            return;
        }
        ByteBuf nextFrame = refusedFrame;
        try {
            ConsensusSession.Ticket ticket = exchange.failoverState().ticket;
            if (ticket == null) {
                ByteBuf body = refusedFrame.slice(
                        refusedFrame.readerIndex() + VsrHeaders.HEADER_SIZE,
                        refusedFrame.readableBytes() - VsrHeaders.HEADER_SIZE);
                nextFrame = vsrEncoder.encode(
                        channel.alloc(),
                        exchange.commandCode(),
                        body,
                        exchange.failoverState().context());
                refusedFrame.release();
            } else if (!ticket.renewRequestId(refusedFrame)) {
                channel.eventLoop()
                        .schedule(
                                () -> retryLifecycleBusy(
                                        exchange, refusedFrame, refusal, uncertain, lifecycleRetryIntervalMs),
                                TRANSIENT_RETRY_INTERVAL_MS,
                                TimeUnit.MILLISECONDS)
                        .addListener(retry -> {
                            if (retry.isCancelled()) {
                                releaseFrame(exchange, refusedFrame);
                                exchange.responseFuture().completeExceptionally(refusal);
                            }
                        });
                return;
            }
            exchange.failoverState().requestId = VsrHeaders.readRequestId(nextFrame);
            exchange.failoverState().committed = false;
            writeVsrFrame(
                    exchange,
                    nextFrame,
                    false,
                    Math.min(lifecycleRetryIntervalMs * 2, LIFECYCLE_RETRY_MAX_INTERVAL_MS));
        } catch (RuntimeException error) {
            releaseFrame(exchange, nextFrame);
            exchange.responseFuture().completeExceptionally(error);
        }
    }

    private static boolean isUncertainOutcome(IggyServerException error) {
        return error != null && error.getRawErrorCode() == TRANSIENT_NOT_COMMITTED;
    }

    private static Throwable preserveUncertainOutcome(Throwable error, boolean uncertain) {
        if (uncertain && error != null) {
            return IggyServerException.fromTcpResponse(TRANSIENT_NOT_COMMITTED, new byte[0]);
        }
        return error;
    }

    private static IggyTimeoutException responseTimeout(int commandCode) {
        return new IggyTimeoutException("Timed out waiting for a response to command code " + commandCode);
    }

    static IggyServerException findServerError(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof IggyServerException serverError) {
                return serverError;
            }
            cause = cause.getCause();
        }
        return null;
    }

    private static void releaseIfPresent(ByteBuf payload) {
        if (payload != null) {
            payload.release();
        }
    }

    static void completeWithResponse(CompletableFuture<ByteBuf> future, ByteBuf response, Throwable error) {
        if (error != null) {
            future.completeExceptionally(error);
        } else {
            completeWithResponse(future, response);
        }
    }

    private static void completeWithResponse(CompletableFuture<ByteBuf> future, ByteBuf response) {
        if (!future.complete(response)) {
            response.release();
        }
    }

    private void releaseChannel(Channel channel) {
        channelPool.release(channel).addListener(future -> {
            if (!future.isSuccess()) {
                log.warn(
                        "Failed to release VSR channel lease: {}",
                        future.cause().getMessage());
                channel.close();
            }
        });
    }

    private static long toTimeoutNanos(Duration timeout) {
        try {
            return timeout.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private boolean shouldRetryTransient(
            int commandCode, Throwable error, long deadlineNanos, long notAcceptedDeadlineNanos, boolean routed) {
        if (!(error instanceof IggyServerException serverError)) {
            return false;
        }
        if (isPollRoutingCode(commandCode)
                || (routed
                        && (commandCode == CommandCode.Messages.POLL.getValue()
                                || commandCode == CommandCode.ConsumerOffset.GET.getValue()))) {
            return false;
        }
        if (serverError.getRawErrorCode() == TRANSIENT_NOT_COMMITTED) {
            return System.nanoTime() < deadlineNanos;
        }
        if (serverError.getRawErrorCode() == TRANSIENT_NOT_ACCEPTED) {
            return !routed && System.nanoTime() < notAcceptedDeadlineNanos;
        }
        return false;
    }

    private void handlePostResponse(
            Channel channel, int commandCode, boolean isLoginOp, ByteBuf response, Throwable ex) {
        if (isLoginOp) {
            if (ex == null) {
                authenticated = true;
                authenticatedUserId = response.getUnsignedIntLE(response.readerIndex());
                long generation = authGeneration.incrementAndGet();
                IggyAuthenticator.setAuthGeneration(channel, generation);
            } else {
                login.clear();
            }
        }
        if (commandCode == CommandCode.System.BIND_SESSION.getValue() && ex == null) {
            if (response.readableBytes() < REGISTER_RESPONSE_MIN_BYTES
                    || response.getLongLE(response.readerIndex() + Integer.BYTES) != consensusSession.boundSession()) {
                throw new IggyNotConnectedException("BindSession returned a different session epoch");
            }
            IggyAuthenticator.setAuthGeneration(channel, authGeneration.get());
        }
        if (commandCode == CommandCode.User.LOGOUT.getValue() && ex == null) {
            authenticated = false;
            authGeneration.incrementAndGet();
            IggyAuthenticator.clearAuthGeneration(channel);
            login.clear();
        }
    }

    /**
     * Ends the session that a bind refused, unless a reset already replaced it.
     * The next request on this connection signs in again with the remembered
     * sign-in, or fails without one.
     */
    void resetRefusedSession(long clientLow, long clientHigh, int errorCode) {
        if (consensusSession.resetIfCurrent(clientLow, clientHigh)) {
            onSessionEvicted(errorCode);
        }
    }

    /**
     * A server-side eviction unbinds the transport session and closes its
     * channel. Bumping the generation makes the replacement channel bind the
     * retained session; only a definitive bind refusal starts a new Register.
     *
     * The reason travels to the listener so it can drop what belonged to the
     * evicted session and log what happened. The session itself is kept
     * whichever way the sign-in was made: only an explicit sign-out or close
     * ends one.
     */
    private void onSessionEvicted(int errorCode) {
        authGeneration.incrementAndGet();
        sessionResetListener.accept(errorCode);
    }

    private void captureLoginPayloadIfNeeded(int commandCode, ByteBuf payload) {
        if (isLoginCode(commandCode)) {
            login.set(commandCode, ByteBufUtil.getBytes(payload));
        }
    }

    Optional<AuthenticationSnapshot> authenticationSnapshot() {
        return authenticated ? login.snapshot() : Optional.empty();
    }

    /**
     * Applies a committed password change or rename of the signed-in user to the login that this connection
     * shares, see {@link SessionLogin}, and then tells the credentials listener. Typed, raw and resolved calls all
     * succeed here, and only the frozen request carries the new credentials. A body that does not decode changes
     * nothing.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void refreshCredentials(int commandCode, ByteBuf frame) {
        boolean passwordChange = commandCode == CommandCode.User.CHANGE_PASSWORD.getValue();
        if (!passwordChange && commandCode != CommandCode.User.UPDATE.getValue()) {
            return;
        }
        ByteBuf change = frame.slice(
                frame.readerIndex() + VsrHeaders.HEADER_SIZE, frame.readableBytes() - VsrHeaders.HEADER_SIZE);
        String oldUsername;
        Optional<String> username = Optional.empty();
        Optional<String> password = Optional.empty();
        SessionLogin shared = login;
        synchronized (shared) {
            if (!authenticated || shared.payload == null || shared.commandCode != CommandCode.User.LOGIN.getValue()) {
                return;
            }
            ByteBuf current = Unpooled.wrappedBuffer(shared.payload);
            oldUsername = current.readCharSequence(current.readUnsignedByte(), StandardCharsets.UTF_8)
                    .toString();
            if (!skipUser(change, UserId.of(authenticatedUserId))
                    // UserId refuses a blank name, and no request can name one.
                    && (StringUtils.isBlank(oldUsername) || !skipUser(change, UserId.of(oldUsername)))) {
                return;
            }
            if (passwordChange) {
                password = readField(change).flatMap(currentPassword -> readField(change));
            } else if (change.isReadable() && change.readUnsignedByte() == USERNAME_PRESENT) {
                username = readField(change);
            }
            if (username.isEmpty() && password.isEmpty()) {
                return;
            }
            String oldPassword = current.readCharSequence(current.readUnsignedByte(), StandardCharsets.UTF_8)
                    .toString();
            byte[] nextUsername = username.orElse(oldUsername).getBytes(StandardCharsets.UTF_8);
            byte[] nextPassword = password.orElse(oldPassword).getBytes(StandardCharsets.UTF_8);
            byte[] next = new byte[2 + nextUsername.length + nextPassword.length];
            Unpooled.wrappedBuffer(next)
                    .clear()
                    .writeByte(nextUsername.length)
                    .writeBytes(nextUsername)
                    .writeByte(nextPassword.length)
                    .writeBytes(nextPassword);
            shared.payload = next;
        }
        credentialsListener.refreshed(this, oldUsername, username, password);
    }

    /** Skips the user identifier at the start of {@code change} when it names {@code user}. */
    private static boolean skipUser(ByteBuf change, UserId user) {
        ByteBuf encoded = user.toBytes();
        try {
            int length = encoded.readableBytes();
            if (change.readableBytes() < length
                    || !ByteBufUtil.equals(change, change.readerIndex(), encoded, 0, length)) {
                return false;
            }
            change.skipBytes(length);
            return true;
        } finally {
            encoded.release();
        }
    }

    /** Reads a u8-length-prefixed UTF-8 field, or nothing when the body ends first. */
    private static Optional<String> readField(ByteBuf body) {
        if (!body.isReadable() || body.readableBytes() <= body.getUnsignedByte(body.readerIndex())) {
            return Optional.empty();
        }
        return Optional.of(body.readCharSequence(body.readUnsignedByte(), StandardCharsets.UTF_8)
                .toString());
    }

    public CompletableFuture<Void> close() {
        if (!isClosed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        stopHeartbeat();
        if (consensusSession.owner() == consensusSession) {
            consensusSession.reset();
        }
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        try {
            eventLoop.execute(() -> channels.close().addListener(channelsClosed -> closePool(shutdownFuture)));
        } catch (RejectedExecutionException loopGone) {
            closePool(shutdownFuture);
        }
        return shutdownFuture;
    }

    private void closePool(CompletableFuture<Void> shutdownFuture) {
        try {
            channelPool.closeAsync().addListener(poolClosed -> {
                if (!ownsEventLoopGroup) {
                    completeShutdown(shutdownFuture, poolClosed);
                    return;
                }
                eventLoopGroup
                        .shutdownGracefully()
                        .addListener(groupClosed -> completeShutdown(shutdownFuture, groupClosed));
            });
        } catch (RejectedExecutionException loopGone) {
            log.warn("Event loop rejected the pool close, channel already gone: {}", loopGone.getMessage());
            shutdownFuture.complete(null);
        }
    }

    private static void completeShutdown(CompletableFuture<Void> shutdownFuture, Future<?> step) {
        if (step.isSuccess()) {
            shutdownFuture.complete(null);
        } else {
            shutdownFuture.completeExceptionally(step.cause());
        }
    }

    private static final class PoolChannelHandler extends AbstractChannelPoolHandler {
        private final String host;
        private final int port;
        private final boolean enableTls;
        private final SslContext sslContext;
        private final long dialTimeoutMillis;
        private final ConsensusSession consensusSession;
        private final int maxVsrFrameSize;
        private final IntConsumer onEviction;
        private final Consumer<Channel> onChannelCreated;

        @SuppressWarnings("checkstyle:ParameterNumber")
        PoolChannelHandler(
                String host,
                int port,
                boolean enableTls,
                SslContext sslContext,
                long dialTimeoutMillis,
                ConsensusSession consensusSession,
                int maxVsrFrameSize,
                IntConsumer onEviction,
                Consumer<Channel> onChannelCreated) {
            this.host = host;
            this.port = port;
            this.enableTls = enableTls;
            this.sslContext = sslContext;
            this.dialTimeoutMillis = dialTimeoutMillis;
            this.consensusSession = consensusSession;
            this.maxVsrFrameSize = maxVsrFrameSize;
            this.onEviction = onEviction;
            this.onChannelCreated = onChannelCreated;
        }

        @Override
        public void channelCreated(Channel ch) {
            onChannelCreated.accept(ch);
            ChannelPipeline pipeline = ch.pipeline();
            if (enableTls) {
                SslHandler ssl = sslContext.newHandler(ch.alloc(), host, port);
                // A peer that accepts TCP and then never answers the
                // ClientHello would otherwise hold the dial for Netty's own
                // 10s default, well past the bound the rotation dials under.
                ssl.setHandshakeTimeoutMillis(dialTimeoutMillis);
                pipeline.addLast("ssl", ssl);
            }
            // A pipelining producer's next request is usually written from the
            // completion of the previous reply, so its flush lands inside the
            // read loop that delivered it and several requests leave in one
            // syscall instead of one each. Consolidation is confined to that
            // read loop: with no read in progress every flush passes straight
            // through, so a request on an otherwise idle connection is never
            // waiting on later traffic to push it out.
            pipeline.addLast(
                    "flushConsolidation",
                    new FlushConsolidationHandler(
                            FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, false));
            pipeline.addLast("frameDecoder", new VsrFrameDecoder(maxVsrFrameSize));
            pipeline.addLast("responseHandler", new VsrResponseHandler(consensusSession, onEviction));
        }
    }

    record AuthenticationSnapshot(int commandCode, ByteBuf payload) {}

    /**
     * The login that registers a session again. A connection that binds the session of another connection holds
     * the instance of that connection, so a committed credential change on any of them reaches all of them, and
     * none of them signs in with an old copy.
     */
    private static final class SessionLogin {
        private int commandCode;
        private byte[] payload;

        synchronized void set(int commandCode, byte[] payload) {
            this.commandCode = commandCode;
            this.payload = payload;
        }

        synchronized void clear() {
            payload = null;
        }

        synchronized Optional<AuthenticationSnapshot> snapshot() {
            return payload == null
                    ? Optional.empty()
                    : Optional.of(new AuthenticationSnapshot(commandCode, Unpooled.wrappedBuffer(payload)));
        }
    }

    /** What every write attempt of one request on one channel shares. */
    private record VsrExchange(
            Channel channel,
            VsrResponseHandler handler,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            int commandCode,
            TransientFailoverState failoverState,
            long requestDeadlineNanos,
            long deadlineNanos,
            long notAcceptedDeadlineNanos) {}

    /**
     * Fails a routed exchange that its link failed, by an eviction or a lost
     * connection. An eviction carries the code of a reply, and only this tells
     * the router to try the exchange again where a reply ends the call.
     */
    static final class LinkFailure extends CompletionException {
        LinkFailure(Throwable cause) {
            super(cause);
        }
    }

    static final class TransientFailoverState {
        private PartitionContext context;
        private boolean routed;
        private boolean heartbeat;
        private long requestId;
        private boolean committed;
        private boolean linkFailed;
        private boolean detachedSession;
        private boolean uncertain;
        private boolean everUncertain;
        private ConsensusSession.LaneKey lane;
        private ConsensusSession.Ticket ticket;
        private RuntimeException retainedError;
        private byte[] routingBody;
        private final Set<ConnectionInfo> visitedTargets = new HashSet<>();

        TransientFailoverState() {
            this(PartitionContext.EMPTY);
        }

        TransientFailoverState(PartitionContext context) {
            this.context = context;
        }

        void restore(ConsensusSession.Ticket retained) {
            ticket = retained;
            requestId = retained.operationId();
            uncertain = true;
            everUncertain = true;
        }

        ConsensusSession.Ticket ticket() {
            return ticket;
        }

        RuntimeException retainTicket() {
            return retainTicket(ticket);
        }

        RuntimeException retainTicket(ConsensusSession.Ticket retained) {
            synchronized (this) {
                if (retainedError != null) {
                    return retainedError;
                }
                // Retaining wakes resolvers inline, so publish the guard before invoking callbacks without this lock.
                retainedError = new IggyOperationException(IggyErrorCode.OPERATION_UNCERTAIN, retained.operationId());
            }
            RuntimeException error = retained.retain();
            synchronized (this) {
                retainedError = error;
            }
            return error;
        }

        boolean wasUncertain() {
            return everUncertain;
        }

        boolean committed() {
            return committed;
        }

        void setLane(ConsensusSession.LaneKey lane) {
            this.lane = lane;
        }

        void setRoutingBody(byte[] routingBody) {
            this.routingBody = routingBody;
        }

        void renewRequestId() {
            requestId = 0;
            committed = false;
        }

        PartitionContext context() {
            return context;
        }

        void setContext(PartitionContext context) {
            this.context = context;
        }

        Set<ConnectionInfo> visitedTargets() {
            return visitedTargets;
        }
    }

    @FunctionalInterface
    interface CredentialsListener {
        void refreshed(
                AsyncTcpConnection source,
                String previousUsername,
                Optional<String> username,
                Optional<String> password);
    }

    @FunctionalInterface
    interface TransientFailoverHandler {
        CompletableFuture<ByteBuf> retry(
                AsyncTcpConnection source,
                int commandCode,
                ByteBuf payload,
                long requestDeadlineNanos,
                Throwable rejection,
                TransientFailoverState failoverState);
    }

    public static class TcpConnectionPoolConfig {
        private final int maxPendingAcquires;
        private final long acquireTimeoutMillis;

        public TcpConnectionPoolConfig() {
            this(
                    TcpConnectionPoolConfigBuilder.DEFAULT_MAX_PENDING_ACQUIRES,
                    TcpConnectionPoolConfigBuilder.DEFAULT_ACQUIRE_TIMEOUT_MILLIS);
        }

        public TcpConnectionPoolConfig(int maxPendingAcquires, long acquireTimeoutMillis) {
            this.maxPendingAcquires = maxPendingAcquires;
            this.acquireTimeoutMillis = acquireTimeoutMillis;
        }

        public static TcpConnectionPoolConfigBuilder builder() {
            return new TcpConnectionPoolConfigBuilder();
        }

        public int getMaxPendingAcquires() {
            return this.maxPendingAcquires;
        }

        public long getAcquireTimeoutMillis() {
            return this.acquireTimeoutMillis;
        }

        public static final class TcpConnectionPoolConfigBuilder {
            public static final int DEFAULT_MAX_PENDING_ACQUIRES = 1000;
            public static final int DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 3000;

            private int maxPendingAcquires;
            private long acquireTimeoutMillis;

            public TcpConnectionPoolConfigBuilder() {}

            public TcpConnectionPoolConfigBuilder setMaxPendingAcquires(int maxPendingAcquires) {
                if (maxPendingAcquires <= 0) {
                    throw new IggyInvalidArgumentException("Max Pending Acquires cannot be 0 or negative");
                }
                this.maxPendingAcquires = maxPendingAcquires;
                return this;
            }

            public TcpConnectionPoolConfigBuilder setAcquireTimeoutMillis(long acquireTimeoutMillis) {
                if (acquireTimeoutMillis <= 0) {
                    throw new IggyInvalidArgumentException("Acquire timeout cannot be 0 or negative");
                }
                this.acquireTimeoutMillis = acquireTimeoutMillis;
                return this;
            }

            public TcpConnectionPoolConfig build() {
                if (this.acquireTimeoutMillis == 0) {
                    this.acquireTimeoutMillis = DEFAULT_ACQUIRE_TIMEOUT_MILLIS;
                }
                if (this.maxPendingAcquires == 0) {
                    this.maxPendingAcquires = DEFAULT_MAX_PENDING_ACQUIRES;
                }
                return new TcpConnectionPoolConfig(maxPendingAcquires, acquireTimeoutMillis);
            }
        }
    }
}
