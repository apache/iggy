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
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrRequestEncoder;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyEmptyResponseException;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.exception.IggyTlsException;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    // a leader recheck and safe replay; mirrors TRANSIENT_FAILOVER_CHECK_INTERVAL
    // in core/sdk/src/tcp/tcp_client.rs.
    //
    // Package-private: the client classifies a failed sign-in by these codes,
    // and a transient one is not a rejected credential.
    static final int TRANSIENT_NOT_COMMITTED = 57;
    static final int TRANSIENT_NOT_ACCEPTED = 58;
    private static final Logger log = LoggerFactory.getLogger(AsyncTcpConnection.class);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofMillis(3000);
    // A missing reply must not hold the single VSR-pinned channel forever.
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final long TRANSIENT_RETRY_INTERVAL_MS = 50;
    private static final Duration TRANSIENT_RETRY_BUDGET = Duration.ofSeconds(30);
    private static final Duration NOT_ACCEPTED_RETRY_BUDGET = Duration.ofSeconds(2);

    private final IoEventLoopGroup eventLoopGroup;
    private final FixedChannelPool channelPool;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong authGeneration = new AtomicLong(0);
    private final VsrRequestEncoder vsrEncoder;
    private final TransientFailoverHandler transientFailoverHandler;
    private final IntConsumer sessionResetListener;
    private final Consumer<Throwable> connectionFailureListener;
    private final long requestTimeoutNanos;
    private final long heartbeatIntervalNanos;
    private final Object heartbeatLock = new Object();
    private ByteBuf loginPayload;
    private ScheduledFuture<?> heartbeatTask;
    private boolean heartbeatRunning;

    private volatile int loginCommandCode;
    private volatile boolean authenticated = false;

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
                connectionTimeout,
                Optional.empty(),
                Duration.ofSeconds(5),
                VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                null,
                errorCode -> {},
                ignored -> {});
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    AsyncTcpConnection(
            String host,
            int port,
            boolean enableTls,
            Optional<File> tlsCertificate,
            TcpConnectionPoolConfig poolConfig,
            Optional<Duration> connectionTimeout,
            Optional<Duration> requestTimeout,
            Duration heartbeatInterval,
            int maxVsrFrameSize,
            TransientFailoverHandler transientFailoverHandler,
            IntConsumer sessionResetListener,
            Consumer<Throwable> connectionFailureListener) {
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

        ConsensusSession consensusSession = new ConsensusSession();
        this.vsrEncoder = new VsrRequestEncoder(consensusSession);
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        var bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int)
                        connectionTimeout.orElse(DEFAULT_CONNECTION_TIMEOUT).toMillis())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(host, port);

        // The VSR session (client id, fence epoch, request counter) is bound
        // to one transport connection server-side; sharing it across channels
        // would interleave request ids, so the pool holds a single channel.
        this.channelPool = new FixedChannelPool(
                bootstrap,
                new PoolChannelHandler(
                        host, port, enableTls, sslContext, consensusSession, maxVsrFrameSize, this::onSessionEvicted),
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                poolConfig.getAcquireTimeoutMillis(),
                1,
                poolConfig.getMaxPendingAcquires());

        log.info("Connection pool initialized with a single VSR-pinned connection");
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
            heartbeatTask =
                    eventLoopGroup.next().schedule(this::sendHeartbeat, heartbeatIntervalNanos, TimeUnit.NANOSECONDS);
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
            heartbeat = send(CommandCode.System.PING.getValue(), Unpooled.EMPTY_BUFFER);
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
        });
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

    public <T> CompletableFuture<T> exchangeForEntity(
            CommandCode commandCode, ByteBuf payload, Function<ByteBuf, T> func) {
        return send(commandCode, payload).thenApply(response -> {
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
        return send(commandCode, payload).thenApply(response -> {
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
        return send(commandCode, payload).thenApply(response -> {
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
        return send(commandCode, payload).thenAccept(ByteBuf::release);
    }

    public CompletableFuture<ByteBuf> send(CommandCode commandCode, ByteBuf payload) {
        return send(commandCode.getValue(), payload);
    }

    public CompletableFuture<ByteBuf> send(int commandCode, ByteBuf payload) {
        return send(commandCode, payload, 0);
    }

    CompletableFuture<ByteBuf> send(int commandCode, ByteBuf payload, long requestDeadlineNanos) {
        if (isLoginCode(commandCode) && authenticated) {
            return logoutThenLogin(commandCode, payload);
        }
        captureLoginPayloadIfNeeded(commandCode, payload);
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();
        CompletableFuture<ByteBuf> callerFuture = new CompletableFuture<>();
        ByteBuf failoverPayload =
                transientFailoverHandler != null && !isLoginCode(commandCode) ? payload.retainedDuplicate() : null;

        channelPool.acquire().addListener((FutureListener<Channel>) f -> {
            if (!f.isSuccess()) {
                payload.release();
                releaseIfPresent(failoverPayload);
                notifyConnectionFailure(f.cause());
                callerFuture.completeExceptionally(mapAcquireException(f.cause()));
                return;
            }
            dispatchAcquiredChannel(
                    f.getNow(),
                    commandCode,
                    payload,
                    failoverPayload,
                    responseFuture,
                    callerFuture,
                    requestDeadlineNanos);
        });

        return callerFuture;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void dispatchAcquiredChannel(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            ByteBuf failoverPayload,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long requestDeadlineNanos) {
        Runnable dispatch = () -> dispatchOnChannel(
                channel, commandCode, payload, failoverPayload, responseFuture, callerFuture, requestDeadlineNanos);
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

    private void dispatchOnChannel(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            ByteBuf failoverPayload,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long inheritedRequestDeadlineNanos) {
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
                response,
                error));
        authenticationStep(channel, commandCode, requestDeadlineNanos)
                .whenComplete((ignored, authError) -> completeAuthenticationStep(
                        channel,
                        commandCode,
                        payload,
                        responseFuture,
                        requestDeadlineNanos,
                        holdLeaseUntilResponse,
                        authError));
    }

    private CompletableFuture<Void> authenticationStep(Channel channel, int commandCode, long requestDeadlineNanos) {
        if (isLoginCode(commandCode) || !requiresAuthentication(commandCode)) {
            return CompletableFuture.completedFuture(null);
        }
        if (!authenticated) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        ByteBuf loginPayloadCopy = getLoginPayloadCopy();
        if (loginPayloadCopy == null) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }
        return IggyAuthenticator.ensureAuthenticated(
                channel,
                loginPayloadCopy,
                authGeneration,
                payloadToLogin ->
                        sendAuthenticationFrame(channel, payloadToLogin, loginCommandCode, requestDeadlineNanos));
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    private void completeAuthenticationStep(
            Channel channel,
            int commandCode,
            ByteBuf payload,
            CompletableFuture<ByteBuf> responseFuture,
            long requestDeadlineNanos,
            boolean holdLeaseUntilResponse,
            Throwable authError) {
        try {
            if (authError != null) {
                payload.release();
                responseFuture.completeExceptionally(authError);
                return;
            }
            sendFrame(channel, payload, commandCode, responseFuture, requestDeadlineNanos);
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
            ByteBuf response,
            Throwable error) {
        try {
            handlePostResponse(channel, commandCode, isLoginCommand, error);
        } catch (RuntimeException bookkeepingError) {
            log.error("Post-response bookkeeping failed: {}", bookkeepingError.getMessage());
        }
        if (error == null) {
            releaseIfPresent(failoverPayload);
            completeWithResponse(callerFuture, response);
            return;
        }
        completeFailedRequest(commandCode, failoverPayload, requestDeadlineNanos, callerFuture, error);
    }

    private void completeFailedRequest(
            int commandCode,
            ByteBuf failoverPayload,
            long requestDeadlineNanos,
            CompletableFuture<ByteBuf> callerFuture,
            Throwable error) {
        IggyTimeoutException timeout = findResponseTimeout(error);
        if (timeout != null) {
            notifyConnectionFailure(timeout);
        }
        IggyServerException serverError = findServerError(error);
        if (shouldRecheckLeader(serverError, failoverPayload, requestDeadlineNanos)) {
            retryAfterLeaderRecheck(commandCode, failoverPayload, requestDeadlineNanos, serverError, callerFuture);
            return;
        }
        releaseIfPresent(failoverPayload);
        callerFuture.completeExceptionally(error);
    }

    private static boolean shouldRecheckLeader(
            IggyServerException serverError, ByteBuf failoverPayload, long requestDeadlineNanos) {
        return serverError != null
                && serverError.getRawErrorCode() == TRANSIENT_NOT_ACCEPTED
                && failoverPayload != null
                && requestDeadlineNanos - System.nanoTime() > 0;
    }

    private void retryAfterLeaderRecheck(
            int commandCode,
            ByteBuf payload,
            long requestDeadlineNanos,
            IggyServerException rejection,
            CompletableFuture<ByteBuf> callerFuture) {
        CompletableFuture<ByteBuf> retry;
        try {
            retry = transientFailoverHandler.retry(this, commandCode, payload, requestDeadlineNanos, rejection);
        } catch (RuntimeException retryError) {
            payload.release();
            callerFuture.completeExceptionally(retryError);
            return;
        }
        retry.whenComplete((response, error) -> {
            if (error != null) {
                callerFuture.completeExceptionally(error);
            } else {
                completeWithResponse(callerFuture, response);
            }
        });
    }

    private CompletableFuture<ByteBuf> sendAuthenticationFrame(
            Channel channel, ByteBuf payload, int commandCode, long requestDeadlineNanos) {
        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        sendFrame(channel, payload, commandCode, loginFuture, requestDeadlineNanos);
        return loginFuture;
    }

    /**
     * Pool acquire failures and expired replies both make the current target
     * unusable. The listener lets the owning client run its redial strategy
     * while the failed request surfaces to its caller.
     */
    private void notifyConnectionFailure(Throwable cause) {
        try {
            connectionFailureListener.accept(cause);
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
     * A Register on an already-bound VSR connection is answered with a replay
     * of the original register reply, while the client has re-armed a fresh
     * identity; its reset request counter would then collide with the
     * server's dedup table and mutations would be silently swallowed. Unbind
     * first, then login fresh.
     */
    private CompletableFuture<ByteBuf> logoutThenLogin(int commandCode, ByteBuf payload) {
        return send(CommandCode.User.LOGOUT.getValue(), Unpooled.EMPTY_BUFFER)
                .handle((logoutResponse, logoutError) -> {
                    if (logoutResponse != null) {
                        logoutResponse.release();
                    }
                    return null;
                })
                .thenCompose(ignored -> send(commandCode, payload));
    }

    private static boolean isLoginCode(int commandCode) {
        return commandCode == CommandCode.User.LOGIN.getValue()
                || commandCode == CommandCode.PersonalAccessToken.LOGIN.getValue();
    }

    private static boolean mutatesSessionState(int commandCode) {
        return isLoginCode(commandCode) || commandCode == CommandCode.User.LOGOUT.getValue();
    }

    /**
     * Ping is the only command the server answers without a bound session. The
     * cluster roster is auth-gated as well, so an unauthenticated caller cannot
     * enumerate the topology and leader selection can only run on a bound
     * session.
     */
    private static boolean requiresAuthentication(int commandCode) {
        return !isAllowedBeforeAuthentication(commandCode);
    }

    private static boolean isAllowedBeforeAuthentication(int commandCode) {
        return commandCode == CommandCode.System.PING.getValue();
    }

    private void sendFrame(
            Channel channel,
            ByteBuf payload,
            int commandCode,
            CompletableFuture<ByteBuf> responseFuture,
            long requestDeadlineNanos) {
        try {
            VsrResponseHandler handler = channel.pipeline().get(VsrResponseHandler.class);
            if (handler == null) {
                throw new IggyClientException("Channel missing VsrResponseHandler");
            }

            ByteBuf frame = vsrEncoder.encode(channel.alloc(), commandCode, payload);
            long nowNanos = System.nanoTime();
            long deadlineNanos = nowNanos + TRANSIENT_RETRY_BUDGET.toNanos();
            long notAcceptedDeadlineNanos =
                    isLoginCode(commandCode) ? deadlineNanos : nowNanos + NOT_ACCEPTED_RETRY_BUDGET.toNanos();
            writeVsrFrame(
                    channel,
                    handler,
                    frame,
                    responseFuture,
                    requestDeadlineNanos,
                    deadlineNanos,
                    notAcceptedDeadlineNanos,
                    commandCode);
        } catch (RuntimeException e) {
            responseFuture.completeExceptionally(e);
        } finally {
            payload.release();
        }
    }

    /**
     * One VSR write attempt. A transient denial (the cluster could not commit
     * or accept yet) replays the SAME encoded frame so the server's dedup
     * sees the same request id; everything else resolves the caller.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private void writeVsrFrame(
            Channel channel,
            VsrResponseHandler handler,
            ByteBuf frame,
            CompletableFuture<ByteBuf> responseFuture,
            long requestDeadlineNanos,
            long deadlineNanos,
            long notAcceptedDeadlineNanos,
            int commandCode) {
        if (requestDeadlineNanos - System.nanoTime() <= 0) {
            IggyTimeoutException timeout = responseTimeout(commandCode);
            handler.closeChannel(channel, timeout);
            frame.release();
            responseFuture.completeExceptionally(timeout);
            return;
        }
        CompletableFuture<ByteBuf> attempt = new CompletableFuture<>();
        try {
            handler.registerRequest(channel, frame, attempt, requestDeadlineNanos, commandCode);
        } catch (RuntimeException error) {
            handler.closeChannel(channel, error);
            frame.release();
            responseFuture.completeExceptionally(error);
            return;
        }
        channel.writeAndFlush(frame.retainedDuplicate()).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                log.error("Failed to send frame: {}", future.cause().getMessage());
                // A failed write leaves framing undefined. Closing removes and
                // fails every pending entry before the channel can be reused.
                handler.closeChannel(channel, future.cause());
            }
        });
        attempt.whenComplete((response, error) -> {
            if (shouldRetryTransient(error, deadlineNanos, notAcceptedDeadlineNanos) && channel.isActive()) {
                try {
                    channel.eventLoop()
                            .schedule(
                                    () -> writeVsrFrame(
                                            channel,
                                            handler,
                                            frame,
                                            responseFuture,
                                            requestDeadlineNanos,
                                            deadlineNanos,
                                            notAcceptedDeadlineNanos,
                                            commandCode),
                                    TRANSIENT_RETRY_INTERVAL_MS,
                                    TimeUnit.MILLISECONDS);
                    return;
                } catch (RejectedExecutionException retryRejected) {
                    log.warn("Event loop rejected a VSR retry, failing the request: {}", retryRejected.getMessage());
                }
            }
            frame.release();
            if (error != null) {
                responseFuture.completeExceptionally(error);
            } else {
                responseFuture.complete(response);
            }
        });
    }

    private static IggyTimeoutException responseTimeout(int commandCode) {
        return new IggyTimeoutException("Timed out waiting for a response to command code " + commandCode);
    }

    private static IggyTimeoutException findResponseTimeout(Throwable error) {
        Throwable cause = error;
        while (cause != null) {
            if (cause instanceof IggyTimeoutException timeout) {
                return timeout;
            }
            cause = cause.getCause();
        }
        return null;
    }

    private static IggyServerException findServerError(Throwable error) {
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

    private static boolean shouldRetryTransient(Throwable error, long deadlineNanos, long notAcceptedDeadlineNanos) {
        if (!(error instanceof IggyServerException serverError)) {
            return false;
        }
        if (serverError.getRawErrorCode() == TRANSIENT_NOT_COMMITTED) {
            return System.nanoTime() < deadlineNanos;
        }
        if (serverError.getRawErrorCode() == TRANSIENT_NOT_ACCEPTED) {
            return System.nanoTime() < notAcceptedDeadlineNanos;
        }
        return false;
    }

    private void handlePostResponse(Channel channel, int commandCode, boolean isLoginOp, Throwable ex) {
        if (isLoginOp) {
            if (ex == null) {
                authenticated = true;
                long generation = authGeneration.incrementAndGet();
                IggyAuthenticator.setAuthGeneration(channel, generation);
            } else {
                releaseLoginPayload();
            }
        }
        if (commandCode == CommandCode.User.LOGOUT.getValue()) {
            authenticated = false;
            authGeneration.incrementAndGet();
            IggyAuthenticator.clearAuthGeneration(channel);
        }
    }

    /**
     * A server-side eviction unbinds the transport session and closes its
     * channel. Bumping the generation makes the replacement channel re-run
     * login and Register. The fresh session invalidates cached routing state
     * such as consumer-group assignments.
     *
     * The reason travels to the listener, which owns the question of whether
     * the session may be re-established at all: only it knows whether the
     * sign-in was configured on the client or run by a caller.
     */
    private void onSessionEvicted(int errorCode) {
        authGeneration.incrementAndGet();
        sessionResetListener.accept(errorCode);
    }

    private void captureLoginPayloadIfNeeded(int commandCode, ByteBuf payload) {
        if (isLoginCode(commandCode)) {
            updateLoginPayload(commandCode, payload);
        }
    }

    private synchronized void updateLoginPayload(int commandCode, ByteBuf payload) {
        if (this.loginPayload != null) {
            loginPayload.release();
        }
        this.loginPayload = payload.retainedSlice();
        this.loginCommandCode = commandCode;
    }

    private synchronized ByteBuf getLoginPayloadCopy() {
        if (this.loginPayload != null) {
            return loginPayload.retainedDuplicate();
        }
        return null;
    }

    synchronized Optional<AuthenticationSnapshot> authenticationSnapshot() {
        if (!authenticated || loginPayload == null) {
            return Optional.empty();
        }
        return Optional.of(new AuthenticationSnapshot(loginCommandCode, loginPayload.retainedDuplicate()));
    }

    private synchronized void releaseLoginPayload() {
        if (this.loginPayload != null) {
            loginPayload.release();
            this.loginPayload = null;
        }
    }

    public CompletableFuture<Void> close() {
        if (!isClosed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        stopHeartbeat();
        releaseLoginPayload();
        CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
        channelPool
                .closeAsync()
                .addListener(f -> eventLoopGroup.shutdownGracefully().addListener(sf -> {
                    if (sf.isSuccess()) {
                        shutdownFuture.complete(null);
                    } else {
                        shutdownFuture.completeExceptionally(sf.cause());
                    }
                }));
        return shutdownFuture;
    }

    private static final class PoolChannelHandler extends AbstractChannelPoolHandler {
        private final String host;
        private final int port;
        private final boolean enableTls;
        private final SslContext sslContext;
        private final ConsensusSession consensusSession;
        private final int maxVsrFrameSize;
        private final IntConsumer onEviction;

        PoolChannelHandler(
                String host,
                int port,
                boolean enableTls,
                SslContext sslContext,
                ConsensusSession consensusSession,
                int maxVsrFrameSize,
                IntConsumer onEviction) {
            this.host = host;
            this.port = port;
            this.enableTls = enableTls;
            this.sslContext = sslContext;
            this.consensusSession = consensusSession;
            this.maxVsrFrameSize = maxVsrFrameSize;
            this.onEviction = onEviction;
        }

        @Override
        public void channelCreated(Channel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (enableTls) {
                pipeline.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
            }
            pipeline.addLast("frameDecoder", new VsrFrameDecoder(maxVsrFrameSize));
            pipeline.addLast("responseHandler", new VsrResponseHandler(consensusSession, onEviction));
        }
    }

    record AuthenticationSnapshot(int commandCode, ByteBuf payload) {}

    @FunctionalInterface
    interface TransientFailoverHandler {
        CompletableFuture<ByteBuf> retry(
                AsyncTcpConnection source,
                int commandCode,
                ByteBuf payload,
                long requestDeadlineNanos,
                IggyServerException rejection);
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
