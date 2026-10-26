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
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
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
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

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
    static final Duration NOT_ACCEPTED_RETRY_BUDGET = Duration.ofSeconds(2);
    static final String SIGN_IN_REPLACED = "A new sign-in replaced this one";
    static final long LIFECYCLE_RETRY_MAX_INTERVAL_MS = 1000;
    // The pool holds one channel, and one channel lives on one loop.
    static final int DEFAULT_IO_THREADS = 1;
    static final String NOT_AUTHENTICATED = "Not authenticated, call login first";

    private static final int STALE_CLIENT = 30;
    private static final int UNAUTHENTICATED = 40;
    private static final int LIFECYCLE_BUSY = 88;
    private static final int REGISTER_RESPONSE_MIN_BYTES = 17;
    private static final Logger log = LoggerFactory.getLogger(AsyncTcpConnection.class);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofMillis(3000);
    // A missing reply must not hold the single VSR-pinned channel forever.
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final long TRANSIENT_RETRY_INTERVAL_MS = 50;
    // Each lifecycle retry commits another refusal, so its pause doubles from
    // the transient interval up to this cap; mirrors LIFECYCLE_RETRY_MAX_INTERVAL
    // in core/sdk/src/vsr.rs.
    private static final Duration TRANSIENT_RETRY_BUDGET = Duration.ofSeconds(30);
    // The pause before a leader recheck that follows NOT_PRIMARY in a row grows from the base to the cap with full
    // jitter; mirrors RETRY_BACKOFF_BASE and RETRY_BACKOFF_MAX in core/sdk/src/links/mod.rs.
    private static final Duration RETRY_BACKOFF_BASE = Duration.ofMillis(50);
    private static final Duration RETRY_BACKOFF_MAX = Duration.ofSeconds(1);
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
    private final ConnectionHeartbeat heartbeat;
    // Shared with the connections that bind this session, see bindSharedSession.
    private volatile SessionLogin login = new SessionLogin();
    private BooleanSupplier signInOwed = () -> false;
    private volatile CredentialsListener credentialsListener = (source, previousUsername, username, password) -> {};
    private volatile LongConsumer signInRefusedListener = lineage -> {};

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
        this.heartbeat = new ConnectionHeartbeat(
                eventLoop, toTimeoutNanos(heartbeatInterval), this::sendHeartbeat, isClosed::get);

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
                        heartbeat.start();
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

    /** Sends a ping whose timed-out reply counts as a lost link, see completeRequest. */
    private CompletableFuture<ByteBuf> sendHeartbeat() {
        TransientFailoverState state = new TransientFailoverState();
        state.heartbeat = true;
        return send(CommandCode.System.PING.getValue(), Unpooled.EMPTY_BUFFER, 0, state);
    }

    /** Runs after each heartbeat that the server answered. */
    void onHeartbeat(Runnable listener, BooleanSupplier owed) {
        heartbeat.onAnswered(listener);
        signInOwed = owed;
    }

    /**
     * Closes the channel before aborting Register, whose late reply could match the next Register's request id.
     * Other requests fail as not connected, so they do not redial.
     */
    void abortSignIn(CompletableFuture<?> signIn) {
        try {
            eventLoop.execute(() -> {
                if (signIn.isDone()) {
                    return;
                }
                closeSignInChannels(channels);
                signIn.cancel(false);
            });
        } catch (RejectedExecutionException loopGone) {
            signIn.cancel(false);
        }
    }

    void discardAuthentication() {
        synchronized (consensusSession) {
            synchronized (consensusSession.owner()) {
                authenticated = false;
                login.clear();
                authGeneration.incrementAndGet();
            }
        }
        List<Channel> abandoned = List.copyOf(channels);
        try {
            if (eventLoop.inEventLoop()) {
                closeSignInChannels(abandoned);
            } else {
                eventLoop.execute(() -> closeSignInChannels(abandoned));
            }
        } catch (RejectedExecutionException loopGone) {
            abandoned.forEach(Channel::close);
        }
    }

    private void closeSignInChannels(Iterable<Channel> abandoned) {
        IggyNotConnectedException replaced = new IggyNotConnectedException(SIGN_IN_REPLACED);
        for (Channel channel : abandoned) {
            VsrResponseHandler handler = channel.pipeline().get(VsrResponseHandler.class);
            if (handler != null) {
                handler.closeChannel(channel, replaced);
            } else {
                channel.close();
            }
        }
    }

    /** Runs after a committed password change or rename of the signed-in user replaced the login of this connection. */
    void onCredentialsRefreshed(CredentialsListener listener) {
        credentialsListener = listener;
    }

    /** Runs after the server refused an automatic sign-in of this connection, which forgot its login. */
    void onSignInRefused(LongConsumer listener) {
        signInRefusedListener = listener;
    }

    boolean heartbeatScheduled() {
        return heartbeat.scheduled();
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

    /** Whether a channel of this connection is open. The group drops a channel once it closes. */
    boolean hasOpenChannel() {
        return !channels.isEmpty();
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

    private CompletableFuture<ByteBuf> send(
            int commandCode,
            ByteBuf payload,
            long requestDeadlineNanos,
            TransientFailoverState failoverState,
            long requiredSessionGeneration) {
        long deadline = requestDeadlineNanos == 0 ? System.nanoTime() + requestTimeoutNanos : requestDeadlineNanos;
        if (isLoginCode(commandCode) && authenticated) {
            return logoutThenLogin(commandCode, payload, deadline);
        }
        if (commandCode != CommandCode.System.PING.getValue()) {
            failoverState.startIn(consensusSession);
        }
        captureLoginPayloadIfNeeded(commandCode, payload);
        CompletableFuture<ByteBuf> callerFuture = new CompletableFuture<>();
        ByteBuf failoverPayload = transientFailoverHandler != null
                        && !failoverState.routed
                        && !isLoginCode(commandCode)
                        && !isPollRoutingCode(commandCode)
                ? payload.retainedDuplicate()
                : null;
        attempt(
                new Call(commandCode, failoverPayload, deadline, callerFuture, failoverState),
                payload,
                requiredSessionGeneration);
        return callerFuture;
    }

    private void attempt(Call call, ByteBuf payload, long requiredSessionGeneration) {
        IggyAuthenticator.attempt(this, call, payload, requiredSessionGeneration);
    }

    void acquireForCall(Call call, ByteBuf payload, long requiredSessionGeneration) {
        TransientFailoverState failoverState = call.state();
        failoverState.linkFailed = false;
        failoverState.detachedSession = false;
        var acquiring = channelPool.acquire();
        // The timer and the acquire listener can race, and only the first of them ends the attempt.
        AtomicBoolean acquireSettled = new AtomicBoolean();
        var acquisitionTimeout = eventLoop.schedule(
                () -> {
                    if (acquireSettled.compareAndSet(false, true)) {
                        acquiring.cancel(false);
                        call.fail(
                                failoverState.ticket != null && failoverState.uncertain
                                        ? failoverState.retainTicket()
                                        : responseTimeout(call.commandCode()));
                    }
                },
                Math.max(0, call.deadline() - System.nanoTime()),
                TimeUnit.NANOSECONDS);
        call.callerFuture().whenComplete((response, error) -> {
            if (call.callerFuture().isCancelled()) {
                acquiring.cancel(false);
            }
        });
        acquiring.addListener((FutureListener<Channel>) f -> {
            acquisitionTimeout.cancel(false);
            boolean settles = acquireSettled.compareAndSet(false, true);
            if (!f.isSuccess()) {
                payload.release();
                notifyConnectionFailure(f.cause());
                if (settles) {
                    completeFailedRequest(call, mapAcquireException(f.cause()));
                }
            } else if (!settles || call.callerFuture().isDone()) {
                payload.release();
                releaseChannel(f.getNow());
                if (settles) {
                    call.fail(responseTimeout(call.commandCode()));
                }
            } else {
                dispatchAcquiredChannel(f.getNow(), call, payload, requiredSessionGeneration);
            }
        });
    }

    CompletableFuture<ByteBuf> sendRouted(
            int commandCode, ByteBuf payload, long sessionGeneration, long deadline, TransientFailoverState state) {
        state.routed = true;
        return send(commandCode, payload, deadline, state, sessionGeneration);
    }

    private void dispatchAcquiredChannel(Channel channel, Call call, ByteBuf payload, long requiredSessionGeneration) {
        Runnable dispatch = () -> dispatchOnChannel(channel, call, payload, requiredSessionGeneration);
        if (channel.eventLoop().inEventLoop()) {
            dispatch.run();
            return;
        }
        try {
            channel.eventLoop().execute(dispatch);
        } catch (RejectedExecutionException error) {
            payload.release();
            releaseChannel(channel);
            call.fail(error);
        }
    }

    private void dispatchOnChannel(Channel channel, Call call, ByteBuf payload, long requiredSessionGeneration) {
        int commandCode = call.commandCode();
        boolean isLoginCommand = isLoginCode(commandCode);
        boolean holdLeaseUntilResponse = mutatesSessionState(commandCode);
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();
        responseFuture.whenComplete((response, error) -> completeResponse(
                channel,
                commandCode,
                isLoginCommand,
                call.failoverPayload(),
                call.deadline(),
                holdLeaseUntilResponse,
                call.callerFuture(),
                call.state(),
                response,
                error));
        (isLoginCommand || isAllowedBeforeAuthentication(commandCode)
                        ? CompletableFuture.<Void>completedFuture(null)
                        : IggyAuthenticator.authenticate(this, channel, call.deadline(), call.state()))
                .whenComplete((ignored, authError) -> completeAuthenticationStep(
                        channel,
                        call,
                        payload,
                        responseFuture,
                        holdLeaseUntilResponse,
                        requiredSessionGeneration,
                        authError));
    }

    AtomicLong authenticationGeneration() {
        return authGeneration;
    }

    CompletableFuture<Void> awaitAuthentication(long deadline, TransientFailoverState state) {
        return IggyAuthenticator.awaitAuthentication(this, deadline, state, signInOwed);
    }

    private void completeAuthenticationStep(
            Channel channel,
            Call call,
            ByteBuf payload,
            CompletableFuture<ByteBuf> responseFuture,
            boolean holdLeaseUntilResponse,
            long requiredSessionGeneration,
            Throwable authError) {
        TransientFailoverState state = call.state();
        boolean retry = state.retryAuthentication && !call.over();
        try {
            if (retry) {
                state.retryAuthentication = false;
                attempt(call, payload, requiredSessionGeneration);
                return;
            }
            // A timed-out authentication step sent nothing of the call, so the call can be retried.
            Throwable failure = state.authenticationFailure(authError);
            if (failure != null || call.callerFuture().isDone()) {
                payload.release();
                responseFuture.completeExceptionally(failure == null ? responseTimeout(call.commandCode()) : failure);
                return;
            }
            if (requiredSessionGeneration != 0 && requiredSessionGeneration != sessionGeneration()) {
                payload.release();
                responseFuture.completeExceptionally(notAccepted());
                return;
            }
            sendFrame(
                    channel, payload, call.commandCode(), responseFuture, call.callerFuture(), call.deadline(), state);
        } finally {
            if (!holdLeaseUntilResponse || retry) {
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
            synchronized (consensusSession) {
                synchronized (consensusSession.owner()) {
                    if ((isLoginCommand || VsrOperation.operationForCode(commandCode) == VsrOperation.NON_REPLICATED)
                            && failoverState.signedOut()) {
                        throw failoverState.lineageEnded(error);
                    }
                    handlePostResponse(channel, commandCode, isLoginCommand, response, error);
                }
            }
        } catch (RuntimeException bookkeepingError) {
            releaseIfPresent(response);
            new Call(commandCode, failoverPayload, requestDeadlineNanos, callerFuture, failoverState)
                    .fail(bookkeepingError);
            return;
        }
        if (error == null) {
            releaseIfPresent(failoverPayload);
            completeWithResponse(callerFuture, response);
            return;
        }
        Throwable cause = unwrap(error);
        if (failoverState.heartbeat && cause instanceof IggyTimeoutException) {
            channel.close();
            notifyConnectionFailure(new IggyConnectionException("The heartbeat reply timed out", cause));
        }
        if (cause instanceof IggyConnectionException || cause instanceof java.io.IOException) {
            notifyConnectionFailure(cause);
        }
        completeFailedRequest(
                new Call(commandCode, failoverPayload, requestDeadlineNanos, callerFuture, failoverState), error);
    }

    /**
     * Retries unsent calls after session recovery, and transient refusals and links lost before the frame went out
     * after a leader recheck, as in Rust. Routed calls and terminal failures return to their caller.
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void completeFailedRequest(Call call, Throwable error) {
        Throwable cause = unwrap(error);
        TransientFailoverState failoverState = call.state();
        if (failoverState.routed) {
            releaseIfPresent(call.failoverPayload());
            call.callerFuture().completeExceptionally(failoverState.linkFailed ? new LinkFailure(error) : error);
            return;
        }
        IggyServerException serverError = findServerError(error);
        boolean notPrimaryInARow = failoverState.notPrimary;
        failoverState.notPrimary = serverError != null && serverError.getRawErrorCode() == NOT_PRIMARY;
        // Only a link that failed a registered frame can have carried it.
        boolean unsentLoss = lostLink(failoverState, cause) && !failoverState.linkFailed;
        if (call.failoverPayload() == null || call.over()) {
            call.end(error);
        } else if (failoverState.ticket != null && failoverState.ticket.takeRestart()) {
            restartUnderSession(call);
        } else if (!isRefusal(serverError)
                && !unsentLoss
                && !(failoverState.uncertain && lostLink(failoverState, cause))) {
            call.end(error);
        } else {
            failoverState.refused |= isRefusal(serverError) || unsentLoss;
            if (!failoverState.notPrimary || notPrimaryInARow) {
                pauseBeforeRecheck(call, cause);
            } else {
                scheduleNextAttempt(call, 0, () -> retryAfterLeaderRecheck(call, cause), cause);
            }
        }
    }

    /** A detached-session eviction loses the link too: the admitted attempt can still commit under that session. */
    private static boolean lostLink(TransientFailoverState state, Throwable error) {
        return error instanceof IggyConnectionException
                || error instanceof java.io.IOException
                || (state.linkFailed && state.detachedSession);
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }

    static IggyServerException notAccepted() {
        return IggyServerException.fromTcpResponse(TRANSIENT_NOT_ACCEPTED, new byte[0]);
    }

    /** Whether the server refused the request before it admitted it. */
    private static boolean isRefusal(IggyServerException serverError) {
        return serverError != null
                && (serverError.getRawErrorCode() == TRANSIENT_NOT_ACCEPTED
                        || serverError.getRawErrorCode() == NOT_PRIMARY);
    }

    /**
     * Paces lost-link and non-admission retries with full jitter and a doubling ceiling. First NOT_PRIMARY retries
     * immediately; a pause that would reach the deadline ends the call.
     */
    private void pauseBeforeRecheck(Call call, Throwable rejection) {
        TransientFailoverState state = call.state();
        long pause = ThreadLocalRandom.current().nextLong(state.pauseCeilingNanos + 1);
        state.pauseCeilingNanos = Math.min(state.pauseCeilingNanos * 2, RETRY_BACKOFF_MAX.toNanos());
        if (call.deadline() - System.nanoTime() - pause <= 0) {
            call.end(notAccepted());
        } else {
            scheduleNextAttempt(call, pause, () -> retryAfterLeaderRecheck(call, rejection), rejection);
        }
    }

    /**
     * Runs {@code next} on the event loop after {@code delayNanos}, so a failed attempt never calls into the next
     * one. A loop that shuts down first drops it unrun, and the call ends with {@code rejection}.
     */
    private void scheduleNextAttempt(Call call, long delayNanos, Runnable next, Throwable rejection) {
        try {
            eventLoop.schedule(next, delayNanos, TimeUnit.NANOSECONDS).addListener(scheduled -> {
                if (scheduled.isCancelled()) {
                    call.end(rejection);
                }
            });
        } catch (RejectedExecutionException loopGone) {
            call.end(rejection);
        }
    }

    /** Starts the next attempt of the call on the connection that the leader recheck chose. */
    private void retryAfterLeaderRecheck(Call call, Throwable rejection) {
        if (call.over()) {
            call.end(rejection);
            return;
        }
        CompletableFuture<AsyncTcpConnection> next;
        try {
            next = transientFailoverHandler.recheck(this, call.deadline(), rejection, call.state());
        } catch (RuntimeException recheckError) {
            call.end(recheckError);
            return;
        }
        call.callerFuture().whenComplete((response, error) -> {
            if (call.callerFuture().isCancelled()) {
                next.cancel(false);
            }
        });
        next.whenComplete((target, recheckError) -> {
            if (recheckError != null) {
                call.end(call.state().signedOut() ? call.state().lineageEnded(recheckError) : recheckError);
            } else {
                target.attemptAgain(call, rejection);
            }
        });
    }

    /**
     * Sends a call that has not sent its frame again on this connection once a session is bound in its lineage, as
     * in Rust. The call keeps its lane meanwhile, so the calls behind it keep their order. A sign-in or a sign-out
     * of the caller ends the call with 40, and a session that is not bound by the deadline ends it with 58.
     */
    private void restartUnderSession(Call call) {
        CompletableFuture<Void> sessionBound =
                call.state().ticket.awaitSession(call.state().lineage, call.deadline(), eventLoop);
        call.callerFuture().whenComplete((response, error) -> {
            if (call.callerFuture().isCancelled()) {
                sessionBound.cancel(false);
            }
        });
        sessionBound.whenComplete((ignored, waitError) -> {
            if (waitError != null) {
                call.end(waitError);
            } else {
                Throwable refusal = notAccepted();
                scheduleNextAttempt(call, 0, () -> attemptAgain(call, refusal), refusal);
            }
        });
    }

    /**
     * Starts the next attempt of a call on this connection, unless its caller gave up or its deadline passed. Then
     * the call ends with {@code rejection}, the failure that the attempt was to retry.
     */
    private void attemptAgain(Call call, Throwable rejection) {
        if (call.over()) {
            call.end(rejection);
        } else {
            attempt(call, call.failoverPayload().retainedDuplicate(), 0);
        }
    }

    CompletableFuture<ByteBuf> sendAuthenticationFrame(
            Channel channel,
            AuthenticationSnapshot authentication,
            long requestDeadlineNanos,
            TransientFailoverState callerState) {
        ByteBuf payload = authentication.payload();
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
                                payload.release();
                                return signInAfterRefusedBind(channel, requestDeadlineNanos, callerState);
                            }
                        }
                        payload.release();
                        return CompletableFuture.<ByteBuf>failedFuture(error);
                    })
                    .thenCompose(Function.identity());
        }
        long generation = consensusSession.owner().generation();
        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        TransientFailoverState signInState = new TransientFailoverState();
        signInState.startIn(consensusSession);
        sendFrame(
                channel,
                payload,
                authentication.commandCode(),
                loginFuture,
                loginFuture,
                requestDeadlineNanos,
                signInState);
        return loginFuture.whenComplete((response, error) ->
                forgetRefusedSignIn(generation, authentication.version(), signInState, callerState, error));
    }

    /** Ends a refused automatic sign-in unless its credentials or owner have changed. */
    private void forgetRefusedSignIn(
            long generation,
            long loginVersion,
            TransientFailoverState signInState,
            TransientFailoverState callerState,
            Throwable error) {
        if (lostLink(signInState, unwrap(error)) || !AsyncIggyTcpClient.isSignInRejection(unwrap(error))) {
            return;
        }
        synchronized (consensusSession) {
            synchronized (consensusSession.owner()) {
                if (signInState.signedOut() || consensusSession.owner().generation() != generation) {
                    return;
                }
                if (!login.clear(loginVersion)) {
                    callerState.retryAuthentication = true;
                    return;
                }
                authenticated = false;
            }
        }
        if (consensusSession.refuseSignIn(generation, error)) {
            signInRefusedListener.accept(signInState.lineage + 1);
        }
    }

    /** Uses credentials after any change that committed while the refused bind awaited its reply. */
    private CompletableFuture<ByteBuf> signInAfterRefusedBind(
            Channel channel, long requestDeadlineNanos, TransientFailoverState callerState) {
        return authenticationSnapshot()
                .map(current -> sendAuthenticationFrame(channel, current, requestDeadlineNanos, callerState))
                .orElseGet(() -> CompletableFuture.failedFuture(IggyAuthenticator.notSignedIn(this)));
    }

    /** Lets the client redial after a pool acquire failure or expired reply. */
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

    /** Logs out first because a bound connection would replay its original Register identity. */
    private CompletableFuture<ByteBuf> logoutThenLogin(int commandCode, ByteBuf payload, long deadline) {
        // A sign-in ends the lineage once, as in Rust. The client already did it for a link that follows its session.
        if (consensusSession.owner() == consensusSession) {
            consensusSession.endLineage();
        }
        TransientFailoverState logout = new TransientFailoverState();
        logout.keepsLineage = true;
        return send(CommandCode.User.LOGOUT.getValue(), Unpooled.EMPTY_BUFFER, deadline, logout)
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

    static boolean isLoginCode(int commandCode) {
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

    boolean isAllowedBeforeAuthentication(int commandCode) {
        return commandCode == CommandCode.System.PING.getValue()
                || (sharedSession
                        && (commandCode == CommandCode.System.BIND_SESSION.getValue()
                                || isPartitionCommand(commandCode)));
    }

    private static boolean isOffsetWriteCode(int commandCode) {
        return commandCode == CommandCode.ConsumerOffset.STORE.getValue()
                || commandCode == CommandCode.ConsumerOffset.DELETE.getValue();
    }

    /** Writes the first attempt; an authentication step holds the response future as its caller future. */
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

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private void sendAdmittedFrame(
            Channel channel,
            ByteBuf payload,
            int commandCode,
            CompletableFuture<ByteBuf> responseFuture,
            CompletableFuture<ByteBuf> callerFuture,
            long requestDeadlineNanos,
            TransientFailoverState failoverState) {
        try {
            if (failoverState.ticket != null
                    && failoverState.ticket.prepareWrite(channel.isActive(), failoverState.lineage)) {
                failoverState.renewRequestId();
            }
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
                frame = encodeFrame(channel, commandCode, payload, failoverState);
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

    private ByteBuf encodeFrame(Channel channel, int commandCode, ByteBuf payload, TransientFailoverState state) {
        // Match the accessors' child-before-owner lock order so a caller change cannot splice two identities.
        synchronized (consensusSession) {
            synchronized (consensusSession.owner()) {
                if (state.signedOut()) {
                    throw state.lineageEnded(null);
                }
                return vsrEncoder.encode(channel.alloc(), commandCode, payload, state.context, state.requestId);
            }
        }
    }

    /** Replays transient refusals with the same dedup identity; committed lifecycle refusals need a new id. */
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
        if (exchange.failoverState().keepsLineage) {
            pending.keepLineage();
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
        if (exchange.failoverState().ticket != null
                && (isRefusal(serverError)
                        || (serverError != null && serverError.getRawErrorCode() == LIFECYCLE_BUSY)
                        || PollRouter.isNamespaceMiss(error))) {
            exchange.failoverState().ticket.settleSent(nextUncertain);
        }
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

    /** Closes a detached link before its caller retries by binding or registering on a replacement. */
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

    static void releaseIfPresent(ByteBuf payload) {
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

    /** Replaces a bind-refused session while preserving lane order and calls whose frames were not sent. */
    void resetRefusedSession(long clientLow, long clientHigh, int errorCode) {
        if (consensusSession.replaceLostSession(clientLow, clientHigh)) {
            onSessionEvicted(errorCode);
        }
    }

    /** Invalidates the transport attachment after eviction; only a definitive bind refusal replaces the session. */
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

    SessionLogin sessionLogin() {
        return login;
    }

    void followLogin(SessionLogin credentials) {
        login = credentials;
    }

    /** Refreshes the shared login from a committed typed, raw, or resolved credential-change frame. */
    private void refreshCredentials(int commandCode, ByteBuf frame) {
        if (authenticated) {
            login.refresh(commandCode, frame, authenticatedUserId)
                    .ifPresent(change -> credentialsListener.refreshed(
                            this, change.previousUsername(), change.username(), change.password()));
        }
    }

    public CompletableFuture<Void> close() {
        if (!isClosed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        heartbeat.stop();
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

    record AuthenticationSnapshot(int commandCode, ByteBuf payload, long version) {}

    /** One call across its attempts, which all send its failover payload again and complete its caller future. */
    record Call(
            int commandCode,
            ByteBuf failoverPayload,
            long deadline,
            CompletableFuture<ByteBuf> callerFuture,
            TransientFailoverState state) {
        boolean over() {
            return callerFuture.isDone() || deadline - System.nanoTime() <= 0;
        }

        /** Ends the call without another attempt. A routed call leaves its ticket to its router. */
        void fail(Throwable error) {
            if (state.routed) {
                callerFuture.completeExceptionally(error);
            } else {
                end(error);
            }
        }

        /**
         * Ends the call with {@code error}, or with its uncertainty when an attempt can have taken effect. A call
         * that a 58 or 89 refusal or a link lost before its frame went out sent to another attempt, and that its
         * deadline ends without another answer, reports 58, as the deadline of the Rust driver reports the refusal
         * of a call that no node admitted.
         */
        void end(Throwable error) {
            releaseIfPresent(failoverPayload);
            IggyServerException serverError = findServerError(error);
            Throwable outcome = state.refused
                            && deadline - System.nanoTime() <= 0
                            && (serverError == null || isRefusal(serverError))
                    ? notAccepted()
                    : error;
            if (state.ticket != null) {
                if (state.uncertain || callerFuture.isCancelled()) {
                    outcome = state.retainTicket();
                } else {
                    state.ticket.complete();
                    state.ticket = null;
                }
            }
            callerFuture.completeExceptionally(outcome);
        }
    }

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
        private boolean keepsLineage;
        private boolean retryAuthentication;
        private long requestId;
        private boolean committed;
        private boolean linkFailed;
        private boolean detachedSession;
        private boolean uncertain;
        private boolean everUncertain;
        // The last attempt got NOT_PRIMARY, so another one in a row pauses first, see pauseBeforeRecheck.
        private boolean notPrimary;
        // A 58 or 89 refusal or a link lost before the frame went out sent the call to another attempt, see Call#end.
        private boolean refused;
        private long pauseCeilingNanos = RETRY_BACKOFF_BASE.toNanos();
        private ConsensusSession.LaneKey lane;
        private ConsensusSession.Ticket ticket;
        private RuntimeException retainedError;
        private byte[] routingBody;
        private final Set<ConnectionInfo> visitedTargets = new HashSet<>();
        // The session whose lineage the call started in, see ConsensusSession#lineage. Every call that can hold a
        // ticket starts in one, and a sign-in, a ping or a frame of an authentication step needs none.
        private ConsensusSession lineageOwner;
        private long lineage;

        TransientFailoverState() {
            this(PartitionContext.EMPTY);
        }

        TransientFailoverState(PartitionContext context) {
            this.context = context;
        }

        long lineage() {
            return lineage;
        }

        /** Starts the call in the current lineage of {@code session}, unless an earlier attempt started it. */
        void startIn(ConsensusSession session) {
            if (lineageOwner == null) {
                lineageOwner = session;
                lineage = session.lineage();
            }
        }

        /** A fresh state for another history of the same call, which stays in the lineage that the call started in. */
        TransientFailoverState nextHistory() {
            TransientFailoverState next = new TransientFailoverState();
            next.lineageOwner = lineageOwner;
            next.lineage = lineage;
            return next;
        }

        /** Whether a sign-in or a sign-out of the caller ended the lineage that the call started in. */
        boolean signedOut() {
            return lineageOwner != null && lineageOwner.lineage() != lineage;
        }

        IggyServerException lineageEnded(Throwable cause) {
            return lineageOwner.lineageEnded(lineage, cause);
        }

        Throwable authenticationFailure(Throwable error) {
            return signedOut()
                    ? lineageEnded(error)
                    : unwrap(error) instanceof IggyTimeoutException ? notAccepted() : error;
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
