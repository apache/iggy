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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.IoEventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.FutureListener;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyEmptyResponseException;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyNotConnectedException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTlsException;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Async TCP connection using Netty for non-blocking I/O.
 * Manages the connection lifecycle and request/response correlation.
 */
public class AsyncTcpConnection {
    private static final Logger log = LoggerFactory.getLogger(AsyncTcpConnection.class);

    private final IoEventLoopGroup eventLoopGroup;
    private final FixedChannelPool channelPool;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong authGeneration = new AtomicLong(0);
    private ByteBuf loginPayload;

    private volatile int loginCommandCode;
    private volatile boolean authenticated = false;

    public AsyncTcpConnection(
            String host,
            int port,
            boolean enableTls,
            Optional<File> tlsCertificate,
            TCPConnectionPoolConfig poolConfig) {
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

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

        var bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(host, port);

        this.channelPool = new FixedChannelPool(
                bootstrap,
                new PoolChannelHandler(host, port, enableTls, sslContext),
                ChannelHealthChecker.ACTIVE,
                FixedChannelPool.AcquireTimeoutAction.FAIL,
                poolConfig.getAcquireTimeoutMillis(),
                poolConfig.getMaxConnections(),
                poolConfig.getMaxPendingAcquires());

        log.info("Connection pool initialized with max connections: {}", poolConfig.getMaxConnections());
    }

    /**
     * Validates server reachability by eagerly acquiring and releasing one connection.
     */
    public CompletableFuture<Void> connect() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        channelPool.acquire().addListener((FutureListener<Channel>) f -> {
            if (f.isSuccess()) {
                channelPool.release(f.getNow());
                future.complete(null);
            } else {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
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
        captureLoginPayloadIfNeeded(commandCode, payload);
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();

        channelPool.acquire().addListener((FutureListener<Channel>) f -> {
            if (!f.isSuccess()) {
                payload.release();
                responseFuture.completeExceptionally(mapAcquireException(f.cause()));
                return;
            }

            Channel channel = f.getNow();
            boolean isLoginCommand = (commandCode == CommandCode.User.LOGIN.getValue()
                    || commandCode == CommandCode.PersonalAccessToken.LOGIN.getValue());
            boolean requiresAuth = !isLoginCommand
                    && commandCode != CommandCode.System.PING.getValue()
                    && commandCode != CommandCode.System.GET_STATS.getValue();

            responseFuture.handle((res, ex) -> {
                handlePostResponse(channel, commandCode, isLoginCommand, ex);
                return null;
            });

            CompletableFuture<Void> authStep;
            if (!requiresAuth) {
                authStep = CompletableFuture.completedFuture(null);
            } else if (!authenticated) {
                payload.release();
                responseFuture.completeExceptionally(
                        new IggyNotConnectedException("Not authenticated, call login first"));
                return;
            } else {
                ByteBuf loginPayloadCopy = getLoginPayloadCopy();
                if (loginPayloadCopy == null) {
                    payload.release();
                    responseFuture.completeExceptionally(
                            new IggyNotConnectedException("Not authenticated, call login first"));
                    return;
                }
                authStep = IggyAuthenticator.ensureAuthenticated(
                        channel, loginPayloadCopy, loginCommandCode, authGeneration);
            }

            authStep.thenRun(() -> sendFrame(channel, payload, commandCode, responseFuture))
                    .exceptionally(ex -> {
                        responseFuture.completeExceptionally(ex);
                        return null;
                    });
        });

        return responseFuture;
    }

    private static Throwable mapAcquireException(Throwable cause) {
        if (cause instanceof IllegalStateException) {
            return new IggyNotConnectedException("Connection pool is closed");
        }
        return cause;
    }

    private void sendFrame(
            Channel channel, ByteBuf payload, int commandCode, CompletableFuture<ByteBuf> responseFuture) {
        try {
            IggyResponseHandler handler = channel.pipeline().get(IggyResponseHandler.class);
            if (handler == null) {
                throw new IggyClientException("Channel missing IggyResponseHandler");
            }

            handler.enqueueRequest(responseFuture);
            ByteBuf frame = IggyFrameEncoder.encode(channel.alloc(), commandCode, payload);

            channel.writeAndFlush(frame).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.error("Failed to send frame: {}", future.cause().getMessage());
                    responseFuture.completeExceptionally(future.cause());
                } else {
                    log.trace("Frame sent successfully to {}", channel.remoteAddress());
                }
            });
        } catch (RuntimeException e) {
            responseFuture.completeExceptionally(e);
        } finally {
            payload.release();
        }
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
        channelPool.release(channel);
    }

    private void captureLoginPayloadIfNeeded(int commandCode, ByteBuf payload) {
        if (commandCode == CommandCode.User.LOGIN.getValue()
                || commandCode == CommandCode.PersonalAccessToken.LOGIN.getValue()) {
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

        PoolChannelHandler(String host, int port, boolean enableTls, SslContext sslContext) {
            this.host = host;
            this.port = port;
            this.enableTls = enableTls;
            this.sslContext = sslContext;
        }

        @Override
        public void channelCreated(Channel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            if (enableTls) {
                pipeline.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
            }
            pipeline.addLast("frameDecoder", new IggyFrameDecoder());
            pipeline.addLast("responseHandler", new IggyResponseHandler());
        }
    }

    public static class IggyResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final Queue<CompletableFuture<ByteBuf>> responseQueue = new ConcurrentLinkedQueue<>();

        public void enqueueRequest(CompletableFuture<ByteBuf> future) {
            responseQueue.add(future);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            int status = msg.readIntLE();
            int length = msg.readIntLE();

            CompletableFuture<ByteBuf> future = responseQueue.poll();

            if (future != null) {
                if (status == 0) {
                    future.complete(msg.retainedSlice());
                } else {
                    byte[] errorBytes = length > 0 ? new byte[length] : new byte[0];
                    msg.readBytes(errorBytes);
                    future.completeExceptionally(IggyServerException.fromTcpResponse(status, errorBytes));
                }
            } else {
                log.error(
                        "Received response on channel {} but no request was waiting!",
                        ctx.channel().id());
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            CompletableFuture<ByteBuf> f;
            while ((f = responseQueue.poll()) != null) {
                f.completeExceptionally(cause);
            }
            ctx.close();
        }
    }

    public static class TCPConnectionPoolConfig {
        private final int maxConnections;
        private final int maxPendingAcquires;
        private final long acquireTimeoutMillis;

        public TCPConnectionPoolConfig() {
            this(
                    Builder.DEFAULT_MAX_CONNECTION,
                    Builder.DEFAULT_MAX_PENDING_ACQUIRES,
                    Builder.DEFAULT_ACQUIRE_TIMEOUT_MILLIS);
        }

        public TCPConnectionPoolConfig(int maxConnections, int maxPendingAcquires, long acquireTimeoutMillis) {
            this.maxConnections = maxConnections;
            this.maxPendingAcquires = maxPendingAcquires;
            this.acquireTimeoutMillis = acquireTimeoutMillis;
        }

        public int getMaxConnections() {
            return this.maxConnections;
        }

        public int getMaxPendingAcquires() {
            return this.maxPendingAcquires;
        }

        public long getAcquireTimeoutMillis() {
            return this.acquireTimeoutMillis;
        }

        public static final class Builder {
            public static final int DEFAULT_MAX_CONNECTION = 5;
            public static final int DEFAULT_MAX_PENDING_ACQUIRES = 1000;
            public static final int DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 3000;

            private int maxConnections;
            private int maxPendingAcquires;
            private long acquireTimeoutMillis;

            public Builder() {}

            public Builder setMaxConnections(int maxConnections) {
                if (maxConnections <= 0) {
                    throw new IggyInvalidArgumentException("Connection pool size cannot be 0 or negative");
                }
                this.maxConnections = maxConnections;
                return this;
            }

            public Builder setMaxPendingAcquires(int maxPendingAcquires) {
                if (maxPendingAcquires <= 0) {
                    throw new IggyInvalidArgumentException("Max Pending Acquires cannot be 0 or negative");
                }
                this.maxPendingAcquires = maxPendingAcquires;
                return this;
            }

            public Builder setAcquireTimeoutMillis(long acquireTimeoutMillis) {
                if (acquireTimeoutMillis <= 0) {
                    throw new IggyInvalidArgumentException("Acquire timeout cannot be 0 or negative");
                }
                this.acquireTimeoutMillis = acquireTimeoutMillis;
                return this;
            }

            public TCPConnectionPoolConfig build() {
                if (this.maxConnections == 0) {
                    this.maxConnections = DEFAULT_MAX_CONNECTION;
                }
                if (this.acquireTimeoutMillis == 0) {
                    this.acquireTimeoutMillis = DEFAULT_ACQUIRE_TIMEOUT_MILLIS;
                }
                if (this.maxPendingAcquires == 0) {
                    this.maxPendingAcquires = DEFAULT_MAX_PENDING_ACQUIRES;
                }
                return new TCPConnectionPoolConfig(maxConnections, maxPendingAcquires, acquireTimeoutMillis);
            }
        }
    }
}
