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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
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
import java.util.function.Function;

/**
 * Async TCP connection using Netty for non-blocking I/O.
 * Manages the connection lifecycle and request/response correlation.
 */
public class AsyncTcpConnection {
    private static final Logger log = LoggerFactory.getLogger(AsyncTcpConnection.class);
    private final String host;
    private final int port;
    private final boolean enableTls;
    private final Optional<File> tlsCertificate;
    private final SslContext sslContext;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private SimpleChannelPool channelPool;
    private final TCPConnectionPoolConfig poolConfig;
    private ByteBuf loginPayload;
    private AtomicBoolean isAuthenticated = new AtomicBoolean(false);

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public AsyncTcpConnection(String host, int port) {
        this(host, port, false, Optional.empty(), new TCPConnectionPoolConfig());
    }

    public AsyncTcpConnection(
            String host,
            int port,
            boolean enableTls,
            Optional<File> tlsCertificate,
            TCPConnectionPoolConfig poolConfig) {
        this.host = host;
        this.port = port;
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;
        this.poolConfig = poolConfig;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();

        if (this.enableTls) {
            try {
                SslContextBuilder builder = SslContextBuilder.forClient();
                this.tlsCertificate.ifPresent(builder::trustManager);
                this.sslContext = builder.build();
            } catch (SSLException e) {
                throw new IggyTlsException("Failed to build SSL context for AsyncTcpConnection", e);
            }
        } else {
            this.sslContext = null;
        }
        configureBootstrap();
    }

    private void configureBootstrap() {
        bootstrap
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .remoteAddress(this.host, this.port);
    }

    /**
     * Initialises Connection pool.
     */
    public CompletableFuture<Void> connect() {
        if (isClosed.get()) {
            return CompletableFuture.failedFuture(new IggyClientException("Client is Closed"));
        }
        AbstractChannelPoolHandler poolHandler = new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                if (enableTls) {
                    // adding ssl if ssl enabled
                    pipeline.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
                }
                // Adding the FrameDecoder to end of channel pipeline
                pipeline.addLast("frameDecoder", new IggyFrameDecoder());

                // Adding Response Handler Now Stateful
                pipeline.addLast("responseHandler", new IggyResponseHandler());
            }

            @Override
            public void channelAcquired(Channel ch) {
                IggyResponseHandler handler = ch.pipeline().get(IggyResponseHandler.class);
                handler.setPool(channelPool);
            }
        };

        this.channelPool = new FixedChannelPool(
                bootstrap,
                poolHandler,
                ChannelHealthChecker.ACTIVE, // Check If the connection is Active Before Lending
                FixedChannelPool.AcquireTimeoutAction.FAIL, // Fail If we take too long
                poolConfig.getAcquireTimeoutMillis(),
                poolConfig.getMaxConnections(),
                poolConfig.getMaxPendingAcquires());
        log.info("Connection pool initialized with max connections: {}", poolConfig.getMaxConnections());
        return CompletableFuture.completedFuture(null);
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
        return send(commandCode, payload).thenAccept(response -> response.release());
    }

    public CompletableFuture<ByteBuf> send(CommandCode commandCode, ByteBuf payload) {
        return send(commandCode.getValue(), payload);
    }

    /**
     * Sends a command asynchronously and returns the response.
     * Uses Netty's EventLoop to ensure thread-safe sequential request processing with FIFO response matching.
     */
    public CompletableFuture<ByteBuf> send(int commandCode, ByteBuf payload) {
        if (isClosed.get()) {
            return CompletableFuture.failedFuture(
                    new IggyNotConnectedException("Connection not established or closed"));
        }
        if (channelPool == null) {
            return CompletableFuture.failedFuture(
                    new IggyNotConnectedException("Connection not established or closed"));
        }

        captureLoginPayloadIfNeeded(commandCode, payload);
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();

        channelPool.acquire().addListener((FutureListener<Channel>) f -> {
            if (!f.isSuccess()) {
                responseFuture.completeExceptionally(f.cause());
                return;
            }

            Channel channel = f.getNow();
            if (Boolean.FALSE.equals(isAuthenticated.get())) {
                IggyAuthenticator.setAuthAttribute(channel, isAuthenticated);
            }
            CompletableFuture<Void> authStep;
            boolean isLoginOp = (commandCode == CommandCode.User.LOGIN.getValue()
                    || commandCode == CommandCode.PersonalAccessToken.LOGIN.getValue());

            if (isLoginOp) {
                authStep = CompletableFuture.completedFuture(null);
            } else {
                if (loginPayload == null) {
                    responseFuture.completeExceptionally(new IggyNotConnectedException("Login First"));
                }
                authStep = IggyAuthenticator.ensureAuthenticated(
                        channel, loginPayload.retainedDuplicate(), CommandCode.User.LOGIN.getValue());
            }

            authStep.thenRun(() -> sendFrame(channel, payload, commandCode, responseFuture))
                    .exceptionally(ex -> {
                        payload.release();
                        responseFuture.completeExceptionally(ex);
                        return null;
                    });

            responseFuture.handle((res, ex) -> {
                handlePostResponse(channel, commandCode, isLoginOp, ex);
                return null;
            });
        });

        return responseFuture;
    }

    private void sendFrame(
            Channel channel, ByteBuf payload, int commandCode, CompletableFuture<ByteBuf> responseFuture) {
        try {

            IggyResponseHandler handler = channel.pipeline().get(IggyResponseHandler.class);
            if (handler == null) {
                throw new IggyClientException("Channel missing IggyResponseHandler");
            }

            // Enqueuing request so handler knows who to call back;
            handler.enqueueRequest(responseFuture);

            ByteBuf frame = IggyFrameEncoder.encode(channel.alloc(), commandCode, payload);

            payload.release();

            // Send the frame
            channel.writeAndFlush(frame).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    log.error("Failed to send frame: {}", future.cause().getMessage());
                    frame.release();
                    channel.close();
                    responseFuture.completeExceptionally(future.cause());
                } else {
                    log.trace("Frame sent successfully to {}", channel.remoteAddress());
                }
            });

        } catch (RuntimeException e) {
            responseFuture.completeExceptionally(e);
        }
    }

    private void handlePostResponse(Channel channel, int commandCode, boolean isLoginOp, Throwable ex) {
        if (isLoginOp) {
            if (ex == null) {
                isAuthenticated.set(true);
            } else {
                releaseLoginPayload();
            }
        }
        if (commandCode == CommandCode.User.LOGOUT.getValue()) {
            isAuthenticated.set(false);
            IggyAuthenticator.setAuthAttribute(channel, isAuthenticated);
        }
        if (channelPool != null) {
            channelPool.release(channel);
        }
    }

    private void captureLoginPayloadIfNeeded(int commandCode, ByteBuf payload) {
        if (commandCode == CommandCode.User.LOGIN.getValue() || commandCode == CommandCode.User.UPDATE.getValue()) {
            updateLoginPayload(payload);
        }
    }

    private synchronized void updateLoginPayload(ByteBuf payload) {
        if (this.loginPayload != null) {
            loginPayload.release();
        }
        this.loginPayload = payload.retainedSlice();
    }

    private synchronized void releaseLoginPayload() {
        if (this.loginPayload != null) {
            loginPayload.release();
            this.loginPayload = null;
        }
    }

    /**
     * Closes the connection and releases resources.
     */
    public CompletableFuture<Void> close() {
        if (isClosed.compareAndSet(false, true)) {
            if (channelPool != null) {
                channelPool.close();
            }
            CompletableFuture<Void> shutdownFuture = new CompletableFuture<>();
            eventLoopGroup.shutdownGracefully().addListener(f -> {
                if (f.isSuccess()) {
                    shutdownFuture.complete(null);
                } else {
                    shutdownFuture.completeExceptionally(null);
                }
            });
            return shutdownFuture;
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Response handler that correlates responses with requests.
     */
    public static class IggyResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final Queue<CompletableFuture<ByteBuf>> responseQueue = new ConcurrentLinkedQueue<>();
        private SimpleChannelPool pool;

        public IggyResponseHandler() {
            this.pool = null;
        }

        public void setPool(SimpleChannelPool pool) {
            this.pool = pool;
        }

        public void enqueueRequest(CompletableFuture<ByteBuf> future) {
            responseQueue.add(future);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            // Read response header (status and length only - no request ID)
            int status = msg.readIntLE();
            int length = msg.readIntLE();

            CompletableFuture<ByteBuf> future = responseQueue.poll();

            if (future != null) {

                if (status == 0) {
                    // Success - pass the remaining buffer as response
                    future.complete(msg.retainedSlice());
                } else {
                    // Error - the payload contains the error message

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
            // If the connection dies, fail ALL waiting requests for this connection
            CompletableFuture<ByteBuf> f;
            while ((f = responseQueue.poll()) != null) {
                f.completeExceptionally(cause);
            }
            if (pool != null) {
                pool.release(ctx.channel());
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

        // Builder Class for TCPConnectionPoolConfig
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
