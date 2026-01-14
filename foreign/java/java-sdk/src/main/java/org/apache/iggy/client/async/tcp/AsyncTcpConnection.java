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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
    // private Channel channel;
    // private ChannelHealthChecker channelHealthChecker;

    // Pooling System;
    private SimpleChannelPool channelPool;
    private final TCPConnectionPoolConfig poolConfig;
    private final PoolMetrics poolMetrics;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    // private final AtomicLong requestIdGenerator = new AtomicLong(0);
    // private final ConcurrentHashMap<Long, CompletableFuture<ByteBuf>> pendingRequests = new ConcurrentHashMap<>();

    public AsyncTcpConnection(String host, int port) {
        this(host, port, false, Optional.empty(), new TCPConnectionPoolConfig(5, 1000, 1000));
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
        this.poolMetrics = new PoolMetrics();

        if (this.enableTls) {
            try {
                SslContextBuilder builder = SslContextBuilder.forClient();
                this.tlsCertificate.ifPresent(builder::trustManager);
                this.sslContext = builder.build();
            } catch (SSLException e) {
                throw new RuntimeException("Failed to build SSL context for AsyncTcpConnection", e);
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
            return CompletableFuture.failedFuture(new IllegalStateException("Client is Closed"));
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

                // Adding Response Handler Now Statefull
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

    /**
     * Returns Pool metrics.
     */
    public PoolMetrics getMetrics() {
        return this.poolMetrics;
    }

    /**
     * Sends a command asynchronously and returns the response.
     * Uses Netty's EventLoop to ensure thread-safe sequential request processing with FIFO response matching.
     */
    public CompletableFuture<ByteBuf> sendAsync(int commandCode, ByteBuf payload) {
        if (isClosed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        if (channelPool == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client not connected call connect first"));
        }

        // Starting the response clock
        long starttime = System.nanoTime();

        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();

        channelPool.acquire().addListener((FutureListener<Channel>) f -> {

            // Stoping the watch to record waitime
            long waitTime = System.nanoTime() - starttime;
            poolMetrics.recordWaitTime(waitTime);

            if (!f.isSuccess()) {
                poolMetrics.recordError();
                responseFuture.completeExceptionally(f.cause());
                return;
            }

            // Connection Aquired
            poolMetrics.incrementActive();

            Channel channel = f.getNow();
            try {
                IggyResponseHandler handler = channel.pipeline().get(IggyResponseHandler.class);

                CompletableFuture<ByteBuf> trackedFuture = responseFuture.whenComplete((res, ex) -> {
                    poolMetrics.decrementActive();
                });
                if (handler == null) {
                    throw new IllegalStateException("Channel missing IggyResponseHandler");
                }

                // Enqueuing request so handler knows who to call back;
                handler.enqueueRequest(responseFuture);

                // Build the request frame exactly like the blocking client
                // Frame format: [payload_size:4][command:4][payload:N]
                // where payload_size = 4 (command size) + N (payload size)
                int payloadSize = payload.readableBytes();
                int framePayloadSize = 4 + payloadSize; // command (4 bytes) + payload

                ByteBuf frame = channel.alloc().buffer(4 + framePayloadSize);
                frame.writeIntLE(framePayloadSize); // Length field (includes command)
                frame.writeIntLE(commandCode); // Command
                frame.writeBytes(payload, payload.readerIndex(), payloadSize); // Payload

                // Debug: print frame bytes
                byte[] frameBytes = new byte[Math.min(frame.readableBytes(), 30)];
                if (log.isTraceEnabled()) {
                    frame.getBytes(0, frameBytes);
                    StringBuilder hex = new StringBuilder();
                    for (byte b : frameBytes) {
                        hex.append(String.format("%02x ", b));
                    }
                    log.trace(
                            "Sending frame with command: {}, payload size: {}, frame payload size (with command): {}, total frame size: {}",
                            commandCode,
                            payloadSize,
                            framePayloadSize,
                            frame.readableBytes());
                    log.trace("Frame bytes (hex): {}", hex.toString());
                }

                payload.release();

                // Send the frame
                channel.writeAndFlush(frame).addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        log.error("Failed to send frame: {}", future.cause().getMessage());
                        channel.close();
                        channelPool.release(channel);
                        poolMetrics.recordError();
                        responseFuture.completeExceptionally(future.cause());
                    } else {
                        log.trace("Frame sent successfully to {}", channel.remoteAddress());
                    }
                });

            } catch (RuntimeException e) {
                channelPool.release(channel);
                responseFuture.completeExceptionally(e);
            }
        });

        return responseFuture;
    }

    /**
     * Closes the connection and releases resources.
     */
    public CompletableFuture<Void> close() {
        if (isClosed.compareAndSet(false, true)) {
            if (channelPool != null) {
                channelPool.close();
            }
            return CompletableFuture.runAsync(eventLoopGroup::shutdownGracefully);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Response handler that correlates responses with requests.
     */
    private static class IggyResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
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
                    if (length > 0) {
                        byte[] errorBytes = new byte[length];
                        msg.readBytes(errorBytes);
                        future.completeExceptionally(new RuntimeException("Server error: " + new String(errorBytes)));
                    } else {
                        future.completeExceptionally(new RuntimeException("Server error with status: " + status));
                    }
                }
            } else {
                log.error(
                        "Received response on channel {} but no request was waiting!",
                        ctx.channel().id());
            }
            if (pool != null) {
                pool.release(ctx.channel());
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

    // Inner Class for Channel pool configurations
    public static class TCPConnectionPoolConfig {
        private final int maxConnections;
        private final int maxPendingAcquires;
        private final long acquireTimeoutMillis;

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
            private int maxConnections = 5;
            private int maxPendingAcquires = 1000;
            private long acquireTimeoutMillis = 5000;

            public Builder() {}

            public Builder setMaxConnections(int maxConnections) {
                this.maxConnections = maxConnections;
                return this;
            }

            public Builder setMaxPendingAcquires(int maxPendingAcquires) {
                this.maxPendingAcquires = maxPendingAcquires;
                return this;
            }

            public Builder setAcquireTimeoutMillis(long acquireTimeoutMillis) {
                this.acquireTimeoutMillis = acquireTimeoutMillis;
                return this;
            }

            public TCPConnectionPoolConfig build() {
                return new TCPConnectionPoolConfig(maxConnections, maxPendingAcquires, acquireTimeoutMillis);
            }
        }
    }
}
