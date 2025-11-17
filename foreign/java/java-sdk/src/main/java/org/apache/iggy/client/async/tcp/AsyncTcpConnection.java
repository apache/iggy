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
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Async TCP connection using Netty for non-blocking I/O.
 * Manages the connection lifecycle and request/response correlation.
 */
public class AsyncTcpConnection {
    private static final Logger logger = LoggerFactory.getLogger(AsyncTcpConnection.class);

    private final String host;
    private final int port;
    private final EventLoopGroup eventLoopGroup;
    private final Bootstrap bootstrap;
    private final ScheduledExecutorService scheduler;
    private Channel channel;
    private final AtomicLong requestIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, CompletableFuture<ByteBuf>> pendingRequests = new ConcurrentHashMap<>();
    private Duration requestTimeout = Duration.ofSeconds(30); // default timeout

    public AsyncTcpConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "AsyncTcpConnection-Timeout-" + hashCode());
            t.setDaemon(true);
            return t;
        });
        this.bootstrap = new Bootstrap();
        configureBootstrap();
    }

    private void configureBootstrap() {
        bootstrap.group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();

                    // Custom frame decoder for Iggy protocol responses
                    pipeline.addLast("frameDecoder", new IggyFrameDecoder());

                    // No encoder needed - we build complete frames following Iggy protocol
                    // The protocol already includes the length field, so adding an encoder
                    // would duplicate it. This matches the blocking client implementation.

                    // Response handler
                    pipeline.addLast("responseHandler",
                        new IggyResponseHandler(pendingRequests));
                }
            });
    }

    /**
     * Connects to the server asynchronously.
     */
    public CompletableFuture<Void> connect() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        bootstrap.connect(host, port).addListener((ChannelFutureListener) channelFuture -> {
            if (channelFuture.isSuccess()) {
                channel = channelFuture.channel();
                future.complete(null);
            } else {
                future.completeExceptionally(channelFuture.cause());
            }
        });

        return future;
    }

    /**
     * Sets the request timeout duration.
     */
    public void setRequestTimeout(Duration timeout) {
        this.requestTimeout = timeout;
    }

    /**
     * Sends a command asynchronously and returns the response.
     */
    public CompletableFuture<ByteBuf> sendAsync(int commandCode, ByteBuf payload) {
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Connection not established or closed"));
        }

        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();
        long requestId = requestIdGenerator.incrementAndGet();
        pendingRequests.put(requestId, responseFuture);

        int payloadSize = payload.readableBytes();
        int framePayloadSize = 4 + payloadSize;

        ByteBuf frame = channel.alloc().buffer(4 + framePayloadSize);
        frame.writeIntLE(framePayloadSize);
        frame.writeIntLE(commandCode);
        frame.writeBytes(payload, payload.readerIndex(), payloadSize);

        byte[] frameBytes = new byte[Math.min(frame.readableBytes(), 30)];
        if (logger.isTraceEnabled()) {
            frame.getBytes(0, frameBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : frameBytes) {
                hex.append(String.format("%02x ", b));
            }
            logger.trace("Sending frame with command: {}, payload size: {}, frame payload size (with command): {}, total frame size: {}",
                commandCode, payloadSize, framePayloadSize, frame.readableBytes());
            logger.trace("Frame bytes (hex): {}", hex.toString());
        }

        payload.release();

        // Schedule timeout to fail the request if no response is received
        scheduler.schedule(() -> {
            if (!responseFuture.isDone()) {
                CompletableFuture<ByteBuf> removed = pendingRequests.remove(requestId);
                if (removed != null) {
                    removed.completeExceptionally(
                        new java.util.concurrent.TimeoutException(
                            "Request timeout after " + requestTimeout.toMillis() + "ms"));
                }
            }
        }, requestTimeout.toMillis(), TimeUnit.MILLISECONDS);

        channel.writeAndFlush(frame).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                logger.error("Failed to send frame: {}", future.cause().getMessage());
                pendingRequests.remove(requestId);
                responseFuture.completeExceptionally(future.cause());
            } else {
                logger.trace("Frame sent successfully to {}", channel.remoteAddress());
            }
        });

        return responseFuture;
    }

    /**
     * Closes the connection and releases resources.
     * This method is idempotent - multiple calls are safe.
     */
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Shutdown scheduler
        scheduler.shutdown();

        if (channel != null && channel.isActive()) {
            channel.close().addListener((ChannelFutureListener) channelFuture -> {
                eventLoopGroup.shutdownGracefully();
                if (channelFuture.isSuccess()) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(channelFuture.cause());
                }
            });
        } else {
            if (!eventLoopGroup.isShuttingDown() && !eventLoopGroup.isShutdown()) {
                eventLoopGroup.shutdownGracefully();
            }
            future.complete(null);
        }

        return future;
    }

    /**
     * Response handler that correlates responses with requests.
     */
    private static class IggyResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final ConcurrentHashMap<Long, CompletableFuture<ByteBuf>> pendingRequests;

        public IggyResponseHandler(ConcurrentHashMap<Long, CompletableFuture<ByteBuf>> pendingRequests) {
            this.pendingRequests = pendingRequests;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            // Read response header (status and length only - no request ID)
            int status = msg.readIntLE();
            int length = msg.readIntLE();

            // Since Iggy doesn't use request IDs, we process responses in order
            // Get the oldest pending request
            if (!pendingRequests.isEmpty()) {
                Long oldestRequestId = pendingRequests.keySet().stream()
                    .min(Long::compare)
                    .orElse(null);

                if (oldestRequestId != null) {
                    CompletableFuture<ByteBuf> future = pendingRequests.remove(oldestRequestId);

                    if (status == 0) {
                        // Success - pass the remaining buffer as response
                        future.complete(msg.retainedSlice());
                    } else {
                        // Error - the payload contains the error message
                        if (length > 0) {
                            byte[] errorBytes = new byte[length];
                            msg.readBytes(errorBytes);
                            future.completeExceptionally(
                                new RuntimeException("Server error: " + new String(errorBytes)));
                        } else {
                            future.completeExceptionally(
                                new RuntimeException("Server error with status: " + status));
                        }
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Fail all pending requests
            pendingRequests.values().forEach(future ->
                future.completeExceptionally(cause));
            pendingRequests.clear();
            ctx.close();
        }
    }
}
