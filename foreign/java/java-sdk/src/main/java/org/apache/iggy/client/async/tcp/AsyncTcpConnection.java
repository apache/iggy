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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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
    private Channel channel;
    private final AtomicLong requestIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<Long, CompletableFuture<ByteBuf>> pendingRequests = new ConcurrentHashMap<>();

    public AsyncTcpConnection(String host, int port) {
        this(host, port, false, Optional.empty());
    }

    public AsyncTcpConnection(String host, int port, boolean enableTls, Optional<File> tlsCertificate) {
        this.host = host;
        this.port = port;
        this.enableTls = enableTls;
        this.tlsCertificate = tlsCertificate;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap();
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
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        if (enableTls) {
                            pipeline.addLast("ssl", sslContext.newHandler(ch.alloc(), host, port));
                        }

                        // Custom frame decoder for Iggy protocol responses
                        pipeline.addLast("frameDecoder", new IggyFrameDecoder());

                        // No encoder needed - we build complete frames following Iggy protocol
                        // The protocol already includes the length field, so adding an encoder
                        // would duplicate it. This matches the blocking client implementation.

                        // Response handler
                        pipeline.addLast("responseHandler", new IggyResponseHandler(pendingRequests));
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
     * Sends a command asynchronously and returns the response.
     */
    public CompletableFuture<ByteBuf> sendAsync(int commandCode, ByteBuf payload) {
        if (channel == null || !channel.isActive()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Connection not established or closed"));
        }

        // Since Iggy doesn't use request IDs, we'll just use a simple queue
        // Each request will get the next response in order
        CompletableFuture<ByteBuf> responseFuture = new CompletableFuture<>();
        long requestId = requestIdGenerator.incrementAndGet();
        pendingRequests.put(requestId, responseFuture);

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
                pendingRequests.remove(requestId);
                responseFuture.completeExceptionally(future.cause());
            } else {
                log.trace("Frame sent successfully to {}", channel.remoteAddress());
            }
        });

        return responseFuture;
    }

    /**
     * Closes the connection and releases resources.
     */
    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();

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
            eventLoopGroup.shutdownGracefully();
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
                Long oldestRequestId =
                        pendingRequests.keySet().stream().min(Long::compare).orElse(null);

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
                            future.completeExceptionally(new RuntimeException("Server error with status: " + status));
                        }
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Fail all pending requests
            pendingRequests.values().forEach(future -> future.completeExceptionally(cause));
            pendingRequests.clear();
            ctx.close();
        }
    }
}
