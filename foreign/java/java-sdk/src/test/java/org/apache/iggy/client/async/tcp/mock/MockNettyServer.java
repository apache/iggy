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

package org.apache.iggy.client.async.tcp.mock;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Mock Netty server for testing async TCP client error handling scenarios.
 * This server can simulate various network conditions and error responses.
 */
public class MockNettyServer {
    private static final Logger logger = LoggerFactory.getLogger(MockNettyServer.class);

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private int port;
    
    // Configurable behaviors
    private BiFunction<Integer, ByteBuf, ByteBuf> requestHandler;
    private Consumer<ChannelHandlerContext> connectionHandler;
    
    // Fault injection
    private boolean dropConnectionAfterBytes = false;
    private int bytesToSendBeforeDrop = 0;
    private Duration responseDelay = Duration.ZERO;
    private boolean sendMalformedResponse = false;
    private boolean acceptButNotRespond = false;
    private int statusCode = 0;
    private byte[] responsePayload = new byte[0];

    public MockNettyServer(int port) {
        this.port = port;
        this.requestHandler = (command, payload) -> {
            // Default handler - echo back the command
            return createResponse(0, new byte[0]);
        };
    }

    /**
     * Starts the mock server.
     *
     * @return a CompletableFuture that completes when the server is started
     */
    public CompletableFuture<Void> start() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        
                        // Add frame decoder and encoder for proper message framing
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));
                        
                        // Add our mock handler
                        pipeline.addLast(new MockServerHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
            
            // Bind and start to accept incoming connections
            ChannelFuture channelFuture = bootstrap.bind(port).sync();
            serverChannel = channelFuture.channel();
            
            logger.info("Mock server started on port {}", port);
            future.complete(null);
        } catch (Exception e) {
            logger.error("Failed to start mock server", e);
            future.completeExceptionally(e);
        }
        
        return future;
    }

    /**
     * Stops the mock server.
     *
     * @throws InterruptedException if interrupted while waiting for shutdown
     */
    public void stop() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully().sync();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
        }
        logger.info("Mock server stopped");
    }

    /**
     * Sets a custom request handler for processing incoming requests.
     *
     * @param handler the request handler function
     */
    public void setRequestHandler(BiFunction<Integer, ByteBuf, ByteBuf> handler) {
        this.requestHandler = handler;
    }

    /**
     * Sets a connection handler for processing new connections.
     *
     * @param handler the connection handler
     */
    public void setConnectionHandler(Consumer<ChannelHandlerContext> handler) {
        this.connectionHandler = handler;
    }

    /**
     * Configures the server to drop connections after sending a specific number of bytes.
     *
     * @param bytesToSend the number of bytes to send before dropping the connection
     */
    public void setDropConnectionAfterBytes(int bytesToSend) {
        this.dropConnectionAfterBytes = true;
        this.bytesToSendBeforeDrop = bytesToSend;
    }

    /**
     * Configures the server to add a delay before responding.
     *
     * @param delay the delay duration
     */
    public void setResponseDelay(Duration delay) {
        this.responseDelay = delay;
    }

    /**
     * Configures the server to send malformed responses.
     */
    public void setSendMalformedResponse() {
        this.sendMalformedResponse = true;
    }

    /**
     * Configures the server to accept connections but not respond to requests.
     */
    public void setAcceptButNotRespond() {
        this.acceptButNotRespond = true;
    }

    /**
     * Configures the server to send a specific response.
     *
     * @param statusCode the status code
     * @param payload the response payload
     */
    public void setResponse(int statusCode, byte[] payload) {
        this.statusCode = statusCode;
        this.responsePayload = payload != null ? payload : new byte[0];
    }

    /**
     * Creates a response frame.
     *
     * @param status the status code
     * @param payload the response payload
     * @return a ByteBuf containing the response frame
     */
    private ByteBuf createResponse(int status, byte[] payload) {
        int length = payload != null ? payload.length : 0;
        // Note: This method should only be called from within a handler context
        // For now, we'll create an unpooled buffer
        ByteBuf response = io.netty.buffer.Unpooled.buffer(8 + length);
        response.writeIntLE(status);
        response.writeIntLE(length);
        if (payload != null && length > 0) {
            response.writeBytes(payload);
        }
        return response;
    }

    /**
     * Creates an error response frame with status code and error message.
     *
     * @param statusCode the error status code
     * @param payload the error message payload
     * @return a ByteBuf containing the error response frame
     */
    private ByteBuf createErrorResponse(int statusCode, byte[] payload) {
        return createResponse(statusCode, payload);
    }

    /**
     * Netty handler for processing incoming connections and requests.
     */
    private class MockServerHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            if (acceptButNotRespond) {
                // Just consume the message and don't respond
                logger.debug("Accepting message but not responding (simulating timeout)");
                return;
            }

            if (sendMalformedResponse) {
                // Send a malformed response (incomplete frame)
                ByteBuf malformed = ctx.alloc().buffer(4);
                malformed.writeIntLE(0); // Only status, no length
                ctx.writeAndFlush(malformed);
                return;
            }

            // Parse the request: [length:4][command:4][payload:N]
            if (msg.readableBytes() < 8) {
                logger.warn("Received incomplete request frame");
                return;
            }

            int frameLength = msg.readIntLE();
            int command = msg.readIntLE();
            ByteBuf payload = msg.readSlice(frameLength - 4); // Remaining bytes after command

            // If statusCode is not 0, send error response directly
            if (statusCode != 0) {
                ByteBuf errorResponse = createErrorResponse(statusCode, responsePayload);
                
                if (dropConnectionAfterBytes) {
                    // Send partial response and then drop connection
                    if (errorResponse.readableBytes() > bytesToSendBeforeDrop) {
                        ByteBuf partial = errorResponse.readSlice(bytesToSendBeforeDrop);
                        ctx.writeAndFlush(partial).addListener(ChannelFutureListener.CLOSE);
                        return;
                    }
                }

                if (!responseDelay.isZero()) {
                    // Add delay before responding
                    Thread.sleep(responseDelay.toMillis());
                }

                ctx.writeAndFlush(errorResponse);
                return;
            }

            // Process the request using the configured handler
            ByteBuf response = requestHandler.apply(command, payload);

            if (dropConnectionAfterBytes) {
                // Send partial response and then drop connection
                if (response.readableBytes() > bytesToSendBeforeDrop) {
                    ByteBuf partial = response.readSlice(bytesToSendBeforeDrop);
                    ctx.writeAndFlush(partial).addListener(ChannelFutureListener.CLOSE);
                    return;
                }
            }

            if (!responseDelay.isZero()) {
                // Add delay before responding
                Thread.sleep(responseDelay.toMillis());
            }

            // Send the response
            ctx.writeAndFlush(response);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.debug("New client connection from {}", ctx.channel().remoteAddress());
            if (connectionHandler != null) {
                connectionHandler.accept(ctx);
            }
            super.channelActive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception in mock server handler", cause);
            ctx.close();
        }
    }
}