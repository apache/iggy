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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.iggy.client.blocking.tcp.CommandCode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal asynchronous TCP client for Iggy.
 * This class handles the low-level TCP communication with the server.
 */
final class InternalAsyncTcpClient {

    private static final int REQUEST_INITIAL_BYTES_LENGTH = 4;
    private static final int COMMAND_LENGTH = 4;
    private static final int RESPONSE_INITIAL_BYTES_LENGTH = 8;

    private final String host;
    private final int port;
    private final Map<Long, CompletableFuture<IggyResponse>> responseMap = new ConcurrentHashMap<>();
    private final AtomicLong requestIdGenerator = new AtomicLong(0);
    private EventLoopGroup group;
    private Channel channel;

    /**
     * Creates a new InternalAsyncTcpClient.
     *
     * @param host the server host
     * @param port the server port
     */
    InternalAsyncTcpClient(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the server.
     *
     * @return a CompletableFuture that completes when the connection is established
     */
    CompletableFuture<Void> connect() {
        CompletableFuture<Void> connectFuture = new CompletableFuture<>();

        group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new IggyResponseDecoder());
                        }
                    });

            bootstrap.connect(host, port).addListener(future -> {
                if (future.isSuccess()) {
                    channel = ((io.netty.channel.ChannelFuture) future).channel();
                    connectFuture.complete(null);
                } else {
                    connectFuture.completeExceptionally(future.cause());
                }
            });
        } catch (Exception e) {
            connectFuture.completeExceptionally(e);
        }

        return connectFuture;
    }

    /**
     * Sends a command to the server.
     *
     * @param code the command code
     * @return a CompletableFuture that emits the response
     */
    CompletableFuture<ByteBuf> send(CommandCode code) {
        return send(code.getValue());
    }

    /**
     * Sends a command to the server.
     *
     * @param command the command code
     * @return a CompletableFuture that emits the response
     */
    CompletableFuture<ByteBuf> send(int command) {
        return send(command, Unpooled.EMPTY_BUFFER);
    }

    /**
     * Sends a command with a payload to the server.
     *
     * @param code    the command code
     * @param payload the payload
     * @return a CompletableFuture that emits the response
     */
    CompletableFuture<ByteBuf> send(CommandCode code, ByteBuf payload) {
        return send(code.getValue(), payload);
    }

    /**
     * Sends a command with a payload to the server.
     *
     * @param command the command code
     * @param payload the payload
     * @return a CompletableFuture that emits the response
     */
    CompletableFuture<ByteBuf> send(int command, ByteBuf payload) {
        long requestId = requestIdGenerator.incrementAndGet();
        var payloadSize = payload.readableBytes() + COMMAND_LENGTH;
        var buffer = Unpooled.buffer(REQUEST_INITIAL_BYTES_LENGTH + payloadSize);
        buffer.writeIntLE(payloadSize);
        buffer.writeIntLE(command);
        buffer.writeBytes(payload);

        CompletableFuture<IggyResponse> responseFuture = new CompletableFuture<>();
        responseMap.put(requestId, responseFuture);

        CompletableFuture<ByteBuf> resultFuture = responseFuture
                .thenApply(this::handleResponse)
                .exceptionally(e -> {
                    responseMap.remove(requestId);
                    throw new RuntimeException("Error processing response", e);
                });

        channel.writeAndFlush(buffer).addListener(future -> {
            if (!future.isSuccess()) {
                responseMap.remove(requestId);
                resultFuture.completeExceptionally(future.cause());
            }
        });

        return resultFuture;
    }

    /**
     * Handles the response from the server.
     *
     * @param response the response
     * @return the response payload
     */
    private ByteBuf handleResponse(IggyResponse response) {
        if (response.status() != 0) {
            throw new RuntimeException("Received an invalid response with status " + response.status());
        }
        if (response.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return response.payload();
    }

    /**
     * Decoder for Iggy responses.
     */
    class IggyResponseDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list) {
            if (byteBuf.readableBytes() < RESPONSE_INITIAL_BYTES_LENGTH) {
                return;
            }
            byteBuf.markReaderIndex();
            var status = byteBuf.readUnsignedIntLE();
            var responseLength = byteBuf.readUnsignedIntLE();
            if (byteBuf.readableBytes() < responseLength) {
                byteBuf.resetReaderIndex();
                return;
            }
            var length = Long.valueOf(responseLength).intValue();
            // Use the current request ID - in a real implementation, this would be extracted from the response
            long requestId = requestIdGenerator.get();
            IggyResponse response = new IggyResponse(requestId, status, length, byteBuf.readBytes(length));

            CompletableFuture<IggyResponse> future = responseMap.remove(requestId);
            if (future != null) {
                future.complete(response);
            }
        }
    }

    /**
     * Represents a response from the Iggy server.
     *
     * @param requestId the request ID
     * @param status    the status code
     * @param length    the payload length
     * @param payload   the payload
     */
    record IggyResponse(long requestId, long status, int length, ByteBuf payload) {
    }
}
