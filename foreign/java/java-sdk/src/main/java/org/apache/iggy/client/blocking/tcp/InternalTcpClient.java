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

package org.apache.iggy.client.blocking.tcp;

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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

final class InternalTcpClient {

    private static final int REQUEST_INITIAL_BYTES_LENGTH = 4;
    private static final int COMMAND_LENGTH = 4;
    private static final int RESPONSE_INITIAL_BYTES_LENGTH = 8;

    private final String host;
    private final int port;
    private final BlockingQueue<IggyResponse> responses = new LinkedBlockingQueue<>();
    private EventLoopGroup group;
    private Channel channel;

    InternalTcpClient(String host, Integer port) {
        this.host = host;
        this.port = port;
    }

    void connect() {
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

            // Connect synchronously since this is a blocking client
            channel = bootstrap.connect(host, port).sync().channel();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to connect", e);
        }
    }

    ByteBuf send(CommandCode code) {
        return send(code.getValue());
    }

    /** Use {@link #send(CommandCode)} instead. */
    @Deprecated
    ByteBuf send(int command) {
        return send(command, Unpooled.EMPTY_BUFFER);
    }

    ByteBuf send(CommandCode code, ByteBuf payload) {
        return send(code.getValue(), payload);
    }

    /** Use {@link #send(CommandCode, ByteBuf)} instead. */
    @Deprecated
    ByteBuf send(int command, ByteBuf payload) {
        var payloadSize = payload.readableBytes() + COMMAND_LENGTH;
        var buffer = Unpooled.buffer(REQUEST_INITIAL_BYTES_LENGTH + payloadSize);
        buffer.writeIntLE(payloadSize);
        buffer.writeIntLE(command);
        buffer.writeBytes(payload);

        // Send the buffer and wait for a response
        channel.writeAndFlush(buffer).syncUninterruptibly();
        try {
            IggyResponse response = responses.take();
            return handleResponse(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteBuf handleResponse(IggyResponse response) {
        if (response.status() != 0) {
            throw new RuntimeException("Received an invalid response with status " + response.status());
        }
        if (response.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        return response.payload();
    }

    class IggyResponseDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
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
            IggyResponse response = new IggyResponse(status, length, byteBuf.readBytes(length));
            list.add(response);
            responses.add(response); // Add the response to the responses queue
        }
    }

    record IggyResponse(long status, int length, ByteBuf payload) {
    }

}
