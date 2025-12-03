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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

final class InternalTcpClient {

    private static final int REQUEST_INITIAL_BYTES_LENGTH = 4;
    private static final int COMMAND_LENGTH = 4;
    private static final int RESPONSE_INITIAL_BYTES_LENGTH = 8;

    private final TcpClient client;
    private final BlockingQueue<IggyResponse> responses = new LinkedBlockingQueue<>();
    private Connection connection;

    InternalTcpClient(String host, Integer port) {
        this(host, port, false, Optional.empty());
    }

    InternalTcpClient(String host, Integer port, boolean enableTls, Optional<File> tlsCertificate) {
        TcpClient tcpClient = TcpClient.create()
                .host(host)
                .port(port)
                .doOnConnected(conn -> conn.addHandlerLast(new IggyResponseDecoder()));

        if (enableTls) {
            try {
                SslContextBuilder builder = SslContextBuilder.forClient();
                tlsCertificate.ifPresent(builder::trustManager);
                SslContext sslContext = builder.build();
                tcpClient = tcpClient.secure(sslSpec -> sslSpec.sslContext(sslContext));
            } catch (SSLException e) {
                throw new RuntimeException("Failed to configure TLS for TcpClient", e);
            }
        }

        client = tcpClient;
    }

    void connect() {
        this.connection = client.connectNow();
        this.connection.inbound().receiveObject().ofType(IggyResponse.class).subscribe(responses::add);
    }

    <T> List<T> exchangeForList(CommandCode command, Function<ByteBuf, T> responseMapper) {
        return exchangeForList(command, Unpooled.EMPTY_BUFFER, responseMapper);
    }

    <T> List<T> exchangeForList(CommandCode command, ByteBuf payload, Function<ByteBuf, T> responseMapper) {
        return exchange(command, payload, response -> {
            List<T> list = new ArrayList<>();
            var buf = response.payload();
            while (buf.isReadable()) {
                list.add(responseMapper.apply(buf));
            }
            return list;
        });
    }

    <T> Optional<T> exchangeForOptional(CommandCode command, ByteBuf payload, Function<ByteBuf, T> responseMapper) {
        return exchange(command, payload, response -> {
            ByteBuf buf = response.payload();
            if (!buf.isReadable()) {
                return Optional.empty();
            }
            return Optional.ofNullable(responseMapper.apply(buf));
        });
    }

    <T> T exchangeForEntity(CommandCode command, ByteBuf payload, Function<ByteBuf, T> responseMapper) {
        return exchange(command, payload, response -> {
            if (!response.payload().isReadable()) {
                throw new RuntimeException("Received an empty response for command " + command);
            }
            return responseMapper.apply(response.payload());
        });
    }

    void send(CommandCode command) {
        send(command, Unpooled.EMPTY_BUFFER);
    }

    void send(CommandCode command, ByteBuf payload) {
        exchange(command, payload, response -> null);
    }

    private <T> T exchange(CommandCode command, ByteBuf payload, Function<IggyResponse, T> responseMapper) {
        var payloadSize = payload.readableBytes() + COMMAND_LENGTH;
        var buffer = Unpooled.buffer(REQUEST_INITIAL_BYTES_LENGTH + payloadSize);
        buffer.writeIntLE(payloadSize);
        buffer.writeIntLE(command.getValue());
        buffer.writeBytes(payload);

        connection.outbound().send(Mono.just(buffer)).then().block();
        try {
            IggyResponse response = responses.take();
            if (response.status() != 0) {
                throw new RuntimeException("Received an invalid response with status " + response.status());
            }
            try {
                return responseMapper.apply(response);
            } finally {
                response.payload().release();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class IggyResponseDecoder extends ByteToMessageDecoder {
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
            list.add(new IggyResponse(status, length, byteBuf.readBytes(length)));
        }
    }

    record IggyResponse(long status, int length, ByteBuf payload) {}
}
