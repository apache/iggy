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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.AttributeKey;
import org.apache.iggy.client.async.tcp.AsyncTcpConnection.IggyResponseHandler;
import org.apache.iggy.exception.IggyAuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public final class IggyAuthenticator {
    private static final AttributeKey<Boolean> AUTH_KEY = AttributeKey.valueOf("AUTH_KEY");
    private static final Logger log = LoggerFactory.getLogger(IggyAuthenticator.class);

    private IggyAuthenticator() {}

    public static CompletableFuture<Void> ensureAuthenticated(Channel channel, ByteBuf loginPayload, int commandCode) {
        Boolean isAuth = channel.attr(AUTH_KEY).get();
        if (Boolean.TRUE.equals(isAuth)) {
            return CompletableFuture.completedFuture(null);
        }
        if (loginPayload.equals(null)) {
            return CompletableFuture.failedFuture(
                    new IggyAuthenticationException(null, commandCode, "login first", null, null));
        }

        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        IggyResponseHandler handler = channel.pipeline().get(IggyResponseHandler.class);
        handler.enqueueRequest(loginFuture);
        ByteBuf frame = IggyFrameEncoder.encode(channel.alloc(), commandCode, loginPayload);
        loginPayload.release();
        channel.writeAndFlush(frame).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                frame.release();
                loginFuture.completeExceptionally(f.cause());
            }
        });

        return loginFuture.thenAccept(result -> {
            try {
                channel.attr(AUTH_KEY).set(true);
                log.debug("Channel {} authenticated successfully", channel.id());
            } finally {
                result.release();
            }
        });
    }

    public static void setAuthAttribute(Channel channel, AtomicBoolean value) {
        channel.attr(AUTH_KEY).set(value.get());
    }

    public static Boolean getAuthAttribute(Channel channel) {
        return channel.attr(AUTH_KEY).get();
    }
}
