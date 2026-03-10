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
import org.apache.iggy.exception.IggyNotConnectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

final class IggyAuthenticator {
    private static final Logger log = LoggerFactory.getLogger(IggyAuthenticator.class);
    private static final AttributeKey<Long> AUTH_GENERATION_KEY = AttributeKey.valueOf("AUTH_GENERATION");

    private IggyAuthenticator() {}

    /**
     * Ensures the channel is authenticated for the current authentication generation.
     * If the channel's stored generation matches the current one, it is already authenticated.
     * Otherwise, sends a login command on the channel and updates the generation on success.
     *
     * @param channel           the channel to authenticate
     * @param loginPayload      the login payload to send (will be released by this method)
     * @param commandCode       the login command code
     * @param currentGeneration the current authentication generation counter
     * @return a future that completes when authentication is done
     */
    static CompletableFuture<Void> ensureAuthenticated(
            Channel channel, ByteBuf loginPayload, int commandCode, AtomicLong currentGeneration) {
        Long channelGeneration = channel.attr(AUTH_GENERATION_KEY).get();
        long requiredGeneration = currentGeneration.get();

        if (channelGeneration != null && channelGeneration == requiredGeneration) {
            loginPayload.release();
            return CompletableFuture.completedFuture(null);
        }

        if (loginPayload == null) {
            return CompletableFuture.failedFuture(new IggyNotConnectedException("Not authenticated, call login first"));
        }

        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        IggyResponseHandler handler = channel.pipeline().get(IggyResponseHandler.class);
        handler.enqueueRequest(loginFuture);
        ByteBuf frame = IggyFrameEncoder.encode(channel.alloc(), commandCode, loginPayload);
        loginPayload.release();
        channel.writeAndFlush(frame).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                loginFuture.completeExceptionally(f.cause());
            }
        });

        return loginFuture.thenAccept(result -> {
            try {
                channel.attr(AUTH_GENERATION_KEY).set(currentGeneration.get());
                log.debug("Channel {} authenticated successfully", channel.id());
            } finally {
                result.release();
            }
        });
    }

    static void setAuthGeneration(Channel channel, long generation) {
        channel.attr(AUTH_GENERATION_KEY).set(generation);
    }

    static void clearAuthGeneration(Channel channel) {
        channel.attr(AUTH_GENERATION_KEY).set(null);
    }
}
