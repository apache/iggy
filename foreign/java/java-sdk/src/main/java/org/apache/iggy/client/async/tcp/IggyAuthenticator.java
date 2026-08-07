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
import org.apache.iggy.client.async.tcp.vsr.VsrRequestEncoder;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
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
     * @param encoder           encodes the login command into a VSR request frame
     * @return a future that completes when authentication is done
     */
    static CompletableFuture<Void> ensureAuthenticated(
            Channel channel,
            ByteBuf loginPayload,
            int commandCode,
            AtomicLong currentGeneration,
            VsrRequestEncoder encoder) {
        Long channelGeneration = channel.attr(AUTH_GENERATION_KEY).get();
        long requiredGeneration = currentGeneration.get();

        if (channelGeneration != null && channelGeneration == requiredGeneration) {
            loginPayload.release();
            return CompletableFuture.completedFuture(null);
        }

        ByteBuf frame;
        try {
            frame = encoder.encode(channel.alloc(), commandCode, loginPayload);
        } catch (RuntimeException encodeError) {
            loginPayload.release();
            return CompletableFuture.failedFuture(encodeError);
        }
        loginPayload.release();
        CompletableFuture<ByteBuf> loginFuture = new CompletableFuture<>();
        VsrResponseHandler handler = channel.pipeline().get(VsrResponseHandler.class);
        handler.enqueueRequest(loginFuture);
        channel.writeAndFlush(frame).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                loginFuture.completeExceptionally(f.cause());
                // A failed write leaves the future queued and the framing
                // undefined; close so pending requests fail instead of
                // matching later responses.
                channel.close();
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
