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

import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;

import java.util.function.Consumer;
import java.util.function.IntConsumer;

final class PoolChannelHandler extends AbstractChannelPoolHandler {
    private final String host;
    private final int port;
    private final boolean enableTls;
    private final SslContext sslContext;
    private final long dialTimeoutMillis;
    private final ConsensusSession consensusSession;
    private final int maxVsrFrameSize;
    private final IntConsumer onEviction;
    private final Consumer<Channel> onChannelCreated;

    @SuppressWarnings("checkstyle:ParameterNumber")
    PoolChannelHandler(
            String host,
            int port,
            boolean enableTls,
            SslContext sslContext,
            long dialTimeoutMillis,
            ConsensusSession consensusSession,
            int maxVsrFrameSize,
            IntConsumer onEviction,
            Consumer<Channel> onChannelCreated) {
        this.host = host;
        this.port = port;
        this.enableTls = enableTls;
        this.sslContext = sslContext;
        this.dialTimeoutMillis = dialTimeoutMillis;
        this.consensusSession = consensusSession;
        this.maxVsrFrameSize = maxVsrFrameSize;
        this.onEviction = onEviction;
        this.onChannelCreated = onChannelCreated;
    }

    @Override
    public void channelCreated(Channel ch) {
        onChannelCreated.accept(ch);
        ChannelPipeline pipeline = ch.pipeline();
        if (enableTls) {
            SslHandler ssl = sslContext.newHandler(ch.alloc(), host, port);
            // A peer that accepts TCP and then never answers the
            // ClientHello would otherwise hold the dial for Netty's own
            // 10s default, well past the bound the rotation dials under.
            ssl.setHandshakeTimeoutMillis(dialTimeoutMillis);
            pipeline.addLast("ssl", ssl);
        }
        // A pipelining producer's next request is usually written from the
        // completion of the previous reply, so its flush lands inside the
        // read loop that delivered it and several requests leave in one
        // syscall instead of one each. Consolidation is confined to that
        // read loop: with no read in progress every flush passes straight
        // through, so a request on an otherwise idle connection is never
        // waiting on later traffic to push it out.
        pipeline.addLast(
                "flushConsolidation",
                new FlushConsolidationHandler(FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, false));
        pipeline.addLast("frameDecoder", new VsrFrameDecoder(maxVsrFrameSize));
        pipeline.addLast("responseHandler", new VsrResponseHandler(consensusSession, onEviction));
    }
}
