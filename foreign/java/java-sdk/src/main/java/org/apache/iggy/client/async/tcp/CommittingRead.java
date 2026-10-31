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
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.serde.CommandCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * A poll or fetch that stores the offsets that it reads. The server answers a cancel of one exactly once, refused when
 * the cancel won and nothing was stored, or with what it read and stored. So a cancelled committing read keeps its
 * request and its partitions until that reply shows whether it stored.
 */
final class CommittingRead {
    private static final Logger log = LoggerFactory.getLogger(CommittingRead.class);

    private CommittingRead() {}

    /** Whether a poll or fetch body asks the server to store the offsets that it reads. */
    static boolean commits(int commandCode, ByteBuf body) {
        if (commandCode == CommandCode.Messages.FETCH_MESSAGES.getValue()) {
            return FetchMessages.commits(body);
        }
        if (commandCode == CommandCode.Messages.POLL.getValue()) {
            return body.isReadable() && body.getUnsignedByte(body.writerIndex() - 1) != 0;
        }
        if (commandCode == CommandCode.Messages.POLL_DEFERRED.getValue()) {
            return body.readableBytes() > PollRouter.DEFERRED_PARAMETERS_BYTES
                    && body.getUnsignedByte(body.writerIndex() - PollRouter.DEFERRED_PARAMETERS_BYTES - 1) != 0;
        }
        return false;
    }

    /** Reserves the recovery of a committing read when it is admitted, which blocks its lanes if its outcome is lost. */
    static void admit(ConsensusSession.Ticket ticket, int commandCode, ByteBuf payload) {
        if (ticket != null && commits(commandCode, payload)) {
            ticket.reserveFetch();
        }
    }

    /**
     * Runs {@code completion} for an attempt and then marks the attempt settled on its ticket, so the partitions of a
     * cancelled committing read are released only after its reply.
     */
    static BiConsumer<ByteBuf, Throwable> settling(
            ConsensusSession.Ticket ticket, BiConsumer<ByteBuf, Throwable> completion) {
        CompletableFuture<Void> settled = new CompletableFuture<>();
        if (ticket != null) {
            ticket.attemptStarted(settled);
        }
        return (response, error) -> {
            try {
                completion.accept(response, error);
            } finally {
                settled.complete(null);
            }
        };
    }

    /** A caller cancel stops a committing read on the server and waits for its reply, and cancels any other one. */
    static void onCallerCancel(
            VsrExchange exchange,
            VsrResponseHandler.PendingRequest pending,
            CompletableFuture<ByteBuf> attempt,
            Channel channel) {
        if (!exchange.callerFuture().isCancelled()) {
            return;
        }
        if (committing(exchange)) {
            // The server can store until it answers the cancel, so the partitions stay blocked until that reply.
            exchange.failoverState().ticket().blockUntilSettled();
            pending.cancelOnServer(channel);
        } else {
            attempt.cancel(false);
        }
    }

    /** Whether the caller gave up a committing read, which then gets no retry. */
    static boolean abandoned(VsrExchange exchange, Throwable error) {
        boolean abandoned = committing(exchange) && exchange.callerFuture().isCancelled();
        if (abandoned && error == null) {
            log.warn(
                    "A read with command code {} stored its offsets before its cancel arrived, so its messages are"
                            + " dropped",
                    exchange.commandCode());
        }
        return abandoned;
    }

    private static boolean committing(VsrExchange exchange) {
        ConsensusSession.Ticket ticket = exchange.failoverState().ticket();
        return ticket != null && ticket.commitsOnRead();
    }
}
