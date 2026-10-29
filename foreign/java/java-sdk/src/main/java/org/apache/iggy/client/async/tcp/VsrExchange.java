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
import org.apache.iggy.client.async.tcp.vsr.VsrHeaders;
import org.apache.iggy.client.async.tcp.vsr.VsrResponseHandler;
import org.apache.iggy.serde.CommandCode;

import java.util.concurrent.CompletableFuture;

/** What every write attempt of one request on one channel shares. */
record VsrExchange(
        Channel channel,
        VsrResponseHandler handler,
        CompletableFuture<ByteBuf> responseFuture,
        CompletableFuture<ByteBuf> callerFuture,
        int commandCode,
        AsyncTcpConnection.TransientFailoverState failoverState,
        long requestDeadlineNanos,
        long deadlineNanos,
        long notAcceptedDeadlineNanos) {

    /**
     * The frame of the next attempt, which takes the place of {@code retainedFrame}. A deferred request sends the wait
     * and the request timeout that its deadline leaves, and a fetch lowers its byte limits until its reply fits in
     * {@code maxReplyBody}. If this throws, the caller still owns {@code retainedFrame}.
     */
    ByteBuf frameForAttempt(ByteBuf retainedFrame, long maxReplyBody) {
        int wait;
        int timeout;
        if (commandCode == CommandCode.Messages.POLL_DEFERRED.getValue()) {
            wait = retainedFrame.writerIndex() - PollRouter.DEFERRED_PARAMETERS_BYTES;
            timeout = retainedFrame.writerIndex() - Long.BYTES;
        } else if (commandCode == CommandCode.Messages.FETCH_MESSAGES.getValue()) {
            wait = PollRouter.waitParameters(retainedFrame, retainedFrame.readerIndex() + VsrHeaders.HEADER_SIZE);
            timeout = wait + Long.BYTES;
        } else {
            return retainedFrame;
        }
        var options = PollRouter.deferredBudget(retainedFrame.getLongLE(wait), retainedFrame.getLongLE(timeout));
        var remaining =
                options.remaining(options.requestTimeout().minusNanos(requestDeadlineNanos - System.nanoTime()));
        if (remaining.isEmpty()
                || (commandCode == CommandCode.Messages.FETCH_MESSAGES.getValue()
                        && !FetchMessages.keepsReplyMargin(remaining.get()))) {
            throw AsyncTcpConnection.notAccepted();
        }
        // A progress ticket and pending writers share the frozen frame. Only this attempt's copy changes.
        ByteBuf frame = retainedFrame.copy();
        try {
            frame.setLongLE(wait - retainedFrame.readerIndex(), remaining.get().maxWaitMicros());
            frame.setLongLE(
                    timeout - retainedFrame.readerIndex(), remaining.get().requestTimeoutMicros());
            if (commandCode == CommandCode.Messages.FETCH_MESSAGES.getValue()) {
                FetchMessages.fitReply(frame, wait - retainedFrame.readerIndex(), maxReplyBody);
            }
        } catch (RuntimeException error) {
            frame.release();
            throw error;
        }
        retainedFrame.release();
        return frame;
    }
}
