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
import io.netty.buffer.ByteBufUtil;
import org.apache.iggy.client.async.tcp.AsyncTcpConnection.TransientFailoverState;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.partition.PartitionContext;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.CommandCode;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Caches the partition context that fences one kind of partition request, so a
 * repeated request skips its discovery round trip while the incarnation holds.
 * Transport retries resend the context a request captured. The discovery and the
 * request each use the client's current connection, because a failover can
 * replace it in between.
 *
 * <p>Routes for polls and offset writes follow the Rust SDK's poll router: a
 * failed request evicts the route of its partition, even when it carried the
 * caller's context, and the cache is cleared once full.
 * Send contexts follow its {@code ConsumerGroupClientState}: only a history
 * refusal drops them, every context of the refused topic, and the refused send
 * goes out once more with a fresh one. The client's own stream and topic
 * deletes and partition creates and deletes drop them all through {@link #clear()}.
 */
final class PartitionContexts {
    private static final int MAX_ROUTES = 4096;
    // GetSendContext names the partition last, after the length-prefixed stream
    // and topic identifiers, so every key of one topic starts with the same bytes.
    private static final int PARTITION_ID_HEX_LENGTH = 2 * Integer.BYTES;

    private final Supplier<AsyncTcpConnection> connection;
    private final int discoveryCommand;
    private final int contextOffset;
    private final boolean sendContexts;
    private final Map<String, PartitionContext> contexts = new ConcurrentHashMap<>();

    /**
     * @param contextOffset where the context starts in the discovery reply
     * @param sendContexts whether this caches send contexts rather than routes; a
     *     refused send is sent once more with a fresh context, so only a request
     *     that does not depend on what the caller read from the old incarnation
     *     may use them
     */
    PartitionContexts(
            Supplier<AsyncTcpConnection> connection, int discoveryCommand, int contextOffset, boolean sendContexts) {
        this.connection = connection;
        this.discoveryCommand = discoveryCommand;
        this.contextOffset = contextOffset;
        this.sendContexts = sendContexts;
    }

    static PartitionContexts forSends(Supplier<AsyncTcpConnection> connection) {
        return new PartitionContexts(connection, CommandCode.Messages.GET_SEND_CONTEXT.getValue(), 0, true);
    }

    /**
     * Sends {@code payload} with the context cached for the first {@code keyLength}
     * bytes of {@code discoveryPayload}, which is discovered on a miss. Takes
     * ownership of both buffers.
     */
    CompletableFuture<ByteBuf> send(int command, ByteBuf payload, ByteBuf discoveryPayload, int keyLength) {
        return send(command, payload, discoveryPayload, keyLength, Optional.empty());
    }

    /**
     * Sends {@code payload} with the caller's {@code context} when there is one,
     * which then replaces the cached or discovered context and needs no discovery.
     * Takes ownership of both buffers.
     */
    CompletableFuture<ByteBuf> send(
            int command, ByteBuf payload, ByteBuf discoveryPayload, int keyLength, Optional<PartitionContext> context) {
        String key = ByteBufUtil.hexDump(discoveryPayload, discoveryPayload.readerIndex(), keyLength);
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        attempt(result, command, payload, discoveryPayload, key, sendContexts, context)
                .whenComplete((response, error) -> {
                    payload.release();
                    discoveryPayload.release();
                    AsyncTcpConnection.completeWithResponse(result, response, error);
                });
        return result;
    }

    void clear() {
        contexts.clear();
    }

    private CompletableFuture<ByteBuf> attempt(
            CompletableFuture<ByteBuf> result,
            int command,
            ByteBuf payload,
            ByteBuf discoveryPayload,
            String key,
            boolean refresh,
            Optional<PartitionContext> callerContext) {
        PartitionContext known = callerContext.orElseGet(() -> contexts.get(key));
        CompletableFuture<PartitionContext> captured =
                known != null ? CompletableFuture.completedFuture(known) : discover(key, discoveryPayload);
        // A canceled request lets its discovery finish, because canceling a poll
        // routing command closes the shared coordinator channel.
        return captured.thenCompose(context -> {
            if (result.isCancelled()) {
                return CompletableFuture.failedFuture(new CancellationException());
            }
            CompletableFuture<ByteBuf> sent = sendDuplicate(command, payload, new TransientFailoverState(context));
            result.whenComplete((ignored, error) -> {
                if (result.isCancelled()) {
                    sent.cancel(false);
                }
            });
            return sent.exceptionallyCompose(error -> {
                if (result.isCancelled()) {
                    return CompletableFuture.failedFuture(error);
                }
                if (!sendContexts) {
                    contexts.remove(key);
                    return CompletableFuture.failedFuture(error);
                }
                if (!isHistoryUnavailable(error)) {
                    return CompletableFuture.failedFuture(error);
                }
                String topic = key.substring(0, key.length() - PARTITION_ID_HEX_LENGTH);
                contexts.keySet().removeIf(other -> other.startsWith(topic));
                return refresh
                        ? attempt(result, command, payload, discoveryPayload, key, false, Optional.empty())
                        : CompletableFuture.failedFuture(error);
            });
        });
    }

    private CompletableFuture<PartitionContext> discover(String key, ByteBuf discoveryPayload) {
        return sendDuplicate(
                        discoveryCommand, discoveryPayload, new TransientFailoverState(PartitionContext.EMPTY, true))
                .thenApply(response -> {
                    try {
                        response.skipBytes(contextOffset);
                        PartitionContext context = BytesDeserializer.readPartitionContext(response);
                        if (!sendContexts && contexts.size() >= MAX_ROUTES) {
                            contexts.clear();
                        }
                        contexts.put(key, context);
                        return context;
                    } finally {
                        response.release();
                    }
                });
    }

    /** Sends a retained duplicate of {@code payload}, so a refresh can send it again. */
    private CompletableFuture<ByteBuf> sendDuplicate(int command, ByteBuf payload, TransientFailoverState state) {
        ByteBuf request = payload.retainedDuplicate();
        try {
            return connection.get().send(command, request, 0, state);
        } catch (RuntimeException | Error error) {
            request.release();
            return CompletableFuture.failedFuture(error);
        }
    }

    private static boolean isHistoryUnavailable(Throwable error) {
        IggyServerException serverError = AsyncTcpConnection.findServerError(error);
        return serverError != null && serverError.getRawErrorCode() == IggyErrorCode.HISTORY_UNAVAILABLE.getCode();
    }
}
