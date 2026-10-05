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

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Caches the partition context that fences one kind of partition request, so a
 * repeated request skips its discovery round trip while the incarnation holds. A
 * failed request evicts the context it carried, and the next request discovers
 * a fresh one. Transport retries resend the context a request captured. The
 * discovery and the request each use the client's current connection, because
 * a failover can replace it in between.
 */
final class PartitionContexts {
    private static final int MAX_CONTEXTS = 4096;

    private final Supplier<AsyncTcpConnection> connection;
    private final int discoveryCommand;
    private final int contextOffset;
    private final boolean refreshOnHistoryUnavailable;
    private final Map<String, PartitionContext> contexts = new ConcurrentHashMap<>();

    /**
     * @param contextOffset where the context starts in the discovery reply
     * @param refreshOnHistoryUnavailable whether a request refused for a changed
     *     incarnation is sent once more with a fresh context; only a request that
     *     does not depend on what the caller read from the old incarnation may opt in
     */
    PartitionContexts(
            Supplier<AsyncTcpConnection> connection,
            int discoveryCommand,
            int contextOffset,
            boolean refreshOnHistoryUnavailable) {
        this.connection = connection;
        this.discoveryCommand = discoveryCommand;
        this.contextOffset = contextOffset;
        this.refreshOnHistoryUnavailable = refreshOnHistoryUnavailable;
    }

    /**
     * Sends {@code payload} with the context cached for the first {@code keyLength}
     * bytes of {@code discoveryPayload}, which is discovered on a miss. Takes
     * ownership of both buffers.
     */
    CompletableFuture<ByteBuf> send(int command, ByteBuf payload, ByteBuf discoveryPayload, int keyLength) {
        String key = ByteBufUtil.hexDump(discoveryPayload, discoveryPayload.readerIndex(), keyLength);
        CompletableFuture<ByteBuf> result = new CompletableFuture<>();
        attempt(result, command, payload, discoveryPayload, key, refreshOnHistoryUnavailable)
                .whenComplete((response, error) -> {
                    payload.release();
                    discoveryPayload.release();
                    AsyncTcpConnection.completeWithResponse(result, response, error);
                });
        return result;
    }

    private CompletableFuture<ByteBuf> attempt(
            CompletableFuture<ByteBuf> result,
            int command,
            ByteBuf payload,
            ByteBuf discoveryPayload,
            String key,
            boolean refresh) {
        PartitionContext cached = contexts.get(key);
        CompletableFuture<PartitionContext> captured =
                cached != null ? CompletableFuture.completedFuture(cached) : discover(key, discoveryPayload);
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
                contexts.remove(key, context);
                return refresh && isHistoryUnavailable(error)
                        ? attempt(result, command, payload, discoveryPayload, key, false)
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
                        if (contexts.size() >= MAX_CONTEXTS) {
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
