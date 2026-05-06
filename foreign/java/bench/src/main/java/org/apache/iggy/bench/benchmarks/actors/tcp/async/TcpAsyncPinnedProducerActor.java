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

package org.apache.iggy.bench.benchmarks.actors.tcp.async;

import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.common.generator.DataBatch;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Partitioning;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public final class TcpAsyncPinnedProducerActor {

    private static final long PARTITION_ID = 0L;

    private final GlobalCliArgs globalCliArgs;
    private final int actorId;
    private final StreamId streamId;
    private final TopicId topicId;
    private final Partitioning partitioning;
    private final DataBatch fullBatch;
    private final long targetMessageBatches;
    private final long targetDataBytes;
    private AsyncIggyTcpClient client;

    public TcpAsyncPinnedProducerActor(
            GlobalCliArgs globalCliArgs,
            int actorId,
            String streamName,
            String topicName,
            DataBatch fullBatch,
            long targetMessageBatches,
            long targetDataBytes) {
        this.globalCliArgs = globalCliArgs;
        this.actorId = actorId;
        this.streamId = StreamId.of(streamName);
        this.topicId = TopicId.of(topicName);
        this.partitioning = Partitioning.partitionId(PARTITION_ID);
        this.fullBatch = fullBatch;
        this.targetMessageBatches = targetMessageBatches;
        this.targetDataBytes = targetDataBytes;
    }

    public CompletableFuture<Void> run() {
        try {
            return AsyncIggyTcpClient.builder()
                    .credentials(globalCliArgs.username(), globalCliArgs.password())
                    .buildAndLogin()
                    .thenCompose(client -> {
                        this.client = client;
                        long warmupDeadline = globalCliArgs.warmupTimeMs() <= 0L
                                ? 0L
                                : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(globalCliArgs.warmupTimeMs());

                        return sendMessages(warmupDeadline, 0L, 0L)
                                .handle((ignored, sendFailure) -> {
                                    CompletableFuture<Void> closeFuture = client.close();

                                    if (sendFailure != null) {
                                        CompletableFuture<Void> failedAfterClose =
                                                closeFuture.handle((closeIgnored, closeFailure) -> {
                                                    throw new CompletionException(sendFailure);
                                                });
                                        return failedAfterClose;
                                    }

                                    return closeFuture;
                                })
                                .thenCompose(future -> future);
                    });
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    private CompletableFuture<Void> sendMessages(long warmupDeadline, long sentBatches, long sentBytes) {
        if (warmupDeadline > 0L) {
            if (System.nanoTime() >= warmupDeadline) {
                return sendMessages(0L, 0L, 0L);
            }

            return sendBatch(fullBatch, false)
                    .thenCompose(ignored -> sendMessages(warmupDeadline, sentBatches, sentBytes));
        }

        if (targetDataBytes > 0L ? sentBytes >= targetDataBytes : sentBatches >= targetMessageBatches) {
            return CompletableFuture.completedFuture(null);
        }

        return sendBatch(fullBatch, true).thenCompose(ignored -> {
            long updatedSentBatches = sentBatches + 1L;
            long updatedSentBytes = sentBytes + fullBatch.userDataBytes();
            return sendMessages(0L, updatedSentBatches, updatedSentBytes);
        });
    }

    private CompletableFuture<Void> sendBatch(DataBatch currentBatch, boolean recordMetrics) {
        return client.messages().sendMessages(streamId, topicId, partitioning, currentBatch.messages());
    }
}
