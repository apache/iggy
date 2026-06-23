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

import org.apache.iggy.bench.common.enums.ActorKind;
import org.apache.iggy.bench.common.enums.BenchmarkKind;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.common.generator.DataBatch;
import org.apache.iggy.bench.models.report.metrics.BenchmarkRecord;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;
import org.apache.iggy.bench.report.IndividualMetricsCalculator;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Partitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public final class TcpAsyncPinnedProducerActor {

    private static final Logger log = LoggerFactory.getLogger(TcpAsyncPinnedProducerActor.class);
    private static final long PARTITION_ID = 0L;

    private final GlobalCliArgs globalCliArgs;
    private final int actorId;
    private final StreamId streamId;
    private final TopicId topicId;
    private final Partitioning partitioning;
    private final DataBatch fullBatch;
    private final long targetMessageBatches;
    private final long targetDataBytes;
    private final List<BenchmarkRecord> records;
    private AsyncIggyTcpClient client;
    private long benchmarkStartTimeNs;
    private long sentBatches;
    private long sentMessages;
    private long sentUserDataBytes;
    private long sentTotalBytes;

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
        this.records = new ArrayList<>();
    }

    public CompletableFuture<IndividualMetrics> run() {
        try {
            records.clear();
            benchmarkStartTimeNs = 0L;
            sentBatches = 0L;
            sentMessages = 0L;
            sentUserDataBytes = 0L;
            sentTotalBytes = 0L;

            String target = targetDataBytes > 0L ? targetDataBytes + " user bytes" : targetMessageBatches + " batches";
            String rateLimit = globalCliArgs.rateLimit() > 0L ? Long.toString(globalCliArgs.rateLimit()) : "none";

            log.info(
                    "Producer #{} \u2192 sending {} ({} msgs/batch) on stream {}, rate limit: {}",
                    actorId,
                    target,
                    globalCliArgs.messagesPerBatch(),
                    streamId,
                    rateLimit);
            return AsyncIggyTcpClient.builder()
                    .credentials(globalCliArgs.username(), globalCliArgs.password())
                    .connectionPoolSize(1)
                    .buildAndLogin()
                    .thenCompose(client -> {
                        this.client = client;
                        long warmupDeadline = globalCliArgs.warmupTimeMs() <= 0L
                                ? 0L
                                : System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(globalCliArgs.warmupTimeMs());

                        if (warmupDeadline > 0L) {
                            log.info(
                                    "Producer #{} \u2192 warming up for {} ms...",
                                    actorId,
                                    globalCliArgs.warmupTimeMs());
                        }

                        return sendMessages(warmupDeadline)
                                .thenApply(ignored -> new IndividualMetricsCalculator(
                                                records,
                                                BenchmarkKind.PINNED_PRODUCER,
                                                ActorKind.PRODUCER,
                                                actorId,
                                                globalCliArgs.samplingTimeMs(),
                                                globalCliArgs.movingAverageWindow())
                                        .calculate())
                                .thenApply(metrics -> {
                                    var summary = metrics.summary();

                                    log.info(String.format(
                                            "Producer #%d \u2192 sent %s messages in %s batches of %d messages in %.2f s, total size: %s, "
                                                    + "average throughput: %.2f MB/s, p50 latency: %.2f ms, p90 latency: %.2f ms, "
                                                    + "p95 latency: %.2f ms, p99 latency: %.2f ms, p999 latency: %.2f ms, "
                                                    + "p9999 latency: %.2f ms, average latency: %.2f ms, median latency: %.2f ms, "
                                                    + "min latency: %.2f ms, max latency: %.2f ms, std dev latency: %.2f ms",
                                            actorId,
                                            summary.totalMessages(),
                                            summary.totalMessageBatches(),
                                            globalCliArgs.messagesPerBatch(),
                                            summary.totalTimeSecs(),
                                            summary.totalUserDataBytes() + " bytes",
                                            summary.throughputMegabytesPerSecond(),
                                            summary.p50LatencyMs(),
                                            summary.p90LatencyMs(),
                                            summary.p95LatencyMs(),
                                            summary.p99LatencyMs(),
                                            summary.p999LatencyMs(),
                                            summary.p9999LatencyMs(),
                                            summary.avgLatencyMs(),
                                            summary.medianLatencyMs(),
                                            summary.minLatencyMs(),
                                            summary.maxLatencyMs(),
                                            summary.stdDevLatencyMs()));
                                    return metrics;
                                })
                                .handle((metrics, sendFailure) -> {
                                    CompletableFuture<Void> closeFuture = client.close();

                                    if (sendFailure != null) {
                                        CompletableFuture<IndividualMetrics> failedAfterClose =
                                                closeFuture.handle((closeIgnored, closeFailure) -> {
                                                    throw new CompletionException(sendFailure);
                                                });
                                        return failedAfterClose;
                                    }

                                    return closeFuture.thenApply(closeIgnored -> metrics);
                                })
                                .thenCompose(future -> future);
                    });
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    private CompletableFuture<Void> sendMessages(long warmupDeadline) {
        if (warmupDeadline > 0L) {
            if (System.nanoTime() >= warmupDeadline) {
                records.clear();
                benchmarkStartTimeNs = System.nanoTime();
                sentBatches = 0L;
                sentMessages = 0L;
                sentUserDataBytes = 0L;
                sentTotalBytes = 0L;
                log.info(
                        "Producer #{} \u2192 sending {} messages per batch (each {} bytes) to stream {} using TCP async...",
                        actorId,
                        globalCliArgs.messagesPerBatch(),
                        globalCliArgs.messageSize(),
                        streamId);
                return sendMessages(0L);
            }

            return sendBatch(fullBatch, false).thenCompose(ignored -> sendMessages(warmupDeadline));
        }

        if (benchmarkStartTimeNs == 0L) {
            benchmarkStartTimeNs = System.nanoTime();
            log.info(
                    "Producer #{} \u2192 sending {} messages per batch (each {} bytes) to stream {} using TCP async...",
                    actorId,
                    globalCliArgs.messagesPerBatch(),
                    globalCliArgs.messageSize(),
                    streamId);
            return sendMessages(0L);
        }

        if (targetDataBytes > 0L ? sentUserDataBytes >= targetDataBytes : sentBatches >= targetMessageBatches) {
            String finishCondition = targetDataBytes > 0L
                    ? sentUserDataBytes + "/" + targetDataBytes + " user bytes"
                    : sentBatches + "/" + targetMessageBatches + " batches";
            log.info(
                    "Producer #{} \u2192 finished sending {} messages in {} batches ({} user bytes, {} total bytes), send finish condition: {}",
                    actorId,
                    sentMessages,
                    sentBatches,
                    sentUserDataBytes,
                    sentTotalBytes,
                    finishCondition);
            return CompletableFuture.completedFuture(null);
        }

        return sendBatch(fullBatch, true).thenCompose(latencyUs -> {
            sentBatches += 1L;
            sentMessages += fullBatch.messages().size();
            sentUserDataBytes += fullBatch.userDataBytes();
            sentTotalBytes += fullBatch.totalBytes();
            long elapsedTimeUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - benchmarkStartTimeNs);

            records.add(new BenchmarkRecord(
                    elapsedTimeUs, latencyUs, sentMessages, sentBatches, sentUserDataBytes, sentTotalBytes));

            return sendMessages(0L);
        });
    }

    private CompletableFuture<Long> sendBatch(DataBatch currentBatch, boolean recordMetrics) {
        long startedAt = recordMetrics ? System.nanoTime() : 0L;

        return client.messages()
                .sendMessages(streamId, topicId, partitioning, currentBatch.messages())
                .thenApply(
                        ignored -> recordMetrics ? TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startedAt) : 0L);
    }
}
