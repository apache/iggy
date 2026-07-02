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

package org.apache.iggy.bench.report;

import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.report.context.BenchmarkCacheMetrics;
import org.apache.iggy.bench.models.report.context.BenchmarkCacheMetricsKey;
import org.apache.iggy.bench.models.report.context.BenchmarkServerStats;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.system.CacheMetrics;
import org.apache.iggy.system.CacheMetricsKey;
import org.apache.iggy.system.Stats;

import java.util.HashMap;
import java.util.Map;

public final class ServerStatsCollector {
    private final GlobalCliArgs globalCliArgs;

    public ServerStatsCollector(GlobalCliArgs globalCliArgs) {
        this.globalCliArgs = globalCliArgs;
    }

    public BenchmarkServerStats collect() {
        try (var client = IggyTcpClient.builder()
                .credentials(globalCliArgs.username(), globalCliArgs.password())
                .connectionPoolSize(1)
                .buildAndLogin()) {
            Stats stats = client.system().getStats();
            Map<BenchmarkCacheMetricsKey, BenchmarkCacheMetrics> cacheMetrics = new HashMap<>();

            for (Map.Entry<CacheMetricsKey, CacheMetrics> entry :
                    stats.cacheMetrics().entrySet()) {
                cacheMetrics.put(
                        new BenchmarkCacheMetricsKey(
                                entry.getKey().streamId(),
                                entry.getKey().topicId(),
                                entry.getKey().partitionId()),
                        new BenchmarkCacheMetrics(
                                entry.getValue().hits().longValue(),
                                entry.getValue().misses().longValue(),
                                entry.getValue().hitRatio()));
            }

            return new BenchmarkServerStats(
                    stats.processId(),
                    stats.cpuUsage(),
                    stats.totalCpuUsage(),
                    Long.parseLong(stats.memoryUsage()),
                    Long.parseLong(stats.totalMemory()),
                    Long.parseLong(stats.availableMemory()),
                    stats.runTime().longValue(),
                    stats.startTime().longValue(),
                    Long.parseLong(stats.readBytes()),
                    Long.parseLong(stats.writtenBytes()),
                    Long.parseLong(stats.messagesSizeBytes()),
                    stats.streamsCount(),
                    stats.topicsCount(),
                    stats.partitionsCount(),
                    stats.segmentsCount(),
                    stats.messagesCount().longValue(),
                    stats.clientsCount(),
                    stats.consumerGroupsCount(),
                    stats.hostname(),
                    stats.osName(),
                    stats.osVersion(),
                    stats.kernelVersion(),
                    stats.iggyServerVersion(),
                    stats.iggyServerSemver(),
                    cacheMetrics);
        }
    }
}
