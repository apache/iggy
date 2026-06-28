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

package org.apache.iggy.bench.models.report.context;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record BenchmarkServerStats(
        long processId,
        double cpuUsage,
        double totalCpuUsage,
        long memoryUsage,
        long totalMemory,
        long availableMemory,
        long runTime,
        long startTime,
        long readBytes,
        long writtenBytes,
        long messagesSizeBytes,
        long streamsCount,
        long topicsCount,
        long partitionsCount,
        long segmentsCount,
        long messagesCount,
        long clientsCount,
        long consumerGroupsCount,
        String hostname,
        String osName,
        String osVersion,
        String kernelVersion,
        String iggyServerVersion,
        Optional<Long> iggyServerSemver,
        Map<BenchmarkCacheMetricsKey, BenchmarkCacheMetrics> cacheMetrics) {

    public BenchmarkServerStats {
        hostname = Objects.requireNonNull(hostname, "hostname");
        osName = Objects.requireNonNull(osName, "osName");
        osVersion = Objects.requireNonNull(osVersion, "osVersion");
        kernelVersion = Objects.requireNonNull(kernelVersion, "kernelVersion");
        iggyServerVersion = Objects.requireNonNull(iggyServerVersion, "iggyServerVersion");
        iggyServerSemver = Objects.requireNonNull(iggyServerSemver, "iggyServerSemver");
        cacheMetrics = Map.copyOf(Objects.requireNonNull(cacheMetrics, "cacheMetrics"));
    }
}
