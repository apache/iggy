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

package org.apache.iggy.bench.models.report.metrics;

import org.apache.iggy.bench.common.enums.ActorKind;
import org.apache.iggy.bench.common.enums.BenchmarkKind;

import java.util.Objects;

public record IndividualMetricsSummary(
        BenchmarkKind benchmarkKind,
        ActorKind actorKind,
        int actorId,
        double totalTimeSecs,
        long totalUserDataBytes,
        long totalBytes,
        long totalMessages,
        long totalMessageBatches,
        double throughputMegabytesPerSecond,
        double throughputMessagesPerSecond,
        double p50LatencyMs,
        double p90LatencyMs,
        double p95LatencyMs,
        double p99LatencyMs,
        double p999LatencyMs,
        double p9999LatencyMs,
        double avgLatencyMs,
        double medianLatencyMs,
        double minLatencyMs,
        double maxLatencyMs,
        double stdDevLatencyMs) {

    public IndividualMetricsSummary {
        benchmarkKind = Objects.requireNonNull(benchmarkKind, "benchmarkKind");
        actorKind = Objects.requireNonNull(actorKind, "actorKind");
    }
}
