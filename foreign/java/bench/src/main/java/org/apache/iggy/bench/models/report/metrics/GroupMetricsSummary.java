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

import org.apache.iggy.bench.common.enums.GroupKind;

import java.util.Objects;

public record GroupMetricsSummary(
        GroupKind kind,
        double totalThroughputMegabytesPerSecond,
        double totalThroughputMessagesPerSecond,
        double averageThroughputMegabytesPerSecond,
        double averageThroughputMessagesPerSecond,
        double averageP50LatencyMs,
        double averageP90LatencyMs,
        double averageP95LatencyMs,
        double averageP99LatencyMs,
        double averageP999LatencyMs,
        double averageP9999LatencyMs,
        double averageLatencyMs,
        double averageMedianLatencyMs,
        double minLatencyMs,
        double maxLatencyMs,
        double stdDevLatencyMs) {

    public GroupMetricsSummary {
        kind = Objects.requireNonNull(kind, "kind");
    }
}
