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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Objects;

public record IndividualMetrics(
        IndividualMetricsSummary summary,
        TimeSeries throughputMbTs,
        TimeSeries throughputMsgTs,
        TimeSeries latencyTs,
        @JsonIgnore List<Double> rawLatenciesMs) {

    public IndividualMetrics {
        summary = Objects.requireNonNull(summary, "summary");
        throughputMbTs = Objects.requireNonNull(throughputMbTs, "throughputMbTs");
        throughputMsgTs = Objects.requireNonNull(throughputMsgTs, "throughputMsgTs");
        latencyTs = Objects.requireNonNull(latencyTs, "latencyTs");
        rawLatenciesMs = List.copyOf(Objects.requireNonNull(rawLatenciesMs, "rawLatenciesMs"));
    }
}
