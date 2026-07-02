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

package org.apache.iggy.bench.models.report;

import org.apache.iggy.bench.models.report.context.BenchmarkHardware;
import org.apache.iggy.bench.models.report.context.BenchmarkParams;
import org.apache.iggy.bench.models.report.context.BenchmarkServerStats;
import org.apache.iggy.bench.models.report.metrics.GroupMetrics;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record FinalReport(
        UUID uuid,
        String timestamp,
        BenchmarkServerStats serverStats,
        BenchmarkHardware hardware,
        BenchmarkParams params,
        List<GroupMetrics> groupMetrics,
        List<IndividualMetrics> individualMetrics) {

    public FinalReport {
        uuid = Objects.requireNonNull(uuid, "uuid");
        timestamp = Objects.requireNonNull(timestamp, "timestamp");
        serverStats = Objects.requireNonNull(serverStats, "serverStats");
        hardware = Objects.requireNonNull(hardware, "hardware");
        params = Objects.requireNonNull(params, "params");
        groupMetrics = List.copyOf(Objects.requireNonNull(groupMetrics, "groupMetrics"));
        individualMetrics = List.copyOf(Objects.requireNonNull(individualMetrics, "individualMetrics"));
    }
}
