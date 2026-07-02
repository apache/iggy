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
import org.apache.iggy.bench.common.enums.TimeSeriesKind;

import java.util.List;
import java.util.Objects;

public record TimeSeries(List<TimePoint> points, TimeSeriesKind kind) {

    public TimeSeries {
        points = List.copyOf(Objects.requireNonNull(points, "points"));
        kind = Objects.requireNonNull(kind, "kind");
    }

    @Override
    @JsonIgnore
    public TimeSeriesKind kind() {
        return kind;
    }

    public static TimeSeries empty(TimeSeriesKind kind) {
        return new TimeSeries(List.of(), kind);
    }
}
