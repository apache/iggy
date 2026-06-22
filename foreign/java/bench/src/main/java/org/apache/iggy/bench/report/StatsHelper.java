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

import org.apache.iggy.bench.models.report.metrics.TimePoint;
import org.apache.iggy.bench.models.report.metrics.TimeSeries;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class StatsHelper {

    private StatsHelper() {}

    public static Optional<Double> mean(TimeSeries series) {
        if (series.points().isEmpty()) {
            return Optional.empty();
        }

        double sum = 0.0;
        for (TimePoint point : series.points()) {
            sum += point.value();
        }

        return Optional.of(sum / series.points().size());
    }

    public static Optional<Double> mean(List<Double> values) {
        if (values.isEmpty()) {
            return Optional.empty();
        }

        double sum = 0.0;
        for (Double value : values) {
            sum += value;
        }

        return Optional.of(sum / values.size());
    }

    public static Optional<Double> min(TimeSeries series) {
        if (series.points().isEmpty()) {
            return Optional.empty();
        }

        double minimum = Double.POSITIVE_INFINITY;
        for (TimePoint point : series.points()) {
            minimum = Math.min(minimum, point.value());
        }

        return Optional.of(minimum);
    }

    public static Optional<Double> max(TimeSeries series) {
        if (series.points().isEmpty()) {
            return Optional.empty();
        }

        double maximum = Double.NEGATIVE_INFINITY;
        for (TimePoint point : series.points()) {
            maximum = Math.max(maximum, point.value());
        }

        return Optional.of(maximum);
    }

    public static Optional<Double> stdDev(TimeSeries series) {
        if (series.points().size() < 2) {
            return Optional.empty();
        }

        double mean = mean(series).orElseThrow();
        double variance = 0.0;
        for (TimePoint point : series.points()) {
            double diff = point.value() - mean;
            variance += diff * diff;
        }

        variance /= series.points().size();
        return Optional.of(Math.sqrt(variance));
    }

    public static TimeSeries applyMovingAverage(TimeSeries data, int movingAverageWindow) {
        if (data.points().isEmpty()) {
            return data;
        }

        ArrayDeque<Double> window = new ArrayDeque<>(movingAverageWindow);
        List<TimePoint> points = new ArrayList<>(data.points().size());
        double sum = 0.0;

        for (TimePoint point : data.points()) {
            window.addLast(point.value());
            sum += point.value();

            if (window.size() > movingAverageWindow) {
                Double removed = window.removeFirst();
                sum -= removed;
            }

            double average = sum / window.size();
            points.add(new TimePoint(point.timeS(), Math.round(average * 1000.0) / 1000.0));
        }

        return new TimeSeries(points, data.kind());
    }

    public static double calculatePercentile(List<Double> sortedData, double percentile) {
        if (sortedData.isEmpty()) {
            return 0.0;
        }

        double rank = percentile * (sortedData.size() - 1);
        int lower = (int) Math.floor(rank);
        int upper = (int) Math.ceil(rank);

        double weight = rank - lower;
        return sortedData.get(lower) * (1.0 - weight) + sortedData.get(upper) * weight;
    }
}
