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

import org.apache.iggy.bench.common.enums.ActorKind;
import org.apache.iggy.bench.common.enums.GroupKind;
import org.apache.iggy.bench.common.enums.TimeSeriesKind;
import org.apache.iggy.bench.models.cli.GlobalCliArgs;
import org.apache.iggy.bench.models.report.metrics.DistributionPercentiles;
import org.apache.iggy.bench.models.report.metrics.GroupMetrics;
import org.apache.iggy.bench.models.report.metrics.GroupMetricsSummary;
import org.apache.iggy.bench.models.report.metrics.HistogramBin;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;
import org.apache.iggy.bench.models.report.metrics.IndividualMetricsSummary;
import org.apache.iggy.bench.models.report.metrics.LatencyDistribution;
import org.apache.iggy.bench.models.report.metrics.LogNormalParams;
import org.apache.iggy.bench.models.report.metrics.TimePoint;
import org.apache.iggy.bench.models.report.metrics.TimeSeries;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

public final class GroupMetricsCalculator {

    private static final int NUM_BINS = 50;
    private static final double RANGE_FACTOR = 1.5;

    private final List<IndividualMetrics> individualMetrics;
    private final GlobalCliArgs globalCliArgs;
    private GroupKind groupKind;
    private final List<TimeSeries> throughputMbSeries;
    private final List<TimeSeries> throughputMsgSeries;
    private final List<TimeSeries> latencySeries;
    private final List<Double> allLatenciesMs;
    private double totalThroughputMegabytesPerSecond;
    private double totalThroughputMessagesPerSecond;
    private double totalP50LatencyMs;
    private double totalP90LatencyMs;
    private double totalP95LatencyMs;
    private double totalP99LatencyMs;
    private double totalP999LatencyMs;
    private double totalP9999LatencyMs;
    private double totalAverageLatencyMs;
    private double totalMedianLatencyMs;
    private double minLatencyMs;
    private double maxLatencyMs;

    public GroupMetricsCalculator(List<IndividualMetrics> individualMetrics, GlobalCliArgs globalCliArgs) {
        this.individualMetrics = individualMetrics;
        this.globalCliArgs = globalCliArgs;
        this.throughputMbSeries = new ArrayList<>();
        this.throughputMsgSeries = new ArrayList<>();
        this.latencySeries = new ArrayList<>();
        this.allLatenciesMs = new ArrayList<>();
    }

    public List<GroupMetrics> calculate() {
        if (individualMetrics.isEmpty()) {
            return List.of();
        }

        reset();
        collectMetrics();

        TimeSeries avgThroughputMbTs = StatsHelper.applyMovingAverage(
                aggregateTimeSeries(throughputMbSeries, TimeSeriesKind.THROUGHPUT_MB, false),
                globalCliArgs.movingAverageWindow());
        TimeSeries avgThroughputMsgTs = StatsHelper.applyMovingAverage(
                aggregateTimeSeries(throughputMsgSeries, TimeSeriesKind.THROUGHPUT_MSG, false),
                globalCliArgs.movingAverageWindow());
        TimeSeries avgLatencyTs = StatsHelper.applyMovingAverage(
                aggregateTimeSeries(latencySeries, TimeSeriesKind.LATENCY, true), globalCliArgs.movingAverageWindow());

        double count = individualMetrics.size();
        double stdDevLatencyMs = StatsHelper.stdDev(avgLatencyTs).orElse(0.0);
        GroupMetricsSummary summary = new GroupMetricsSummary(
                groupKind,
                Math.round(totalThroughputMegabytesPerSecond * 1000.0) / 1000.0,
                Math.round(totalThroughputMessagesPerSecond * 1000.0) / 1000.0,
                Math.round(totalThroughputMegabytesPerSecond / count * 1000.0) / 1000.0,
                Math.round(totalThroughputMessagesPerSecond / count * 1000.0) / 1000.0,
                Math.round(totalP50LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalP90LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalP95LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalP99LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalP999LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalP9999LatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalAverageLatencyMs / count * 1000.0) / 1000.0,
                Math.round(totalMedianLatencyMs / count * 1000.0) / 1000.0,
                Math.round(minLatencyMs * 1000.0) / 1000.0,
                Math.round(maxLatencyMs * 1000.0) / 1000.0,
                Math.round(stdDevLatencyMs * 1000.0) / 1000.0);

        Optional<LatencyDistribution> latencyDistribution = calculateLatencyDistribution();

        return List.of(
                new GroupMetrics(summary, avgThroughputMbTs, avgThroughputMsgTs, avgLatencyTs, latencyDistribution));
    }

    private void reset() {
        throughputMbSeries.clear();
        throughputMsgSeries.clear();
        latencySeries.clear();
        allLatenciesMs.clear();
        totalThroughputMegabytesPerSecond = 0.0;
        totalThroughputMessagesPerSecond = 0.0;
        totalP50LatencyMs = 0.0;
        totalP90LatencyMs = 0.0;
        totalP95LatencyMs = 0.0;
        totalP99LatencyMs = 0.0;
        totalP999LatencyMs = 0.0;
        totalP9999LatencyMs = 0.0;
        totalAverageLatencyMs = 0.0;
        totalMedianLatencyMs = 0.0;
        minLatencyMs = Double.POSITIVE_INFINITY;
        maxLatencyMs = Double.NEGATIVE_INFINITY;
    }

    private void collectMetrics() {
        ActorKind actorKind = individualMetrics.get(0).summary().actorKind();
        groupKind = switch (actorKind) {
            case PRODUCER -> GroupKind.PRODUCERS;
            case CONSUMER -> GroupKind.CONSUMERS;
            case PRODUCING_CONSUMER -> GroupKind.PRODUCING_CONSUMERS;
        };

        for (IndividualMetrics metrics : individualMetrics) {
            IndividualMetricsSummary summary = metrics.summary();

            throughputMbSeries.add(metrics.throughputMbTs());
            throughputMsgSeries.add(metrics.throughputMsgTs());
            latencySeries.add(metrics.latencyTs());

            totalThroughputMegabytesPerSecond += summary.throughputMegabytesPerSecond();
            totalThroughputMessagesPerSecond += summary.throughputMessagesPerSecond();
            totalP50LatencyMs += summary.p50LatencyMs();
            totalP90LatencyMs += summary.p90LatencyMs();
            totalP95LatencyMs += summary.p95LatencyMs();
            totalP99LatencyMs += summary.p99LatencyMs();
            totalP999LatencyMs += summary.p999LatencyMs();
            totalP9999LatencyMs += summary.p9999LatencyMs();
            totalAverageLatencyMs += summary.avgLatencyMs();
            totalMedianLatencyMs += summary.medianLatencyMs();
            minLatencyMs = Math.min(minLatencyMs, summary.minLatencyMs());
            maxLatencyMs = Math.max(maxLatencyMs, summary.maxLatencyMs());
            allLatenciesMs.addAll(metrics.rawLatenciesMs());
        }
    }

    private Optional<LatencyDistribution> calculateLatencyDistribution() {
        if (allLatenciesMs.isEmpty()) {
            return Optional.empty();
        }

        allLatenciesMs.sort((left, right) -> Double.compare(left, right));

        double p05 = StatsHelper.calculatePercentile(allLatenciesMs, 0.05);
        double p50 = StatsHelper.calculatePercentile(allLatenciesMs, 0.50);
        double p95 = StatsHelper.calculatePercentile(allLatenciesMs, 0.95);
        double p99 = StatsHelper.calculatePercentile(allLatenciesMs, 0.99);
        DistributionPercentiles percentiles = new DistributionPercentiles(p05, p50, p95, p99);

        return Optional.of(
                new LatencyDistribution(calculateHistogramBins(p99), calculateLogNormalParams(), percentiles));
    }

    private List<HistogramBin> calculateHistogramBins(double p99) {
        double minValue = allLatenciesMs.get(0);
        double maxValue = Math.min(p99 * RANGE_FACTOR, allLatenciesMs.get(allLatenciesMs.size() - 1));
        double range = maxValue - minValue;

        if (range <= 0.0) {
            return List.of(new HistogramBin(minValue, 1.0));
        }

        double binWidth = range / NUM_BINS;
        List<HistogramBin> bins = new ArrayList<>(NUM_BINS);
        int sampleIndex = 0;

        for (int index = 0; index < NUM_BINS; index++) {
            double edge = minValue + index * binWidth;
            double upper = index == NUM_BINS - 1 ? Double.POSITIVE_INFINITY : edge + binWidth;
            long bucketCount = 0L;

            while (sampleIndex < allLatenciesMs.size() && allLatenciesMs.get(sampleIndex) < upper) {
                bucketCount += 1L;
                sampleIndex += 1;
            }

            bins.add(new HistogramBin(edge, bucketCount / (allLatenciesMs.size() * binWidth)));
        }

        return bins;
    }

    private LogNormalParams calculateLogNormalParams() {
        double logSum = 0.0;
        int positiveCount = 0;
        for (Double latency : allLatenciesMs) {
            if (latency > 0.0) {
                logSum += Math.log(latency);
                positiveCount += 1;
            }
        }

        if (positiveCount == 0) {
            return new LogNormalParams(0.0, 0.0);
        }

        double mu = logSum / positiveCount;
        double variance = 0.0;

        for (Double latency : allLatenciesMs) {
            if (latency > 0.0) {
                double diff = Math.log(latency) - mu;
                variance += diff * diff;
            }
        }

        variance /= positiveCount;
        return new LogNormalParams(mu, Math.sqrt(variance));
    }

    private TimeSeries aggregateTimeSeries(List<TimeSeries> series, TimeSeriesKind kind, boolean averageValues) {
        TreeMap<Double, double[]> valuesByTime = new TreeMap<>();

        for (TimeSeries timeSeries : series) {
            for (TimePoint point : timeSeries.points()) {
                double[] values = valuesByTime.get(point.timeS());
                if (values == null) {
                    values = new double[2];
                    valuesByTime.put(point.timeS(), values);
                }

                values[0] += point.value();
                values[1] += 1.0;
            }
        }

        List<TimePoint> points = new ArrayList<>(valuesByTime.size());
        for (var entry : valuesByTime.entrySet()) {
            double[] values = entry.getValue();
            double value = averageValues ? values[0] / values[1] : values[0];
            points.add(new TimePoint(entry.getKey(), value));
        }

        return new TimeSeries(points, kind);
    }
}
