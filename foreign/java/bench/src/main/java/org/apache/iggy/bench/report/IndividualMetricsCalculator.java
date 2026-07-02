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
import org.apache.iggy.bench.common.enums.BenchmarkKind;
import org.apache.iggy.bench.common.enums.TimeSeriesKind;
import org.apache.iggy.bench.models.report.metrics.BenchmarkRecord;
import org.apache.iggy.bench.models.report.metrics.IndividualMetrics;
import org.apache.iggy.bench.models.report.metrics.IndividualMetricsSummary;
import org.apache.iggy.bench.models.report.metrics.LatencyMetrics;
import org.apache.iggy.bench.models.report.metrics.TimePoint;
import org.apache.iggy.bench.models.report.metrics.TimeSeries;

import java.util.ArrayList;
import java.util.List;

public final class IndividualMetricsCalculator {

    private final List<BenchmarkRecord> records;
    private final BenchmarkKind benchmarkKind;
    private final ActorKind actorKind;
    private final int actorId;
    private final long samplingTimeMs;
    private final int movingAverageWindow;

    public IndividualMetricsCalculator(
            List<BenchmarkRecord> records,
            BenchmarkKind benchmarkKind,
            ActorKind actorKind,
            int actorId,
            long samplingTimeMs,
            int movingAverageWindow) {
        this.records = records;
        this.benchmarkKind = benchmarkKind;
        this.actorKind = actorKind;
        this.actorId = actorId;
        this.samplingTimeMs = samplingTimeMs;
        this.movingAverageWindow = movingAverageWindow;
    }

    public IndividualMetrics calculate() {
        if (records.isEmpty()) {
            return new IndividualMetrics(
                    new IndividualMetricsSummary(
                            benchmarkKind,
                            actorKind,
                            actorId,
                            0.0,
                            0L,
                            0L,
                            0L,
                            0L,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0),
                    TimeSeries.empty(TimeSeriesKind.THROUGHPUT_MB),
                    TimeSeries.empty(TimeSeriesKind.THROUGHPUT_MSG),
                    TimeSeries.empty(TimeSeriesKind.LATENCY),
                    List.of());
        }

        BenchmarkRecord lastRecord = records.get(records.size() - 1);
        double totalTimeSecs = lastRecord.elapsedTimeUs() / 1_000_000.0;
        long totalUserDataBytes = lastRecord.userDataBytes();
        long totalBytes = lastRecord.totalBytes();
        long totalMessages = lastRecord.messages();
        long totalMessageBatches = lastRecord.messageBatches();

        TimeSeries throughputMbTs =
                StatsHelper.applyMovingAverage(calculateThroughputMbTimeSeries(), movingAverageWindow);
        TimeSeries throughputMsgTs =
                StatsHelper.applyMovingAverage(calculateThroughputMsgTimeSeries(), movingAverageWindow);
        TimeSeries latencyTs = calculateLatencyTimeSeries();

        double throughputMegabytesPerSecond;
        if (!throughputMbTs.points().isEmpty()) {
            double sum = 0.0;
            for (TimePoint point : throughputMbTs.points()) {
                sum += point.value();
            }
            throughputMegabytesPerSecond = sum / throughputMbTs.points().size();
        } else if (totalTimeSecs > 0.0) {
            throughputMegabytesPerSecond = totalUserDataBytes / 1_000_000.0 / totalTimeSecs;
        } else {
            throughputMegabytesPerSecond = 0.0;
        }

        double throughputMessagesPerSecond;
        if (!throughputMsgTs.points().isEmpty()) {
            double sum = 0.0;
            for (TimePoint point : throughputMsgTs.points()) {
                sum += point.value();
            }
            throughputMessagesPerSecond = sum / throughputMsgTs.points().size();
        } else if (totalTimeSecs > 0.0) {
            throughputMessagesPerSecond = totalMessages / totalTimeSecs;
        } else {
            throughputMessagesPerSecond = 0.0;
        }

        List<Double> rawLatenciesMs = new ArrayList<>(records.size());
        for (BenchmarkRecord record : records) {
            rawLatenciesMs.add(record.latencyUs() / 1_000.0);
        }
        rawLatenciesMs.sort((left, right) -> Double.compare(left, right));

        LatencyMetrics latencyMetrics = calculateLatencyMetrics(rawLatenciesMs, latencyTs);

        return new IndividualMetrics(
                new IndividualMetricsSummary(
                        benchmarkKind,
                        actorKind,
                        actorId,
                        Math.round(totalTimeSecs * 1000.0) / 1000.0,
                        totalUserDataBytes,
                        totalBytes,
                        totalMessages,
                        totalMessageBatches,
                        Math.round(throughputMegabytesPerSecond * 1000.0) / 1000.0,
                        Math.round(throughputMessagesPerSecond * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p50() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p90() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p95() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p99() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p999() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.p9999() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.avg() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.median() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.min() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.max() * 1000.0) / 1000.0,
                        Math.round(latencyMetrics.stdDev() * 1000.0) / 1000.0),
                throughputMbTs,
                throughputMsgTs,
                latencyTs,
                rawLatenciesMs);
    }

    private TimeSeries calculateThroughputMbTimeSeries() {
        if (records.size() < 2) {
            return TimeSeries.empty(TimeSeriesKind.THROUGHPUT_MB);
        }

        long bucketSizeUs = samplingTimeMs * 1_000L;
        long maxTimeUs = 0L;
        for (BenchmarkRecord record : records) {
            maxTimeUs = Math.max(maxTimeUs, record.elapsedTimeUs());
        }
        int bucketCount = (int) ((maxTimeUs + bucketSizeUs - 1L) / bucketSizeUs);
        long[] valuesPerBucket = new long[bucketCount];

        for (int index = 1; index < records.size(); index++) {
            BenchmarkRecord previous = records.get(index - 1);
            BenchmarkRecord current = records.get(index);

            long intervalDurationUs = current.elapsedTimeUs() - previous.elapsedTimeUs();
            if (intervalDurationUs == 0L) {
                continue;
            }

            long intervalUserDataBytes = Math.max(0L, current.userDataBytes() - previous.userDataBytes());
            double userDataBytesPerUs = intervalUserDataBytes / (double) intervalDurationUs;

            long remainingIntervalUs = intervalDurationUs;
            long allocationCursorUs = previous.elapsedTimeUs();

            while (remainingIntervalUs > 0L) {
                long bucketIndex = allocationCursorUs / bucketSizeUs;
                if (bucketIndex >= valuesPerBucket.length) {
                    break;
                }

                long bucketStartUs = bucketIndex * bucketSizeUs;
                long bucketEndUs = bucketStartUs + bucketSizeUs;
                long overlapStartUs = Math.max(allocationCursorUs, bucketStartUs);
                long overlapEndUs = Math.min(allocationCursorUs + remainingIntervalUs, bucketEndUs);
                long overlapUs = overlapEndUs - overlapStartUs;

                if (overlapUs > 0L) {
                    long allocatedValue = Math.round(userDataBytesPerUs * overlapUs);
                    valuesPerBucket[(int) bucketIndex] += allocatedValue;
                }

                long allocatedTimeUs = overlapEndUs - allocationCursorUs;
                remainingIntervalUs -= allocatedTimeUs;
                allocationCursorUs = overlapEndUs;
            }
        }

        List<TimePoint> points = new ArrayList<>();
        for (int index = 0; index < valuesPerBucket.length; index++) {
            long bucketValue = valuesPerBucket[index];
            if (bucketValue <= 0L) {
                continue;
            }

            double timeSeconds = (long) index * bucketSizeUs / 1_000_000.0;
            double throughput = (bucketValue / 1_000_000.0) / (bucketSizeUs / 1_000_000.0);
            points.add(new TimePoint(timeSeconds, Math.round(throughput * 1000.0) / 1000.0));
        }

        return new TimeSeries(points, TimeSeriesKind.THROUGHPUT_MB);
    }

    private TimeSeries calculateThroughputMsgTimeSeries() {
        if (records.size() < 2) {
            return TimeSeries.empty(TimeSeriesKind.THROUGHPUT_MSG);
        }

        long bucketSizeUs = samplingTimeMs * 1_000L;
        long maxTimeUs = 0L;
        for (BenchmarkRecord record : records) {
            maxTimeUs = Math.max(maxTimeUs, record.elapsedTimeUs());
        }
        int bucketCount = (int) ((maxTimeUs + bucketSizeUs - 1L) / bucketSizeUs);
        long[] valuesPerBucket = new long[bucketCount];

        for (int index = 1; index < records.size(); index++) {
            BenchmarkRecord previous = records.get(index - 1);
            BenchmarkRecord current = records.get(index);

            long deltaTimeUs = current.elapsedTimeUs() - previous.elapsedTimeUs();
            if (deltaTimeUs == 0L) {
                continue;
            }

            long deltaValue = Math.max(0L, current.messages() - previous.messages());
            double valuePerUs = deltaValue / (double) deltaTimeUs;

            long remainingTimeUs = deltaTimeUs;
            long currentTimeUs = previous.elapsedTimeUs();

            while (remainingTimeUs > 0L) {
                long bucketIndex = currentTimeUs / bucketSizeUs;
                if (bucketIndex >= valuesPerBucket.length) {
                    break;
                }

                long bucketStartUs = bucketIndex * bucketSizeUs;
                long bucketEndUs = bucketStartUs + bucketSizeUs;
                long overlapStartUs = Math.max(currentTimeUs, bucketStartUs);
                long overlapEndUs = Math.min(currentTimeUs + remainingTimeUs, bucketEndUs);
                long overlapUs = overlapEndUs - overlapStartUs;

                if (overlapUs > 0L) {
                    long allocatedValue = Math.round(valuePerUs * overlapUs);
                    valuesPerBucket[(int) bucketIndex] += allocatedValue;
                }

                long allocatedTimeUs = overlapEndUs - currentTimeUs;
                remainingTimeUs -= allocatedTimeUs;
                currentTimeUs = overlapEndUs;
            }
        }

        List<TimePoint> points = new ArrayList<>();
        for (int index = 0; index < valuesPerBucket.length; index++) {
            long bucketValue = valuesPerBucket[index];
            if (bucketValue <= 0L) {
                continue;
            }

            double timeSeconds = (long) index * bucketSizeUs / 1_000_000.0;
            double throughput = bucketValue / (bucketSizeUs / 1_000_000.0);
            points.add(new TimePoint(timeSeconds, Math.round(throughput * 1000.0) / 1000.0));
        }

        return new TimeSeries(points, TimeSeriesKind.THROUGHPUT_MSG);
    }

    private TimeSeries calculateLatencyTimeSeries() {
        if (records.size() < 2) {
            return TimeSeries.empty(TimeSeriesKind.LATENCY);
        }

        long bucketSizeUs = samplingTimeMs * 1_000L;
        long maxTimeUs = 0L;
        for (BenchmarkRecord record : records) {
            maxTimeUs = Math.max(maxTimeUs, record.elapsedTimeUs());
        }
        int bucketCount = (int) ((maxTimeUs + bucketSizeUs - 1L) / bucketSizeUs);
        long[] totalLatencyPerBucket = new long[bucketCount];
        long[] recordCountPerBucket = new long[bucketCount];

        for (BenchmarkRecord record : records) {
            long bucketIndex = record.elapsedTimeUs() / bucketSizeUs;
            if (bucketIndex >= totalLatencyPerBucket.length) {
                continue;
            }

            totalLatencyPerBucket[(int) bucketIndex] += record.latencyUs();
            recordCountPerBucket[(int) bucketIndex] += 1L;
        }

        List<TimePoint> points = new ArrayList<>();
        for (int index = 0; index < totalLatencyPerBucket.length; index++) {
            if (recordCountPerBucket[index] == 0L) {
                continue;
            }

            double timeSeconds = (long) index * bucketSizeUs / 1_000_000.0;
            double averageLatencyUs = totalLatencyPerBucket[index] / (double) recordCountPerBucket[index];
            double latencyMs = averageLatencyUs / 1_000.0;
            points.add(new TimePoint(timeSeconds, Math.round(latencyMs * 1000.0) / 1000.0));
        }

        return new TimeSeries(points, TimeSeriesKind.LATENCY);
    }

    private LatencyMetrics calculateLatencyMetrics(List<Double> sortedLatenciesMs, TimeSeries latencyTs) {
        double average = StatsHelper.mean(sortedLatenciesMs).orElse(0.0);

        int middle = sortedLatenciesMs.size() / 2;
        double median = sortedLatenciesMs.size() % 2 == 0
                ? (sortedLatenciesMs.get(middle - 1) + sortedLatenciesMs.get(middle)) / 2.0
                : sortedLatenciesMs.get(middle);

        return new LatencyMetrics(
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.50),
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.90),
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.95),
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.99),
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.999),
                StatsHelper.calculatePercentile(sortedLatenciesMs, 0.9999),
                average,
                median,
                StatsHelper.min(latencyTs).orElse(0.0),
                StatsHelper.max(latencyTs).orElse(0.0),
                StatsHelper.stdDev(latencyTs).orElse(0.0));
    }
}
