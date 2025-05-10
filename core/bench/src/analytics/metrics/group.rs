/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::analytics::time_series::{
    calculator::TimeSeriesCalculator,
    processors::{TimeSeriesProcessor, moving_average::MovingAverageProcessor},
};
use bench_report::{
    actor_kind::ActorKind, group_metrics::BenchmarkGroupMetrics,
    group_metrics_kind::GroupMetricsKind, group_metrics_summary::BenchmarkGroupMetricsSummary,
    individual_metrics::BenchmarkIndividualMetrics, utils::*,
};

pub fn from_producers_and_consumers_statistics(
    producers_stats: &[BenchmarkIndividualMetrics],
    consumers_stats: &[BenchmarkIndividualMetrics],
    moving_average_window: u32,
) -> Option<BenchmarkGroupMetrics> {
    let mut summary = from_individual_metrics(
        &[producers_stats, consumers_stats].concat(),
        moving_average_window,
    )?;
    summary.summary.kind = GroupMetricsKind::ProducersAndConsumers;
    Some(summary)
}

pub fn from_individual_metrics(
    stats: &[BenchmarkIndividualMetrics],
    moving_average_window: u32,
) -> Option<BenchmarkGroupMetrics> {
    if stats.is_empty() {
        return None;
    }
    let count = stats.len() as f64;

    // Compute aggregate throughput MB/s
    let total_throughput_megabytes_per_second: f64 = stats
        .iter()
        .map(|r| r.summary.throughput_megabytes_per_second)
        .sum();

    // Compute aggregate throughput messages/s
    let total_throughput_messages_per_second: f64 = stats
        .iter()
        .map(|r| r.summary.throughput_messages_per_second)
        .sum();

    // Compute average throughput MB/s
    let average_throughput_megabytes_per_second = total_throughput_megabytes_per_second / count;

    // Compute average throughput messages/s
    let average_throughput_messages_per_second = total_throughput_messages_per_second / count;

    // Compute average latencies
    let average_p50_latency_ms =
        stats.iter().map(|r| r.summary.p50_latency_ms).sum::<f64>() / count;
    let average_p90_latency_ms =
        stats.iter().map(|r| r.summary.p90_latency_ms).sum::<f64>() / count;
    let average_p95_latency_ms =
        stats.iter().map(|r| r.summary.p95_latency_ms).sum::<f64>() / count;
    let average_p99_latency_ms =
        stats.iter().map(|r| r.summary.p99_latency_ms).sum::<f64>() / count;
    let average_p999_latency_ms: f64 =
        stats.iter().map(|r| r.summary.p999_latency_ms).sum::<f64>() / count;
    let average_p9999_latency_ms: f64 = stats
        .iter()
        .map(|r| r.summary.p9999_latency_ms)
        .sum::<f64>()
        / count;
    let average_avg_latency_ms =
        stats.iter().map(|r| r.summary.avg_latency_ms).sum::<f64>() / count;
    let average_median_latency_ms = stats
        .iter()
        .map(|r| r.summary.median_latency_ms)
        .sum::<f64>()
        / count;

    let kind = match stats.iter().next().unwrap().summary.actor_kind {
        ActorKind::Producer => GroupMetricsKind::Producers,
        ActorKind::Consumer => GroupMetricsKind::Consumers,
        ActorKind::ProducingConsumer => GroupMetricsKind::ProducingConsumers,
    };

    let calculator = TimeSeriesCalculator::new();

    let mut avg_throughput_mb_ts = calculator.aggregate_sum(
        stats
            .iter()
            .map(|r| r.throughput_mb_ts.clone())
            .collect::<Vec<_>>()
            .as_slice(),
    );
    let mut avg_throughput_msg_ts = calculator.aggregate_sum(
        stats
            .iter()
            .map(|r| r.throughput_msg_ts.clone())
            .collect::<Vec<_>>()
            .as_slice(),
    );
    let mut avg_latency_ts = calculator.aggregate_avg(
        stats
            .iter()
            .map(|r| r.latency_ts.clone())
            .collect::<Vec<_>>()
            .as_slice(),
    );

    let sma = MovingAverageProcessor::new(moving_average_window as usize);
    avg_throughput_mb_ts = sma.process(&avg_throughput_mb_ts);
    avg_throughput_msg_ts = sma.process(&avg_throughput_msg_ts);
    avg_latency_ts = sma.process(&avg_latency_ts);

    let min_latency_ms = if !stats.is_empty() {
        stats
            .iter()
            .map(|s| s.summary.min_latency_ms)
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    } else {
        None
    };

    let max_latency_ms = if !stats.is_empty() {
        stats
            .iter()
            .map(|s| s.summary.max_latency_ms)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
    } else {
        None
    };

    let min_latency_ms_value =
        min_latency_ms.unwrap_or_else(|| min(&avg_latency_ts).unwrap_or(0.0));

    let max_latency_ms_value =
        max_latency_ms.unwrap_or_else(|| max(&avg_latency_ts).unwrap_or(0.0));

    let std_dev_latency_ms = std_dev(&avg_latency_ts);

    let summary = BenchmarkGroupMetricsSummary {
        kind,
        total_throughput_megabytes_per_second,
        total_throughput_messages_per_second,
        average_throughput_megabytes_per_second,
        average_throughput_messages_per_second,
        average_p50_latency_ms,
        average_p90_latency_ms,
        average_p95_latency_ms,
        average_p99_latency_ms,
        average_p999_latency_ms,
        average_p9999_latency_ms,
        average_latency_ms: average_avg_latency_ms,
        average_median_latency_ms,
        min_latency_ms: min_latency_ms_value,
        max_latency_ms: max_latency_ms_value,
        std_dev_latency_ms: std_dev_latency_ms.unwrap_or(0.0),
    };

    Some(BenchmarkGroupMetrics {
        summary,
        avg_throughput_mb_ts,
        avg_throughput_msg_ts,
        avg_latency_ts,
    })
}
