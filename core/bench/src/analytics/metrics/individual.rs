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

#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use crate::analytics::record::BenchmarkRecord;
use crate::analytics::time_series::calculator::TimeSeriesCalculator;
use crate::analytics::time_series::processors::TimeSeriesProcessor;
use crate::analytics::time_series::processors::moving_average::MovingAverageProcessor;
use bench_report::actor_kind::ActorKind;
use bench_report::benchmark_kind::BenchmarkKind;
use bench_report::individual_metrics::BenchmarkIndividualMetrics;
use bench_report::individual_metrics_summary::BenchmarkIndividualMetricsSummary;
use bench_report::time_series::TimeSeries;
use bench_report::utils::{max, min, std_dev};
use iggy::prelude::IggyDuration;

pub fn from_records(
    records: &[BenchmarkRecord],
    benchmark_kind: BenchmarkKind,
    actor_kind: ActorKind,
    actor_id: u32,
    sampling_time: IggyDuration,
    moving_average_window: u32,
) -> BenchmarkIndividualMetrics {
    if records.is_empty() {
        return create_empty_metrics(benchmark_kind, actor_kind, actor_id);
    }

    let (
        total_time_secs,
        total_user_data_bytes,
        total_bytes,
        total_messages,
        total_message_batches,
    ) = extract_totals(records);

    let (throughput_mb_ts, throughput_msg_ts, latency_ts) =
        calculate_time_series(records, sampling_time, moving_average_window);

    let (throughput_megabytes_per_second, throughput_messages_per_second) = calculate_throughput(
        &throughput_mb_ts,
        &throughput_msg_ts,
        total_time_secs,
        total_user_data_bytes,
        total_messages,
    );

    let latency_metrics = calculate_latency_metrics(records, &latency_ts);

    BenchmarkIndividualMetrics {
        summary: BenchmarkIndividualMetricsSummary {
            benchmark_kind,
            actor_kind,
            actor_id,
            total_time_secs,
            total_user_data_bytes,
            total_bytes,
            total_messages,
            total_message_batches,
            throughput_megabytes_per_second,
            throughput_messages_per_second,
            p50_latency_ms: latency_metrics.p50,
            p90_latency_ms: latency_metrics.p90,
            p95_latency_ms: latency_metrics.p95,
            p99_latency_ms: latency_metrics.p99,
            p999_latency_ms: latency_metrics.p999,
            p9999_latency_ms: latency_metrics.p9999,
            avg_latency_ms: latency_metrics.avg,
            median_latency_ms: latency_metrics.median,
            min_latency_ms: latency_metrics.min,
            max_latency_ms: latency_metrics.max,
            std_dev_latency_ms: latency_metrics.std_dev,
        },
        throughput_mb_ts,
        throughput_msg_ts,
        latency_ts,
    }
}

fn create_empty_metrics(
    benchmark_kind: BenchmarkKind,
    actor_kind: ActorKind,
    actor_id: u32,
) -> BenchmarkIndividualMetrics {
    BenchmarkIndividualMetrics {
        summary: BenchmarkIndividualMetricsSummary {
            benchmark_kind,
            actor_kind,
            actor_id,
            total_time_secs: 0.0,
            total_user_data_bytes: 0,
            total_bytes: 0,
            total_messages: 0,
            total_message_batches: 0,
            throughput_megabytes_per_second: 0.0,
            throughput_messages_per_second: 0.0,
            p50_latency_ms: 0.0,
            p90_latency_ms: 0.0,
            p95_latency_ms: 0.0,
            p99_latency_ms: 0.0,
            p999_latency_ms: 0.0,
            p9999_latency_ms: 0.0,
            avg_latency_ms: 0.0,
            median_latency_ms: 0.0,
            min_latency_ms: 0.0,
            max_latency_ms: 0.0,
            std_dev_latency_ms: 0.0,
        },
        throughput_mb_ts: TimeSeries::default(),
        throughput_msg_ts: TimeSeries::default(),
        latency_ts: TimeSeries::default(),
    }
}

fn extract_totals(records: &[BenchmarkRecord]) -> (f64, u64, u64, u64, u64) {
    let last_record = records.last().unwrap();
    let total_time_secs = last_record.elapsed_time_us as f64 / 1_000_000.0;
    (
        total_time_secs,
        last_record.user_data_bytes,
        last_record.total_bytes,
        last_record.messages,
        last_record.message_batches,
    )
}

fn calculate_time_series(
    records: &[BenchmarkRecord],
    sampling_time: IggyDuration,
    moving_average_window: u32,
) -> (TimeSeries, TimeSeries, TimeSeries) {
    let throughput_mb_ts = TimeSeriesCalculator::throughput_mb(records, sampling_time);
    let throughput_msg_ts = TimeSeriesCalculator::throughput_msg(records, sampling_time);

    let sma = MovingAverageProcessor::new(moving_average_window as usize);
    let throughput_mb_ts = sma.process(&throughput_mb_ts);
    let throughput_msg_ts = sma.process(&throughput_msg_ts);

    let latency_ts = TimeSeriesCalculator::latency(records, sampling_time);

    (throughput_mb_ts, throughput_msg_ts, latency_ts)
}

fn calculate_throughput(
    throughput_mb_ts: &TimeSeries,
    throughput_msg_ts: &TimeSeries,
    total_time_secs: f64,
    total_user_data_bytes: u64,
    total_messages: u64,
) -> (f64, f64) {
    let throughput_megabytes_per_second = if !throughput_mb_ts.points.is_empty() {
        throughput_mb_ts
            .points
            .iter()
            .map(|point| point.value)
            .sum::<f64>()
            / throughput_mb_ts.points.len() as f64
    } else if total_time_secs > 0.0 {
        total_user_data_bytes as f64 / 1_000_000.0 / total_time_secs
    } else {
        0.0
    };

    let throughput_messages_per_second = if !throughput_msg_ts.points.is_empty() {
        throughput_msg_ts
            .points
            .iter()
            .map(|point| point.value)
            .sum::<f64>()
            / throughput_msg_ts.points.len() as f64
    } else if total_time_secs > 0.0 {
        total_messages as f64 / total_time_secs
    } else {
        0.0
    };

    (
        throughput_megabytes_per_second,
        throughput_messages_per_second,
    )
}

struct LatencyMetrics {
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
    avg: f64,
    median: f64,
    min: f64,
    max: f64,
    std_dev: f64,
}

fn calculate_latency_metrics(
    records: &[BenchmarkRecord],
    latency_ts: &TimeSeries,
) -> LatencyMetrics {
    let mut latencies_ms: Vec<f64> = records
        .iter()
        .map(|r| r.latency_us as f64 / 1_000.0)
        .collect();
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = calculate_percentile(&latencies_ms, 50.0);
    let p90 = calculate_percentile(&latencies_ms, 90.0);
    let p95 = calculate_percentile(&latencies_ms, 95.0);
    let p99 = calculate_percentile(&latencies_ms, 99.0);
    let p999 = calculate_percentile(&latencies_ms, 99.9);
    let p9999 = calculate_percentile(&latencies_ms, 99.99);

    let avg = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;
    let len = latencies_ms.len() / 2;
    let median = if latencies_ms.len().is_multiple_of(2) {
        f64::midpoint(latencies_ms[len - 1], latencies_ms[len])
    } else {
        latencies_ms[len]
    };

    let min = min(latency_ts).unwrap_or(0.0);
    let max = max(latency_ts).unwrap_or(0.0);
    let std_dev = std_dev(latency_ts).unwrap_or(0.0);

    LatencyMetrics {
        p50,
        p90,
        p95,
        p99,
        p999,
        p9999,
        avg,
        median,
        min,
        max,
        std_dev,
    }
}

fn calculate_percentile(sorted_data: &[f64], percentile: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }

    let rank = percentile / 100.0 * (sorted_data.len() - 1) as f64;
    let lower = rank.floor().clamp(0.0, (sorted_data.len() - 1) as f64) as usize;
    let upper = rank.ceil().clamp(0.0, (sorted_data.len() - 1) as f64) as usize;

    if upper >= sorted_data.len() {
        return sorted_data[sorted_data.len() - 1];
    }

    let weight = rank - lower as f64;
    sorted_data[lower].mul_add(1.0 - weight, sorted_data[upper] * weight)
}
