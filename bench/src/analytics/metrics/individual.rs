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

use crate::analytics::record::BenchmarkRecord;
use crate::analytics::time_series::calculator::TimeSeriesCalculator;
use crate::analytics::time_series::processors::moving_average::MovingAverageProcessor;
use crate::analytics::time_series::processors::TimeSeriesProcessor;
use iggy::utils::duration::IggyDuration;
use iggy_bench_report::actor_kind::ActorKind;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::individual_metrics::BenchmarkIndividualMetrics;
use iggy_bench_report::individual_metrics_summary::BenchmarkIndividualMetricsSummary;
use iggy_bench_report::time_series::TimeSeries;
use iggy_bench_report::utils::*;

pub fn from_records(
    records: Vec<BenchmarkRecord>,
    benchmark_kind: BenchmarkKind,
    actor_kind: ActorKind,
    actor_id: u32,
    sampling_time: IggyDuration,
    moving_average_window: u32,
) -> BenchmarkIndividualMetrics {
    if records.is_empty() {
        return BenchmarkIndividualMetrics {
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
        };
    }

    let total_time_secs = records.last().unwrap().elapsed_time_us as f64 / 1_000_000.0;

    let total_user_data_bytes = records.iter().last().unwrap().user_data_bytes;
    let total_bytes = records.iter().last().unwrap().total_bytes;
    let total_messages = records.iter().last().unwrap().messages;
    let total_message_batches = records.iter().last().unwrap().message_batches;

    let calculator = TimeSeriesCalculator::new();

    let throughput_mb_ts = calculator.throughput_mb(&records, sampling_time);
    let throughput_msg_ts = calculator.throughput_msg(&records, sampling_time);

    let sma = MovingAverageProcessor::new(moving_average_window as usize);
    let throughput_mb_ts = sma.process(&throughput_mb_ts);
    let throughput_msg_ts = sma.process(&throughput_msg_ts);

    let throughput_megabytes_per_second = if !throughput_mb_ts.points.is_empty() {
        throughput_mb_ts
            .points
            .iter()
            .map(|point| point.value)
            .sum::<f64>()
            / throughput_mb_ts.points.len() as f64
    } else if total_time_secs > 0.0 {
        (total_user_data_bytes as f64) / 1_000_000.0 / total_time_secs
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
        (total_messages as f64) / total_time_secs
    } else {
        0.0
    };

    let mut latencies_ms: Vec<f64> = records
        .iter()
        .map(|r| r.latency_us as f64 / 1_000.0)
        .collect();
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50_latency_ms = calculate_percentile(&latencies_ms, 50.0);
    let p90_latency_ms = calculate_percentile(&latencies_ms, 90.0);
    let p95_latency_ms = calculate_percentile(&latencies_ms, 95.0);
    let p99_latency_ms = calculate_percentile(&latencies_ms, 99.0);
    let p999_latency_ms = calculate_percentile(&latencies_ms, 99.9);
    let p9999_latency_ms = calculate_percentile(&latencies_ms, 99.99);

    let avg_latency_ms = latencies_ms.iter().sum::<f64>() / latencies_ms.len() as f64;
    let len = latencies_ms.len() / 2;
    let median_latency_ms = if latencies_ms.len() % 2 == 0 {
        (latencies_ms[len - 1] + latencies_ms[len]) / 2.0
    } else {
        latencies_ms[len]
    };

    let latency_ts = calculator.latency(&records, sampling_time);

    let min_latency_ms = min(&latency_ts).unwrap_or(0.0);
    let max_latency_ms = max(&latency_ts).unwrap_or(0.0);
    let std_dev_latency_ms = std_dev(&latency_ts).unwrap_or(0.0);

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
            p50_latency_ms,
            p90_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            p999_latency_ms,
            p9999_latency_ms,
            avg_latency_ms,
            median_latency_ms,
            min_latency_ms,
            max_latency_ms,
            std_dev_latency_ms,
        },
        throughput_mb_ts,
        throughput_msg_ts,
        latency_ts,
    }
}

fn calculate_percentile(sorted_data: &[f64], percentile: f64) -> f64 {
    if sorted_data.is_empty() {
        return 0.0;
    }

    let rank = percentile / 100.0 * (sorted_data.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;

    if upper >= sorted_data.len() {
        return sorted_data[sorted_data.len() - 1];
    }

    let weight = rank - lower as f64;
    sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
}
