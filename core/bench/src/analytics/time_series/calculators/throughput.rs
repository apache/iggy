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

use super::TimeSeriesCalculation;
use crate::analytics::record::BenchmarkRecord;
use iggy::utils::duration::IggyDuration;
use iggy_bench_report::time_series::{TimePoint, TimeSeries, TimeSeriesKind};

/// Common functionality for throughput calculations
pub trait ThroughputCalculation {
    fn get_delta_value(&self, current: &BenchmarkRecord, prev: &BenchmarkRecord) -> u64;
    fn calculate_throughput(&self, value: u64, bucket_size_us: u64) -> f64;
    fn kind(&self) -> TimeSeriesKind;
}

/// Calculator for MB/s throughput
pub struct MBThroughputCalculator;

impl ThroughputCalculation for MBThroughputCalculator {
    fn get_delta_value(&self, current: &BenchmarkRecord, prev: &BenchmarkRecord) -> u64 {
        current.user_data_bytes.saturating_sub(prev.user_data_bytes)
    }

    fn calculate_throughput(&self, bytes: u64, bucket_size_us: u64) -> f64 {
        (bytes as f64) / 1_000_000.0 / (bucket_size_us as f64 / 1_000_000.0)
    }

    fn kind(&self) -> TimeSeriesKind {
        TimeSeriesKind::ThroughputMB
    }
}

/// Calculator for messages/s throughput
pub struct MessageThroughputCalculator;

impl ThroughputCalculation for MessageThroughputCalculator {
    fn get_delta_value(&self, current: &BenchmarkRecord, prev: &BenchmarkRecord) -> u64 {
        current.messages.saturating_sub(prev.messages)
    }

    fn calculate_throughput(&self, messages: u64, bucket_size_us: u64) -> f64 {
        (messages as f64) / (bucket_size_us as f64 / 1_000_000.0)
    }

    fn kind(&self) -> TimeSeriesKind {
        TimeSeriesKind::ThroughputMsg
    }
}

/// Generic throughput calculator that works with different throughput metrics
pub struct ThroughputTimeSeriesCalculator<T: ThroughputCalculation> {
    calculator: T,
}

impl<T: ThroughputCalculation> ThroughputTimeSeriesCalculator<T> {
    pub fn new(calculator: T) -> Self {
        Self { calculator }
    }
}

impl<T: ThroughputCalculation> TimeSeriesCalculation for ThroughputTimeSeriesCalculator<T> {
    fn calculate(&self, records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        if records.len() < 2 {
            return TimeSeries {
                points: Vec::new(),
                kind: self.calculator.kind(),
            };
        }
        let bucket_size_us = bucket_size.as_micros();

        let max_time_us = records.iter().map(|r| r.elapsed_time_us).max().unwrap_or(0);
        let num_buckets = max_time_us.div_ceil(bucket_size_us);
        let mut values_per_bucket = vec![0u64; num_buckets as usize];

        for window in records.windows(2) {
            let (prev, current) = (&window[0], &window[1]);
            let delta_time_us = current.elapsed_time_us.saturating_sub(prev.elapsed_time_us);
            if delta_time_us == 0 {
                continue;
            }

            let delta_value = self.calculator.get_delta_value(current, prev);
            let value_per_us = delta_value as f64 / delta_time_us as f64;

            let mut remaining_time_us = delta_time_us;
            let mut current_time_us = prev.elapsed_time_us;

            while remaining_time_us > 0 {
                let bucket_index = current_time_us / bucket_size_us;
                if bucket_index >= num_buckets {
                    break;
                }

                let bucket_start_us = bucket_index * bucket_size_us;
                let bucket_end_us = bucket_start_us + bucket_size_us;
                let overlap_start_us = current_time_us.max(bucket_start_us);
                let overlap_end_us = (current_time_us + remaining_time_us).min(bucket_end_us);
                let overlap_us = overlap_end_us.saturating_sub(overlap_start_us);

                if overlap_us > 0 {
                    let allocated_value = (value_per_us * overlap_us as f64).round() as u64;
                    values_per_bucket[bucket_index as usize] += allocated_value;
                }

                let allocated_time_us = overlap_end_us.saturating_sub(current_time_us);
                remaining_time_us = remaining_time_us.saturating_sub(allocated_time_us);
                current_time_us = overlap_end_us;
            }
        }

        let points = values_per_bucket
            .iter()
            .enumerate()
            .filter(|(_, &value)| value > 0)
            .map(|(i, &value)| {
                let time_s = (i as u64 * bucket_size_us) as f64 / 1_000_000.0;
                let throughput = self.calculator.calculate_throughput(value, bucket_size_us);
                let rounded_throughput = (throughput * 1000.0).round() / 1000.0;
                TimePoint::new(time_s, rounded_throughput)
            })
            .collect();

        TimeSeries {
            points,
            kind: self.calculator.kind(),
        }
    }
}
