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

use super::TimeSeriesCalculation;
use crate::analytics::record::BenchmarkRecord;
use bench_report::time_series::{TimePoint, TimeSeries, TimeSeriesKind};
use iggy::prelude::IggyDuration;
use tracing::warn;

/// Calculator for latency time series
pub struct LatencyTimeSeriesCalculator;

impl TimeSeriesCalculation for LatencyTimeSeriesCalculator {
    // This implementation is using actual latency values and average latencies per bucket
    fn calculate(&self, records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        if records.len() < 2 {
            warn!("Not enough records to calculate latency");
            return TimeSeries {
                points: Vec::new(),
                kind: TimeSeriesKind::Latency,
            };
        }

        let bucket_size_us = bucket_size.as_micros();

        let max_time_us = records.iter().map(|r| r.elapsed_time_us).max().unwrap();
        let num_buckets = max_time_us.div_ceil(bucket_size_us);
        let mut total_latency_per_bucket = vec![0u64; num_buckets as usize];
        let mut message_count_per_bucket = vec![0u64; num_buckets as usize];

        for record in records {
            let bucket_index = record.elapsed_time_us / bucket_size_us;
            if bucket_index >= num_buckets {
                continue;
            }

            total_latency_per_bucket[bucket_index as usize] += record.latency_us;
            message_count_per_bucket[bucket_index as usize] += 1;
        }

        let points = (0..num_buckets)
            .filter(|&i| message_count_per_bucket[i as usize] > 0)
            .map(|i| {
                let time_s = (i * bucket_size_us) as f64 / 1_000_000.0;
                let avg_latency_us = total_latency_per_bucket[i as usize] as f64
                    / message_count_per_bucket[i as usize] as f64;
                let latency_ms = avg_latency_us / 1000.0;
                let rounded_latency_ms = (latency_ms * 1000.0).round() / 1000.0;
                TimePoint::new(time_s, rounded_latency_ms)
            })
            .collect();

        TimeSeries {
            points,
            kind: TimeSeriesKind::Latency,
        }
    }
}
