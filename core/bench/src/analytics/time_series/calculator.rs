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

use super::calculators::{
    LatencyTimeSeriesCalculator, MBThroughputCalculator, MessageThroughputCalculator,
    ThroughputTimeSeriesCalculator, TimeSeriesCalculation,
};
use crate::analytics::record::BenchmarkRecord;
use bench_report::time_series::{TimePoint, TimeSeries, TimeSeriesKind};
use iggy::prelude::IggyDuration;
use rayon::prelude::*;
use tracing::warn;

/// Calculate time series data from benchmark records
pub struct TimeSeriesCalculator;

impl TimeSeriesCalculator {
    pub fn throughput_mb(records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        let calculator = ThroughputTimeSeriesCalculator::new(MBThroughputCalculator);
        calculator.calculate(records, bucket_size)
    }

    pub fn throughput_msg(records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        let calculator = ThroughputTimeSeriesCalculator::new(MessageThroughputCalculator);
        calculator.calculate(records, bucket_size)
    }

    pub fn latency(records: &[BenchmarkRecord], bucket_size: IggyDuration) -> TimeSeries {
        let calculator = LatencyTimeSeriesCalculator;
        calculator.calculate(records, bucket_size)
    }

    pub fn aggregate_sum(series: &[TimeSeries]) -> TimeSeries {
        if series.is_empty() {
            warn!("Attempting to aggregate empty series");
            return TimeSeries {
                points: Vec::new(),
                kind: TimeSeriesKind::default(),
            };
        }

        let kind = series[0].kind;
        let mut all_times = series
            .iter()
            .flat_map(|s| s.points.iter().map(|p: &TimePoint| p.time_s))
            .collect::<Vec<_>>();
        all_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        all_times.dedup();

        let points = all_times
            .into_par_iter()
            .map(|time| {
                let sum: f64 = series
                    .iter()
                    .filter_map(|s| {
                        s.points
                            .iter()
                            .find(|p| (p.time_s - time).abs() < f64::EPSILON)
                            .map(|p| p.value)
                    })
                    .sum();
                TimePoint::new(time, sum)
            })
            .collect();

        TimeSeries { points, kind }
    }

    pub fn aggregate_avg(series: &[TimeSeries]) -> TimeSeries {
        if series.is_empty() {
            warn!("Attempting to aggregate empty series");
            return TimeSeries {
                points: Vec::new(),
                kind: TimeSeriesKind::default(),
            };
        }

        let kind = series[0].kind;

        let mut all_times = series
            .iter()
            .flat_map(|s| s.points.iter().map(|p| p.time_s))
            .collect::<Vec<_>>();
        all_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        all_times.dedup();

        let points = all_times
            .into_par_iter()
            .map(|time| {
                let matching_values: Vec<f64> = series
                    .iter()
                    .filter_map(|s| {
                        s.points
                            .iter()
                            .find(|p| (p.time_s - time).abs() < f64::EPSILON)
                            .map(|p| p.value)
                    })
                    .collect();

                let count = matching_values.len();
                let sum: f64 = matching_values.iter().sum();
                let avg = if count == 0 { 0.0 } else { sum / count as f64 };
                TimePoint::new(time, avg)
            })
            .collect();

        TimeSeries { points, kind }
    }
}
