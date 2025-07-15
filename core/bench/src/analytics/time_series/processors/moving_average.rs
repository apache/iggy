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

use super::TimeSeriesProcessor;
use bench_report::time_series::{TimePoint, TimeSeries};
use std::collections::VecDeque;
use tracing::warn;

/// Moving average processor
pub struct MovingAverageProcessor {
    window_size: usize,
}

impl MovingAverageProcessor {
    pub const fn new(window_size: usize) -> Self {
        Self { window_size }
    }
}

impl TimeSeriesProcessor for MovingAverageProcessor {
    fn process(&self, data: &TimeSeries) -> TimeSeries {
        if data.points.is_empty() {
            warn!("Attempting to process empty series");
            return data.clone();
        }

        let mut window: VecDeque<f64> = VecDeque::with_capacity(self.window_size);
        let mut points = Vec::with_capacity(data.points.len());
        let mut sum = 0.0;

        for point in &data.points {
            window.push_back(point.value);
            sum += point.value;

            if window.len() > self.window_size {
                if let Some(old_value) = window.pop_front() {
                    sum -= old_value;
                }
            }

            let avg = sum / window.len() as f64;
            let rounded_avg = (avg * 1000.0).round() / 1000.0;
            points.push(TimePoint::new(point.time_s, rounded_avg));
        }

        TimeSeries {
            points,
            kind: data.kind,
        }
    }
}
