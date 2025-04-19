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

use serde::{Deserialize, Serialize};

/// A point in time series data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TimePoint {
    pub time_s: f64,
    pub value: f64,
}

impl TimePoint {
    pub fn new(time_s: f64, value: f64) -> Self {
        Self { time_s, value }
    }
}

/// Time series data with associated metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TimeSeries {
    pub points: Vec<TimePoint>,
    #[serde(skip)]
    pub kind: TimeSeriesKind,
}

/// Types of time series data we can calculate
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum TimeSeriesKind {
    #[default]
    ThroughputMB,
    ThroughputMsg,
    Latency,
}

impl TimeSeries {
    pub fn as_charming_points(&self) -> Vec<Vec<f64>> {
        self.points
            .iter()
            .map(|p| vec![p.time_s, p.value])
            .collect()
    }
}
