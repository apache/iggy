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

use crate::time_series::TimeSeries;
use serde::Serializer;

pub(crate) fn round_float<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64((value * 1000.0).round() / 1000.0)
}

/// Calculate the minimum value from a TimeSeries
///
/// Returns None if the TimeSeries has no points
pub fn min(series: &TimeSeries) -> Option<f64> {
    series
        .points
        .iter()
        .map(|p| p.value)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Calculate the maximum value from a TimeSeries
///
/// Returns None if the TimeSeries has no points
pub fn max(series: &TimeSeries) -> Option<f64> {
    series
        .points
        .iter()
        .map(|p| p.value)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}

/// Calculate the standard deviation of values from a TimeSeries
///
/// Returns None if the TimeSeries has fewer than 2 points
pub fn std_dev(series: &TimeSeries) -> Option<f64> {
    let points_count = series.points.len();

    if points_count < 2 {
        return None;
    }

    let sum: f64 = series.points.iter().map(|p| p.value).sum();
    let mean = sum / points_count as f64;

    let variance = series
        .points
        .iter()
        .map(|p| {
            let diff = p.value - mean;
            diff * diff
        })
        .sum::<f64>()
        / points_count as f64;

    Some(variance.sqrt())
}
