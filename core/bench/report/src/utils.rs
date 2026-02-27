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

use crate::time_series::{TimePoint, TimeSeries};
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

/// LTTB (Largest-Triangle-Three-Buckets) downsampling.
///
/// Reduces `points` to at most `threshold` points while preserving visual shape
/// (peaks, valleys, trends). Returns a clone if `points.len() <= threshold`.
pub fn lttb_downsample(points: &[TimePoint], threshold: usize) -> Vec<TimePoint> {
    let len = points.len();
    if len <= threshold || threshold < 3 {
        return points.to_vec();
    }

    let bucket_count = threshold - 2;
    let bucket_size = (len - 2) as f64 / bucket_count as f64;

    let mut result = Vec::with_capacity(threshold);
    result.push(points[0].clone());

    let mut prev_selected = 0usize;

    for bucket_idx in 0..bucket_count {
        // Compute average of the *next* bucket (used as the third triangle vertex)
        let next_start = ((bucket_idx + 1) as f64 * bucket_size) as usize + 1;
        let next_end = (((bucket_idx + 2) as f64 * bucket_size) as usize + 1).min(len - 1);

        let mut avg_time = 0.0;
        let mut avg_value = 0.0;
        let next_count = (next_end - next_start + 1) as f64;
        for p in &points[next_start..=next_end] {
            avg_time += p.time_s;
            avg_value += p.value;
        }
        avg_time /= next_count;
        avg_value /= next_count;

        // Current bucket range
        let cur_start = (bucket_idx as f64 * bucket_size) as usize + 1;
        let cur_end = next_start;

        // Pick the point in this bucket that forms the largest triangle with
        // the previously selected point and the next-bucket average.
        let prev = &points[prev_selected];
        let mut max_area = -1.0;
        let mut best = cur_start;
        for (i, p) in points[cur_start..cur_end].iter().enumerate() {
            let area = ((prev.time_s - avg_time) * (p.value - prev.value)
                - (prev.time_s - p.time_s) * (avg_value - prev.value))
                .abs();
            if area > max_area {
                max_area = area;
                best = cur_start + i;
            }
        }

        result.push(points[best].clone());
        prev_selected = best;
    }

    result.push(points[len - 1].clone());
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_points(values: impl IntoIterator<Item = f64>) -> Vec<TimePoint> {
        values
            .into_iter()
            .enumerate()
            .map(|(i, v)| TimePoint::new(i as f64, v))
            .collect()
    }

    #[test]
    fn lttb_passthrough_when_below_threshold() {
        let pts = make_points([1.0, 2.0, 3.0]);
        let result = lttb_downsample(&pts, 5);
        assert_eq!(result, pts);
    }

    #[test]
    fn lttb_passthrough_when_equal_to_threshold() {
        let pts = make_points([1.0, 2.0, 3.0, 4.0, 5.0]);
        let result = lttb_downsample(&pts, 5);
        assert_eq!(result, pts);
    }

    #[test]
    fn lttb_reduces_count() {
        let pts = make_points((0..10_000).map(|i| (i as f64).sin()));
        let result = lttb_downsample(&pts, 100);
        assert_eq!(result.len(), 100);
    }

    #[test]
    fn lttb_preserves_endpoints() {
        let pts = make_points((0..1000).map(|i| i as f64 * 0.1));
        let result = lttb_downsample(&pts, 50);
        assert_eq!(result.first().unwrap().time_s, pts.first().unwrap().time_s);
        assert_eq!(result.last().unwrap().time_s, pts.last().unwrap().time_s);
    }

    #[test]
    fn lttb_preserves_peaks() {
        // Triangle wave with clear peaks at indices 50, 150, 250, ...
        let pts: Vec<TimePoint> = (0..500)
            .map(|i| {
                let v = if (i / 50) % 2 == 0 {
                    (i % 50) as f64
                } else {
                    (50 - i % 50) as f64
                };
                TimePoint::new(i as f64, v)
            })
            .collect();

        let result = lttb_downsample(&pts, 100);
        let result_values: Vec<f64> = result.iter().map(|p| p.value).collect();
        let max_val = result_values
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        // LTTB should retain the peaks (value = 50) in the downsampled output
        assert!(
            (max_val - 50.0).abs() < f64::EPSILON,
            "peak 50.0 not preserved, got max {max_val}"
        );
    }

    #[test]
    fn lttb_edge_cases() {
        assert!(lttb_downsample(&[], 10).is_empty());
        let one = make_points([42.0]);
        assert_eq!(lttb_downsample(&one, 10), one);
        let two = make_points([1.0, 2.0]);
        assert_eq!(lttb_downsample(&two, 10), two);
    }
}
