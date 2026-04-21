// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::cmp::Ordering;

/// Format milliseconds with adaptive precision (100+ = int, 10+ = 1dp, 1+ = 2dp, else 3dp).
pub fn format_ms(value: f64) -> String {
    if !value.is_finite() || value < 0.0 {
        return "-".to_string();
    }
    if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        format!("{value:.1}")
    } else if value >= 1.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.3}")
    }
}

/// Format throughput in MB/s, auto-scaling to GB/s or TB/s for large values.
pub fn format_throughput_mb_s(megabytes_per_second: f64) -> String {
    if !megabytes_per_second.is_finite() || megabytes_per_second < 0.0 {
        return "-".to_string();
    }
    if megabytes_per_second >= 1_000_000.0 {
        format!("{:.2} TB/s", megabytes_per_second / 1_000_000.0)
    } else if megabytes_per_second >= 1_000.0 {
        format!("{:.2} GB/s", megabytes_per_second / 1_000.0)
    } else if megabytes_per_second >= 100.0 {
        format!("{megabytes_per_second:.0} MB/s")
    } else {
        format!("{megabytes_per_second:.1} MB/s")
    }
}

/// Format a count with SI suffixes (k, M, B).
pub fn format_count(value: u64) -> String {
    if value >= 1_000_000_000 {
        format!("{:.2}B", value as f64 / 1_000_000_000.0)
    } else if value >= 1_000_000 {
        format!("{:.2}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

/// Format a byte count with decimal SI prefixes (matches throughput conventions).
pub fn format_bytes(value: u64) -> String {
    let value = value as f64;
    if value >= 1_000_000_000_000.0 {
        format!("{:.2} TB", value / 1_000_000_000_000.0)
    } else if value >= 1_000_000_000.0 {
        format!("{:.2} GB", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("{:.2} MB", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1} kB", value / 1_000.0)
    } else {
        format!("{value:.0} B")
    }
}

/// NaN-safe partial comparison. NaN always ranks highest (worst for min, best filtered out).
pub fn nan_safe_cmp(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
    }
}

/// Return `value` if finite, else `fallback`. Coerces NaN/inf into a safe sentinel.
pub fn finite_or(value: f64, fallback: f64) -> f64 {
    if value.is_finite() { value } else { fallback }
}
