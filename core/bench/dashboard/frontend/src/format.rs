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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn given_small_values_when_formatting_ms_should_use_three_decimals() {
        assert_eq!(format_ms(0.0), "0.000");
        assert_eq!(format_ms(0.123456), "0.123");
    }

    #[test]
    fn given_values_across_ranges_when_formatting_ms_should_reduce_precision() {
        assert_eq!(format_ms(1.2345), "1.23");
        assert_eq!(format_ms(12.345), "12.3");
        assert_eq!(format_ms(123.45), "123");
    }

    #[test]
    fn given_nonfinite_or_negative_when_formatting_ms_should_return_dash() {
        assert_eq!(format_ms(f64::NAN), "-");
        assert_eq!(format_ms(f64::INFINITY), "-");
        assert_eq!(format_ms(-1.0), "-");
    }

    #[test]
    fn given_mb_per_second_when_formatting_throughput_should_auto_scale_units() {
        assert_eq!(format_throughput_mb_s(50.0), "50.0 MB/s");
        assert_eq!(format_throughput_mb_s(250.0), "250 MB/s");
        assert_eq!(format_throughput_mb_s(1_500.0), "1.50 GB/s");
        assert_eq!(format_throughput_mb_s(2_000_000.0), "2.00 TB/s");
    }

    #[test]
    fn given_nonfinite_or_negative_when_formatting_throughput_should_return_dash() {
        assert_eq!(format_throughput_mb_s(f64::NAN), "-");
        assert_eq!(format_throughput_mb_s(-10.0), "-");
    }

    #[test]
    fn given_counts_when_formatting_should_apply_si_suffixes() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_500), "1.5k");
        assert_eq!(format_count(2_500_000), "2.50M");
        assert_eq!(format_count(3_000_000_000), "3.00B");
    }

    #[test]
    fn given_byte_counts_when_formatting_should_apply_decimal_si_prefixes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1_500), "1.5 kB");
        assert_eq!(format_bytes(2_500_000), "2.50 MB");
        assert_eq!(format_bytes(3_000_000_000), "3.00 GB");
        assert_eq!(format_bytes(4_000_000_000_000), "4.00 TB");
    }

    #[test]
    fn given_finite_values_when_comparing_should_order_naturally() {
        assert_eq!(nan_safe_cmp(1.0, 2.0), Ordering::Less);
        assert_eq!(nan_safe_cmp(2.0, 1.0), Ordering::Greater);
        assert_eq!(nan_safe_cmp(1.0, 1.0), Ordering::Equal);
    }

    #[test]
    fn given_nan_operands_when_comparing_should_rank_nan_last() {
        assert_eq!(nan_safe_cmp(f64::NAN, 1.0), Ordering::Greater);
        assert_eq!(nan_safe_cmp(1.0, f64::NAN), Ordering::Less);
        assert_eq!(nan_safe_cmp(f64::NAN, f64::NAN), Ordering::Equal);
    }

    #[test]
    fn given_finite_value_when_calling_finite_or_should_return_value() {
        assert_eq!(finite_or(3.0, 0.0), 3.0);
    }

    #[test]
    fn given_nonfinite_value_when_calling_finite_or_should_return_fallback() {
        assert_eq!(finite_or(f64::NAN, 0.0), 0.0);
        assert_eq!(finite_or(f64::INFINITY, -1.0), -1.0);
        assert_eq!(finite_or(f64::NEG_INFINITY, 5.0), 5.0);
    }
}
