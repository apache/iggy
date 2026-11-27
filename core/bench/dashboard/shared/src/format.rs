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

const DIGITS_PER_GROUP: usize = 3;
const DIGIT_GROUP_SEPARATOR: char = ',';

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

/// Format a count with a comma between groups of three digits, for example 40,000.
pub fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / DIGITS_PER_GROUP);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(DIGITS_PER_GROUP) {
            grouped.push(DIGIT_GROUP_SEPARATOR);
        }
        grouped.push(digit);
    }
    grouped
}

pub fn runs_text(count: usize) -> String {
    match count {
        1 => "1 run".to_owned(),
        _ => format!("{count} runs"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn given_counts_when_grouping_digits_should_separate_each_thousand() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(999), "999");
        assert_eq!(group_digits(1_000), "1,000");
        assert_eq!(group_digits(40_000), "40,000");
        assert_eq!(group_digits(1_234_567), "1,234,567");
    }

    #[test]
    fn given_counts_when_written_should_use_singular_for_one_run() {
        assert_eq!(runs_text(0), "0 runs");
        assert_eq!(runs_text(1), "1 run");
        assert_eq!(runs_text(317), "317 runs");
    }
}
