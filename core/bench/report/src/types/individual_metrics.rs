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

use super::{
    individual_metrics_summary::BenchmarkIndividualMetricsSummary, time_series::TimeSeries,
};
use crate::utils::{max, min, std_dev};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct BenchmarkIndividualMetrics {
    pub summary: BenchmarkIndividualMetricsSummary,
    pub throughput_mb_ts: TimeSeries,
    pub throughput_msg_ts: TimeSeries,
    pub latency_ts: TimeSeries,
    /// Per-batch latencies in ms, sorted ascending. Only populated in-memory
    /// during report building for distribution computation; never serialized.
    #[serde(skip)]
    pub raw_latencies_ms: Vec<f64>,
}

// Custom deserializer implementation
impl<'de> Deserialize<'de> for BenchmarkIndividualMetrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BenchmarkIndividualMetricsVisitor;

        impl<'de> Visitor<'de> for BenchmarkIndividualMetricsVisitor {
            type Value = BenchmarkIndividualMetrics;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct BenchmarkIndividualMetrics")
            }

            fn visit_map<V>(self, mut map: V) -> Result<BenchmarkIndividualMetrics, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut summary: Option<BenchmarkIndividualMetricsSummary> = None;
                let mut throughput_mb_ts: Option<TimeSeries> = None;
                let mut throughput_msg_ts: Option<TimeSeries> = None;
                let mut latency_ts: Option<TimeSeries> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "summary" => {
                            summary = Some(map.next_value()?);
                        }
                        "throughput_mb_ts" => {
                            throughput_mb_ts = Some(map.next_value()?);
                        }
                        "throughput_msg_ts" => {
                            throughput_msg_ts = Some(map.next_value()?);
                        }
                        "latency_ts" => {
                            latency_ts = Some(map.next_value()?);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let summary = summary.ok_or_else(|| de::Error::missing_field("summary"))?;
                let throughput_mb_ts =
                    throughput_mb_ts.ok_or_else(|| de::Error::missing_field("throughput_mb_ts"))?;
                let throughput_msg_ts = throughput_msg_ts
                    .ok_or_else(|| de::Error::missing_field("throughput_msg_ts"))?;
                let latency_ts =
                    latency_ts.ok_or_else(|| de::Error::missing_field("latency_ts"))?;

                let mut updated_summary = summary.clone();

                // Backfill for reports written before the three fields
                // existed: `serde(default)` lands them at zero together, so
                // all three being zero is what identifies such a report.
                // Keyed on all three rather than on each alone -- a run whose
                // samples are all equal reports a real zero std dev beside a
                // nonzero min and max, and refilling that from the per-bucket
                // moving average would put back the averaged-away extremes
                // the summary is computed from raw samples to avoid.
                if updated_summary.min_latency_ms == 0.0
                    && updated_summary.max_latency_ms == 0.0
                    && updated_summary.std_dev_latency_ms == 0.0
                {
                    updated_summary.min_latency_ms = min(&latency_ts).unwrap_or(0.0);
                    updated_summary.max_latency_ms = max(&latency_ts).unwrap_or(0.0);
                    updated_summary.std_dev_latency_ms = std_dev(&latency_ts).unwrap_or(0.0);
                }

                Ok(BenchmarkIndividualMetrics {
                    summary: updated_summary,
                    throughput_mb_ts,
                    throughput_msg_ts,
                    latency_ts,
                    raw_latencies_ms: Vec::new(),
                })
            }
        }

        // Use the visitor to deserialize
        deserializer.deserialize_map(BenchmarkIndividualMetricsVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Report JSON carrying `summary` and the three time series, with the
    /// summary's latency extremes spliced in as `extremes`.
    fn report_json(extremes: &str) -> String {
        format!(
            r#"{{
                "summary": {{
                    "benchmark_kind": "pinned_producer",
                    "actor_kind": "producer",
                    "actor_id": 0,
                    "total_time_secs": 1.0,
                    "total_user_data_bytes": 1000,
                    "total_bytes": 1000,
                    "total_messages": 3,
                    "total_message_batches": 3,
                    "throughput_megabytes_per_second": 1.0,
                    "throughput_messages_per_second": 3.0,
                    "p50_latency_ms": 5.0,
                    "p90_latency_ms": 5.0,
                    "p95_latency_ms": 5.0,
                    "p99_latency_ms": 5.0,
                    "p999_latency_ms": 5.0,
                    "p9999_latency_ms": 5.0,
                    "avg_latency_ms": 5.0,
                    "median_latency_ms": 5.0{extremes}
                }},
                "throughput_mb_ts": {{ "points": [] }},
                "throughput_msg_ts": {{ "points": [] }},
                "latency_ts": {{ "points": [
                    {{ "time_s": 0.0, "value": 4.0 }},
                    {{ "time_s": 1.0, "value": 6.0 }}
                ] }}
            }}"#
        )
    }

    #[test]
    fn given_a_report_without_latency_extremes_when_deserialized_should_backfill_from_the_series() {
        let metrics: BenchmarkIndividualMetrics =
            serde_json::from_str(&report_json("")).expect("deserialize legacy report");

        assert!((metrics.summary.min_latency_ms - 4.0).abs() < f64::EPSILON);
        assert!((metrics.summary.max_latency_ms - 6.0).abs() < f64::EPSILON);
        assert!(metrics.summary.std_dev_latency_ms > 0.0);
    }

    /// A run whose samples are all equal reports a real zero std dev beside a
    /// nonzero min and max. Backfilling it from the per-bucket moving average
    /// would put back the averaged-away extremes the summary avoids.
    #[test]
    fn given_a_report_with_a_real_zero_std_dev_when_deserialized_should_keep_it() {
        let json = report_json(
            r#","min_latency_ms": 5.0, "max_latency_ms": 5.0, "std_dev_latency_ms": 0.0"#,
        );

        let metrics: BenchmarkIndividualMetrics =
            serde_json::from_str(&json).expect("deserialize report");

        assert!((metrics.summary.min_latency_ms - 5.0).abs() < f64::EPSILON);
        assert!((metrics.summary.max_latency_ms - 5.0).abs() < f64::EPSILON);
        assert_eq!(metrics.summary.std_dev_latency_ms, 0.0);
    }
}
