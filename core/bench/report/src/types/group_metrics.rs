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
    group_metrics_summary::BenchmarkGroupMetricsSummary, latency_distribution::LatencyDistribution,
    time_series::TimeSeries,
};
use crate::utils::{max, min, std_dev};
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct BenchmarkGroupMetrics {
    pub summary: BenchmarkGroupMetricsSummary,
    pub avg_throughput_mb_ts: TimeSeries,
    pub avg_throughput_msg_ts: TimeSeries,
    pub avg_latency_ts: TimeSeries,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_distribution: Option<LatencyDistribution>,
}

// Custom deserializer implementation
impl<'de> Deserialize<'de> for BenchmarkGroupMetrics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BenchmarkGroupMetricsVisitor;

        impl<'de> Visitor<'de> for BenchmarkGroupMetricsVisitor {
            type Value = BenchmarkGroupMetrics;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct BenchmarkGroupMetrics")
            }

            fn visit_map<V>(self, mut map: V) -> Result<BenchmarkGroupMetrics, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut summary: Option<BenchmarkGroupMetricsSummary> = None;
                let mut avg_throughput_mb_ts: Option<TimeSeries> = None;
                let mut avg_throughput_msg_ts: Option<TimeSeries> = None;
                let mut avg_latency_ts: Option<TimeSeries> = None;
                let mut latency_distribution: Option<LatencyDistribution> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "summary" => {
                            summary = Some(map.next_value()?);
                        }
                        "avg_throughput_mb_ts" => {
                            avg_throughput_mb_ts = Some(map.next_value()?);
                        }
                        "avg_throughput_msg_ts" => {
                            avg_throughput_msg_ts = Some(map.next_value()?);
                        }
                        "avg_latency_ts" => {
                            avg_latency_ts = Some(map.next_value()?);
                        }
                        "latency_distribution" => {
                            latency_distribution = map.next_value()?;
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let summary = summary.ok_or_else(|| de::Error::missing_field("summary"))?;
                let avg_throughput_mb_ts = avg_throughput_mb_ts
                    .ok_or_else(|| de::Error::missing_field("avg_throughput_mb_ts"))?;
                let avg_throughput_msg_ts = avg_throughput_msg_ts
                    .ok_or_else(|| de::Error::missing_field("avg_throughput_msg_ts"))?;
                let avg_latency_ts =
                    avg_latency_ts.ok_or_else(|| de::Error::missing_field("avg_latency_ts"))?;

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
                    updated_summary.min_latency_ms = min(&avg_latency_ts).unwrap_or(0.0);
                    updated_summary.max_latency_ms = max(&avg_latency_ts).unwrap_or(0.0);
                    updated_summary.std_dev_latency_ms = std_dev(&avg_latency_ts).unwrap_or(0.0);
                }

                Ok(BenchmarkGroupMetrics {
                    summary: updated_summary,
                    avg_throughput_mb_ts,
                    avg_throughput_msg_ts,
                    avg_latency_ts,
                    latency_distribution,
                })
            }
        }

        deserializer.deserialize_map(BenchmarkGroupMetricsVisitor)
    }
}
