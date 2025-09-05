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

                if updated_summary.min_latency_ms == 0.0
                    && let Some(min_val) = min(&latency_ts)
                {
                    updated_summary.min_latency_ms = min_val;
                }

                if updated_summary.max_latency_ms == 0.0
                    && let Some(max_val) = max(&latency_ts)
                {
                    updated_summary.max_latency_ms = max_val;
                }

                if updated_summary.std_dev_latency_ms == 0.0
                    && let Some(std_dev_val) = std_dev(&latency_ts)
                {
                    updated_summary.std_dev_latency_ms = std_dev_val;
                }

                Ok(BenchmarkIndividualMetrics {
                    summary: updated_summary,
                    throughput_mb_ts,
                    throughput_msg_ts,
                    latency_ts,
                })
            }
        }

        // Use the visitor to deserialize
        deserializer.deserialize_map(BenchmarkIndividualMetricsVisitor)
    }
}
