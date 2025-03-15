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

use super::group_metrics_kind::GroupMetricsKind;
use crate::utils::round_float;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Clone, PartialEq, Deserialize)]
pub struct BenchmarkGroupMetricsSummary {
    pub kind: GroupMetricsKind,
    #[serde(serialize_with = "round_float")]
    pub total_throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub total_throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p50_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p90_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p95_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p99_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_p9999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub average_median_latency_ms: f64,
}
