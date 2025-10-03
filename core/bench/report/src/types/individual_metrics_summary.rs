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

use super::actor_kind::ActorKind;
use crate::benchmark_kind::BenchmarkKind;
use crate::utils::round_float;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, PartialEq, Deserialize)]
pub struct BenchmarkIndividualMetricsSummary {
    pub benchmark_kind: BenchmarkKind,
    pub actor_kind: ActorKind,
    pub actor_id: usize,
    #[serde(serialize_with = "round_float")]
    pub total_time_secs: f64,
    pub total_user_data_bytes: u64,
    pub total_bytes: u64,
    pub total_messages: u64,
    #[serde(default)]
    pub total_message_batches: usize,
    #[serde(serialize_with = "round_float")]
    pub throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub p50_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p90_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p95_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p99_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p9999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub avg_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub median_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub min_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub max_latency_ms: f64,
    #[serde(default, serialize_with = "round_float")]
    pub std_dev_latency_ms: f64,
}
