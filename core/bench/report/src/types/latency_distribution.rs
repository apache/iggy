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

use crate::utils::round_float;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LatencyDistribution {
    pub bins: Vec<HistogramBin>,
    pub log_normal_params: LogNormalParams,
    pub percentiles: DistributionPercentiles,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistogramBin {
    #[serde(serialize_with = "round_float")]
    pub edge_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub density: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogNormalParams {
    #[serde(serialize_with = "round_float")]
    pub mu: f64,
    #[serde(serialize_with = "round_float")]
    pub sigma: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributionPercentiles {
    #[serde(serialize_with = "round_float")]
    pub p05_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p50_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p95_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p99_ms: f64,
}
