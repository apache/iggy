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

use super::server_stats::BenchmarkServerStats;
use crate::group_metrics::BenchmarkGroupMetrics;
use crate::individual_metrics::BenchmarkIndividualMetrics;
use crate::types::hardware::BenchmarkHardware;
use crate::types::params::BenchmarkParams;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct BenchmarkReport {
    /// Benchmark unique identifier
    pub uuid: Uuid,

    /// Timestamp when the benchmark was finished
    pub timestamp: String,

    /// Benchmark server statistics
    pub server_stats: BenchmarkServerStats,

    /// Benchmark hardware
    pub hardware: BenchmarkHardware,

    /// Benchmark parameters
    pub params: BenchmarkParams,

    /// Benchmark metrics for all actors of same type (all producers, all consumers or all actors)
    pub group_metrics: Vec<BenchmarkGroupMetrics>,

    /// Benchmark metrics per actor (producer/consumer)
    pub individual_metrics: Vec<BenchmarkIndividualMetrics>,
}

impl BenchmarkReport {
    pub fn dump_to_json(&self, output_dir: &str) {
        // Create the output directory
        std::fs::create_dir_all(output_dir).expect("Failed to create output directory");

        let report_path = Path::new(output_dir).join("report.json");
        let report_json = serde_json::to_string(self).unwrap();
        std::fs::write(report_path, report_json).expect("Failed to write report to file");
    }
}
