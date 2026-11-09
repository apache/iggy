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

use super::{BenchmarkCache, Result};
use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::path::Path;
use std::sync::{Arc, PoisonError};
use tracing::{error, info};

impl BenchmarkCache {
    pub fn load(&self) -> Result<()> {
        info!(
            "Building benchmark cache from directory {}",
            self.results_dir.display()
        );

        let entries: Vec<_> = std::fs::read_dir(&self.results_dir)
            .map_err(IggyBenchDashboardServerError::Io)?
            .filter_map(|r: std::result::Result<std::fs::DirEntry, std::io::Error>| r.ok())
            .filter(|entry| entry.file_type().map(|t| t.is_dir()).unwrap_or(false))
            // Servers from before --cache-dir wrote their PNG cache here. It isn't a benchmark run.
            .filter(|entry| entry.file_name() != "embed_cache")
            .collect();

        entries.iter().for_each(|entry| {
            // Relative path to report.json, for example `./performance_results/poll_8_1000_100_10000_tcp_no_cache_e1393367_atlas/report.json`
            let path = entry.path().join("report.json");

            let light_report = match self.load_light_report(&path) {
                Ok(report) => report,
                Err(e) => {
                    error!(
                        "Failed to load light report for {}: {}",
                        entry.path().display(),
                        e
                    );
                    return;
                }
            };

            info!("Loaded light benchmark report for {:?}", &entry.path());

            if light_report.hardware.identifier.is_none() {
                error!(
                    "No identifier found in benchmark report: {:#?}",
                    &entry.path()
                );
                return;
            }

            if light_report.params.gitref.is_none() {
                error!("No gitref found in benchmark report: {:#?}", &entry.path());
                return;
            }

            // Store the benchmark report
            self.benchmarks
                .insert(light_report.uuid, (light_report, path));
        });

        let mut summaries: Vec<_> = self
            .benchmarks
            .iter()
            .map(|entry| BenchmarkSummary::new(&entry.value().0, &self.thresholds))
            .collect();
        summaries.sort_by(|a, b| {
            b.version_order
                .timestamp_us
                .cmp(&a.version_order.timestamp_us)
                .then_with(|| a.uuid.cmp(&b.uuid))
        });
        info!("Built {} benchmark summaries", summaries.len());
        // The writer only swaps an Arc, so a poisoned lock still holds a whole list.
        *self
            .summaries
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::new(summaries);

        Ok(())
    }

    pub fn load_light_report(&self, path: &Path) -> Result<BenchmarkReportLight> {
        let data = std::fs::read_to_string(path).map_err(|e| {
            error!("Failed to read benchmark file {:?}: {}", path, e);
            IggyBenchDashboardServerError::Io(e)
        })?;

        serde_json::from_str(&data).map_err(|e| {
            error!(
                "Failed to parse JSON from {:?}: {}. Content: {}",
                path,
                e,
                if data.len() > 200 {
                    format!("{}... (truncated)", &data[..200])
                } else {
                    data
                }
            );
            IggyBenchDashboardServerError::InvalidJson(e.to_string())
        })
    }
}
