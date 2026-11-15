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

use super::{BenchmarkCache, Result, Runs};
use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
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

        let mut reports = HashMap::with_capacity(entries.len());
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

            // Store the benchmark report
            reports.insert(light_report.uuid, (light_report, path));
        });

        let mut summaries: Vec<_> = reports
            .values()
            .map(|(report, _)| BenchmarkSummary::new(report, &self.thresholds))
            .collect();
        summaries.sort_by(|a, b| {
            b.version_order
                .timestamp_us
                .cmp(&a.version_order.timestamp_us)
                .then_with(|| a.uuid.cmp(&b.uuid))
        });
        info!("Built {} benchmark summaries", summaries.len());
        self.replace_runs(Runs {
            reports,
            summaries: Arc::new(summaries),
        });

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use serde_json::Value;

    const FIXTURE_RUN_DIR: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fixtures/results/live-0.9.0-pinned-producer-20p"
    );
    const SHIPPED_CONFIG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    const REPORT_FILE: &str = "report.json";
    /// `server_stats` of the fixture run, which differ from its `identifier` and `gitref`.
    const FIXTURE_SERVER_HOSTNAME: &str = "ip-10-0-0-40";
    const FIXTURE_SERVER_VERSION: &str = "0.9.0";

    #[test]
    fn given_report_without_gitref_and_identifier_when_loading_should_list_run_with_server_labels()
    {
        let fixture = std::fs::read_to_string(Path::new(FIXTURE_RUN_DIR).join(REPORT_FILE))
            .expect("the fixture report is readable");
        let mut report: Value = serde_json::from_str(&fixture).expect("the fixture report is JSON");
        report["params"]["gitref"] = Value::Null;
        report["hardware"]["identifier"] = Value::Null;
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = results_dir.path().join("run");
        std::fs::create_dir(&run_dir).expect("the run directory is created");
        std::fs::write(run_dir.join(REPORT_FILE), report.to_string())
            .expect("the report is written");
        let thresholds = config::load(Path::new(SHIPPED_CONFIG))
            .expect("the shipped config is valid")
            .thresholds;
        let cache = BenchmarkCache::new(results_dir.path().to_path_buf(), thresholds);

        cache.load().expect("the results directory is readable");

        let summaries = cache.get_summaries();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].machine, FIXTURE_SERVER_HOSTNAME);
        assert_eq!(summaries[0].version, FIXTURE_SERVER_VERSION);
        assert_eq!(summaries[0].gitref, None);
    }
}
