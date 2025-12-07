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
use std::path::Path;
use tokio::io::AsyncReadExt;
use tracing::{error, info};

impl BenchmarkCache {
    pub async fn load(&self) -> Result<()> {
        info!(
            "Building benchmark cache from directory {}",
            self.results_dir.display()
        );

        self.load_gh_workflows().await;

        let entries: Vec<_> = std::fs::read_dir(&self.results_dir)
            .map_err(IggyBenchDashboardServerError::Io)?
            .filter_map(|r: std::result::Result<std::fs::DirEntry, std::io::Error>| r.ok())
            .filter(|entry| entry.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .collect();

        let mut total_removed_size = 0;

        entries.iter().for_each(|entry| {
            // Remove HTML files from the directory
            match self.remove_html_files(&entry.path()) {
                Ok(size) => {
                    total_removed_size += size;
                }
                Err(e) => {
                    error!(
                        "Failed to remove HTML files from {}: {}",
                        entry.path().display(),
                        e
                    );
                }
            }

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

            let identifier = if let Some(identifier) = &light_report.hardware.identifier {
                identifier
            } else {
                error!(
                    "No identifier found in benchmark report: {:#?}",
                    &entry.path()
                );
                return;
            };

            let gitref = if let Some(gitref) = &light_report.params.gitref {
                gitref
            } else {
                error!("No gitref found in benchmark report: {:#?}", &entry.path());
                return;
            };

            // Update hardware to gitref mapping
            self.hardware_to_gitref
                .entry(identifier.clone())
                .or_default()
                .insert(gitref.clone());

            // Update gitref to benchmarks mapping
            self.gitref_to_benchmarks
                .entry(gitref.clone())
                .or_default()
                .insert(light_report.uuid);

            // Store the benchmark report
            self.benchmarks
                .insert(light_report.uuid, (light_report, path));
        });

        info!(
            "Remove HTML files of size: {:.2} MB",
            total_removed_size as f64 / 1_048_576.0
        );

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

    async fn load_gh_workflows(&self) {
        let mut data = String::new();
        let read = self
            .gh_workflows_file
            .lock()
            .await
            .read_to_string(&mut data)
            .await;

        match read {
            Ok(0) => {
                info!("No GH workflows file found")
            }
            Ok(1_usize..) => {
                info!("Loaded GH workflows file");
                data.lines().for_each(|line| {
                    info!("Adding GH workflow: {}", line);
                    self.gh_workflows.insert(line.parse().unwrap());
                });
            }
            Err(e) => {
                info!("Error reading GH workflows file: {e}");
            }
        }
    }

    pub fn remove_html_files(&self, entry_path: &Path) -> std::io::Result<u64> {
        let html_files: Vec<_> = std::fs::read_dir(entry_path)?
            .filter_map(|r| r.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "html")
                    .unwrap_or(false)
            })
            .collect();

        let mut total_size = 0;

        for html_file in html_files {
            let metadata = match html_file.metadata() {
                Ok(meta) => meta,
                Err(e) => {
                    error!(
                        "Failed to get metadata for file {}: {}",
                        html_file.path().display(),
                        e
                    );
                    continue;
                }
            };

            let file_size = metadata.len();
            if let Err(e) = std::fs::remove_file(html_file.path()) {
                error!(
                    "Failed to remove HTML file {} (size: {} bytes): {}",
                    html_file.path().display(),
                    file_size,
                    e
                );
            } else {
                total_size += file_size;
            }
        }

        Ok(total_size)
    }
}
