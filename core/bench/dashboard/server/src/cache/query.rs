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

use super::BenchmarkCache;
use bench_dashboard_shared::BenchmarkReportLight;
use bench_report::hardware::BenchmarkHardware;
use chrono::{self, DateTime, FixedOffset};
use std::collections::HashMap;

impl BenchmarkCache {
    pub fn get_hardware_configurations(&self) -> Vec<BenchmarkHardware> {
        let mut hardware_map = HashMap::new();

        for entry in self.benchmarks.iter() {
            let (report, _) = entry.value();
            if let Some(identifier) = &report.hardware.identifier {
                hardware_map.insert(identifier.clone(), report.hardware.clone());
            }
        }

        hardware_map.into_values().collect()
    }

    pub fn get_gitrefs_for_hardware(&self, hardware: &str) -> Vec<String> {
        let gitref_set = self
            .hardware_to_gitref
            .get(hardware)
            .map(|set| set.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .unwrap_or_default();

        let mut sorted_gitrefs = gitref_set;
        sorted_gitrefs.sort();

        sorted_gitrefs
    }

    pub fn get_benchmarks_for_hardware_and_gitref(
        &self,
        hardware: &str,
        gitref: &str,
    ) -> Vec<BenchmarkReportLight> {
        let mut result = Vec::new();

        if let Some(benchmark_set) = self.gitref_to_benchmarks.get(gitref) {
            for uuid in benchmark_set.iter() {
                if let Some(entry) = self.benchmarks.get(&uuid) {
                    let (report, _) = entry.value();

                    // Check if this benchmark matches our hardware
                    if let Some(identifier) = &report.hardware.identifier {
                        if identifier != hardware {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    result.push(report.clone());
                }
            }
        }

        // Sort benchmarks by pretty_name
        result.sort_by(|a, b| a.params.pretty_name.cmp(&b.params.pretty_name));

        result
    }

    pub fn get_benchmark_trend_data(
        &self,
        params_identifier: &str,
        hardware: &str,
    ) -> Option<Vec<BenchmarkReportLight>> {
        let mut matching_reports = Vec::new();

        for entry in self.benchmarks.iter() {
            let (report, _) = entry.value();

            if let Some(identifier) = &report.hardware.identifier {
                if identifier != hardware {
                    continue;
                }
            } else {
                continue;
            }

            if report.params.params_identifier == params_identifier {
                matching_reports.push(report.clone());
            }
        }

        if matching_reports.is_empty() {
            return None;
        }

        matching_reports.sort_by_key(|report| {
            let date_str = report
                .params
                .gitref_date
                .as_deref()
                .unwrap_or("1970-01-01T00:00:00Z");
            Self::parse_date(date_str)
        });

        Some(matching_reports)
    }

    // Helper function to parse dates with a fallback
    fn parse_date(date_str: &str) -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339(date_str)
            .unwrap_or_else(|_| DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap())
    }

    /// Get the most recently added benchmarks, sorted by creation timestamp (newest first)
    pub fn get_recent_benchmarks(&self, limit: usize) -> Vec<BenchmarkReportLight> {
        let mut recent_benchmarks: Vec<BenchmarkReportLight> = self
            .benchmarks
            .iter()
            .map(|entry| entry.value().0.clone())
            .collect();

        recent_benchmarks.sort_by(|a, b| {
            let parse_a = DateTime::parse_from_rfc3339(&a.timestamp);
            let parse_b = DateTime::parse_from_rfc3339(&b.timestamp);
            match (parse_a, parse_b) {
                (Ok(time_a), Ok(time_b)) => time_b.cmp(&time_a),
                _ => std::cmp::Ordering::Equal,
            }
        });

        if recent_benchmarks.len() > limit {
            recent_benchmarks.truncate(limit);
        }

        recent_benchmarks
    }
}
