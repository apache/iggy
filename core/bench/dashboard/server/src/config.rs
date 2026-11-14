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

use crate::error::IggyBenchDashboardServerError;
use bench_dashboard_shared::config::DashboardConfig;
use std::path::Path;

/// Reads and validates the dashboard configuration file.
pub fn load(path: &Path) -> Result<DashboardConfig, IggyBenchDashboardServerError> {
    let text = std::fs::read_to_string(path).map_err(|source| {
        IggyBenchDashboardServerError::ConfigRead {
            path: path.to_path_buf(),
            source,
        }
    })?;
    let config: DashboardConfig =
        toml::from_str(&text).map_err(|source| IggyBenchDashboardServerError::ConfigParse {
            path: path.to_path_buf(),
            source,
        })?;
    config
        .validate()
        .map_err(|source| IggyBenchDashboardServerError::ConfigInvalid {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bench_dashboard_shared::config::ConfigError;
    use bench_dashboard_shared::delta::Metric;
    use std::path::PathBuf;
    use tempfile::TempDir;

    const SHIPPED_CONFIG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    const VALID_CONFIG: &str = r#"
        [[suites]]
        name = "i4i-single"
        title = "AWS i4i.4xlarge, 1 node"

        [[headlines]]
        suite = "i4i-single"
        workload = "producer-20p"
        title = "Write throughput"
        metric = "throughput"

        [thresholds]
        throughput_pct = 3.0
        p50_pct = 60.0
        p90_pct = 60.0
        p95_pct = 60.0
        p99_pct = 60.0
        p999_pct = 100.0
        max_time_spread_pct = 10.0
    "#;

    fn write_config(text: &str) -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("dashboard.toml");
        std::fs::write(&path, text).expect("write config");
        (dir, path)
    }

    #[test]
    fn given_shipped_config_when_loaded_should_hold_suites_headlines_and_thresholds() {
        let config = load(Path::new(SHIPPED_CONFIG)).expect("the shipped config is valid");
        let suites: Vec<_> = config
            .suites
            .iter()
            .map(|suite| suite.name.as_str())
            .collect();
        let headlines: Vec<_> = config
            .headlines
            .iter()
            .map(|headline| (headline.workload.as_str(), headline.metric))
            .collect();

        assert_eq!(suites, ["i4i-single", "i4i-cluster3", "i3en-cluster3"]);
        assert_eq!(
            headlines,
            [
                ("producer-20p", Metric::Throughput),
                ("producer-20p-persisted@800MB", Metric::P99),
                ("end-to-end-20@1600MB", Metric::P99),
                ("producer-20p@800MB", Metric::P99),
            ]
        );
        assert_eq!(config.thresholds.throughput_pct, 3.0);
        assert_eq!(config.thresholds.p999_pct, 450.0);
        assert_eq!(config.thresholds.max_time_spread_pct, 10.0);
    }

    #[test]
    fn given_headline_of_unknown_suite_when_loaded_should_fail_validation() {
        let (_dir, path) = write_config(
            &VALID_CONFIG.replace("suite = \"i4i-single\"", "suite = \"i4i-cluster3\""),
        );

        let result = load(&path);

        assert!(
            matches!(
                &result,
                Err(IggyBenchDashboardServerError::ConfigInvalid {
                    source: ConfigError::UnknownSuite { suite, .. },
                    ..
                }) if suite == "i4i-cluster3"
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn given_zero_threshold_when_loaded_should_fail_validation() {
        let (_dir, path) = write_config(&VALID_CONFIG.replace("p50_pct = 60.0", "p50_pct = 0.0"));

        let result = load(&path);

        assert!(
            matches!(
                &result,
                Err(IggyBenchDashboardServerError::ConfigInvalid {
                    source: ConfigError::InvalidThreshold {
                        name: "p50_pct",
                        ..
                    },
                    ..
                })
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn given_unknown_key_or_metric_when_loaded_should_fail_to_parse() {
        for text in [
            VALID_CONFIG.replace("p999_pct = 100.0", "p999_pct = 100.0\np9999_pct = 100.0"),
            VALID_CONFIG.replace("metric = \"throughput\"", "metric = \"max\""),
        ] {
            let (_dir, path) = write_config(&text);

            let result = load(&path);

            assert!(
                matches!(
                    result,
                    Err(IggyBenchDashboardServerError::ConfigParse { .. })
                ),
                "got {result:?}"
            );
        }
    }

    #[test]
    fn given_missing_file_when_loaded_should_fail_to_read() {
        let dir = tempfile::tempdir().expect("create temp dir");

        let result = load(&dir.path().join("dashboard.toml"));

        assert!(
            matches!(
                result,
                Err(IggyBenchDashboardServerError::ConfigRead { .. })
            ),
            "got {result:?}"
        );
    }
}
