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

//! The dashboard configuration file: the official suites, the Overview's headline workloads, and
//! the thresholds for the delta rule and the badges.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::delta::Metric;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardConfig {
    /// A run is official if its `suite` is in this list.
    pub suites: Vec<SuiteConfig>,
    pub headlines: Vec<HeadlineConfig>,
    pub thresholds: Thresholds,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteConfig {
    /// The `suite` label of the runs.
    pub name: String,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadlineConfig {
    pub suite: String,
    /// The `workload` label of the runs.
    pub workload: String,
    pub title: String,
    pub metric: Metric,
}

/// Thresholds in percent. A delta threshold applies to the ratio of the larger value to the
/// smaller one, so 60 colors a change above 1.6x in either direction.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thresholds {
    pub throughput_pct: f64,
    pub p50_pct: f64,
    pub p90_pct: f64,
    pub p95_pct: f64,
    pub p99_pct: f64,
    pub p999_pct: f64,
    /// Above this time spread, a rate-limited run gets the badge "uneven load".
    pub max_time_spread_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConfigError {
    #[error("the headline \"{title}\" names the suite \"{suite}\", which isn't in [[suites]]")]
    UnknownSuite { title: String, suite: String },
    #[error("thresholds.{name} is {value}, but it must be a finite number above 0")]
    InvalidThreshold { name: &'static str, value: f64 },
}

impl DashboardConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(headline) = self
            .headlines
            .iter()
            .find(|headline| !self.is_official(Some(&headline.suite)))
        {
            return Err(ConfigError::UnknownSuite {
                title: headline.title.clone(),
                suite: headline.suite.clone(),
            });
        }
        self.thresholds.validate()
    }

    pub fn is_official(&self, suite: Option<&str>) -> bool {
        suite.is_some_and(|suite| self.suites.iter().any(|known| known.name == suite))
    }
}

impl Thresholds {
    fn validate(&self) -> Result<(), ConfigError> {
        let named = [
            ("throughput_pct", self.throughput_pct),
            ("p50_pct", self.p50_pct),
            ("p90_pct", self.p90_pct),
            ("p95_pct", self.p95_pct),
            ("p99_pct", self.p99_pct),
            ("p999_pct", self.p999_pct),
            ("max_time_spread_pct", self.max_time_spread_pct),
        ];
        match named
            .into_iter()
            .find(|(_, value)| !(value.is_finite() && *value > 0.0))
        {
            Some((name, value)) => Err(ConfigError::InvalidThreshold { name, value }),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::THRESHOLDS;

    const SUITE: &str = "i4i-single";

    fn config() -> DashboardConfig {
        DashboardConfig {
            suites: vec![SuiteConfig {
                name: SUITE.to_owned(),
                title: "AWS i4i.4xlarge, 1 node".to_owned(),
            }],
            headlines: vec![HeadlineConfig {
                suite: SUITE.to_owned(),
                workload: "producer-20p".to_owned(),
                title: "Write throughput".to_owned(),
                metric: Metric::Throughput,
            }],
            thresholds: THRESHOLDS,
        }
    }

    #[test]
    fn given_valid_config_when_validated_should_pass_and_mark_its_suites_official() {
        let config = config();

        assert_eq!(config.validate(), Ok(()));
        assert!(config.is_official(Some(SUITE)));
        assert!(!config.is_official(Some("atlas-single")));
        assert!(!config.is_official(None));
    }

    #[test]
    fn given_headline_of_unknown_suite_when_validated_should_name_it() {
        let mut config = config();
        config.headlines[0].suite = "i3en-single".to_owned();

        assert_eq!(
            config.validate(),
            Err(ConfigError::UnknownSuite {
                title: "Write throughput".to_owned(),
                suite: "i3en-single".to_owned(),
            })
        );
    }

    #[test]
    fn given_threshold_not_above_zero_when_validated_should_name_it() {
        for value in [0.0, -3.0, f64::INFINITY] {
            let mut config = config();
            config.thresholds.p99_pct = value;

            assert_eq!(
                config.validate(),
                Err(ConfigError::InvalidThreshold {
                    name: "p99_pct",
                    value
                })
            );
        }
        let mut config = config();
        config.thresholds.max_time_spread_pct = f64::NAN;

        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidThreshold {
                name: "max_time_spread_pct",
                ..
            })
        ));
    }

    #[test]
    fn given_unknown_field_when_parsed_should_fail() {
        let mut json = serde_json::to_value(config()).expect("the config serializes");
        json["thresholds"]["p9999_pct"] = 100.0.into();

        assert!(serde_json::from_value::<DashboardConfig>(json).is_err());
    }
}
