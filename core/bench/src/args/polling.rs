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

use std::fmt::{Display, Formatter, Write};

use bench_report::benchmark_kind::BenchmarkKind;
use clap::ValueEnum;
use iggy::prelude::PollingKind;
use serde::Serialize;

use super::common::IggyBenchArgs;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PollingMode {
    Offset,
    Next,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LatencyKind {
    Poll,
    OriginTimestamp,
}

impl Display for PollingMode {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Offset => "offset",
            Self::Next => "next",
        })
    }
}

impl Display for LatencyKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Poll => "poll",
            Self::OriginTimestamp => "origin-timestamp",
        })
    }
}

impl From<PollingMode> for PollingKind {
    fn from(value: PollingMode) -> Self {
        match value {
            PollingMode::Offset => Self::Offset,
            PollingMode::Next => Self::Next,
        }
    }
}

impl IggyBenchArgs {
    pub fn resolved_polling_kind(&self) -> PollingMode {
        self.polling_kind.unwrap_or_else(|| {
            if self.number_of_consumer_groups() > 0 {
                PollingMode::Next
            } else {
                PollingMode::Offset
            }
        })
    }

    pub fn resolved_latency_kind(&self) -> LatencyKind {
        self.latency_kind.unwrap_or_else(|| match self.kind() {
            BenchmarkKind::PinnedConsumer | BenchmarkKind::BalancedConsumerGroup => {
                LatencyKind::Poll
            }
            _ => LatencyKind::OriginTimestamp,
        })
    }

    pub fn poll_selector_suffix(&self) -> String {
        let mut suffix = String::new();
        if let Some(polling) = self.polling_kind {
            let _ = write!(suffix, "_polling-{polling}");
        }
        if let Some(latency) = self.latency_kind {
            let _ = write!(suffix, "_latency-{latency}");
        }
        suffix
    }

    pub fn validate_poll_selectors(&self) -> Result<(), &'static str> {
        if self.polling_kind.is_none() && self.latency_kind.is_none() {
            return Ok(());
        }
        if self.high_level_api {
            return Err(
                "--polling-kind and --latency-kind require the low-level API; omit --high-level-api",
            );
        }
        if matches!(
            self.kind(),
            BenchmarkKind::PinnedProducer | BenchmarkKind::BalancedProducer
        ) {
            return Err("--polling-kind and --latency-kind require a benchmark with consumers");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{LatencyKind, PollingMode};
    use crate::args::common::IggyBenchArgs;

    #[test]
    fn selectors_preserve_mode_defaults_and_accept_global_overrides() {
        for (mode, polling, latency) in [
            ("pinned-consumer", PollingMode::Offset, LatencyKind::Poll),
            (
                "balanced-consumer-group",
                PollingMode::Next,
                LatencyKind::Poll,
            ),
            (
                "pinned-producer-and-consumer",
                PollingMode::Offset,
                LatencyKind::OriginTimestamp,
            ),
            (
                "balanced-producer-and-consumer-group",
                PollingMode::Next,
                LatencyKind::OriginTimestamp,
            ),
            (
                "end-to-end-producing-consumer",
                PollingMode::Offset,
                LatencyKind::OriginTimestamp,
            ),
            (
                "end-to-end-producing-consumer-group",
                PollingMode::Next,
                LatencyKind::OriginTimestamp,
            ),
        ] {
            let args = IggyBenchArgs::try_parse_from(["iggy-bench", mode, "tcp"]).unwrap();
            assert_eq!(args.resolved_polling_kind(), polling);
            assert_eq!(args.resolved_latency_kind(), latency);
            let args = IggyBenchArgs::try_parse_from([
                "iggy-bench",
                mode,
                "tcp",
                "--polling-kind",
                "next",
                "--latency-kind",
                "poll",
            ])
            .unwrap();
            assert!(args.validate_poll_selectors().is_ok());
            assert_eq!(args.resolved_polling_kind(), PollingMode::Next);
            assert_eq!(args.resolved_latency_kind(), LatencyKind::Poll);
        }
    }

    #[test]
    fn unsupported_api_and_producer_selectors_are_rejected() {
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--high-level-api",
            "--polling-kind",
            "next",
            "pinned-consumer",
            "tcp",
        ])
        .unwrap();
        assert!(args.validate_poll_selectors().is_err());
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--latency-kind",
            "poll",
            "pinned-producer",
            "tcp",
        ])
        .unwrap();
        assert!(args.validate_poll_selectors().is_err());
    }

    #[test]
    fn poll_latency_override_preserves_read_amplification() {
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--latency-kind",
            "poll",
            "balanced-producer-and-consumer-group",
            "--read-amplification",
            "1.25",
            "tcp",
        ])
        .unwrap();
        assert_eq!(args.read_amplification(), Some(1.25));
    }
}
