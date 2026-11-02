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

//! Plain input structs for [`crate::log::logger::Logging`].
//!
//! The `configs` crate depends on `server_common`, so the logger cannot
//! take the `ConfigEnv`-derived config structs directly; instead it takes
//! these mirrors of the fields it consumes, and `configs` provides `From`
//! conversions from `LoggingConfig` / `TelemetryConfig`.

use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{LevelFilter, ParseError};

use iggy_common::{IggyByteSize, IggyDuration};

#[derive(Debug, Clone)]
pub struct LoggingSettings {
    pub path: String,
    pub level: String,
    pub file_enabled: bool,
    pub max_file_size: IggyByteSize,
    pub max_total_size: IggyByteSize,
    pub rotation_check_interval: IggyDuration,
    pub retention: IggyDuration,
}

/// `logging.level` in the `RUST_LOG` syntax, parsed more strictly than
/// `EnvFilter::new` does. `EnvFilter` reads a bare word that is not a level
/// as a target name and enables only that target, so a typo such as `inof`
/// turned off every log line, errors included, without a warning.
#[derive(Debug)]
pub struct LogFilter(EnvFilter);

#[derive(Debug, thiserror::Error)]
pub enum LogFilterError {
    #[error("no filter directive")]
    Empty,

    #[error(
        "`{0}` is not a log level (trace, debug, info, warn, error or off); write `{0}=<level>` to filter one target"
    )]
    NotALevel(String),

    #[error(transparent)]
    InvalidDirective(#[from] ParseError),
}

/// The 0.8.0 and 0.9.0 `config.toml` listed `none` as a level, but `tracing`
/// has no such level.
const NONE_LEVEL_ALIAS: &str = "none";
const OFF_LEVEL: &str = "off";

impl FromStr for LogFilter {
    type Err = LogFilterError;

    fn from_str(level: &str) -> Result<Self, Self::Err> {
        // `EnvFilter` mis-parses a directive with spaces around it.
        let directives: Vec<&str> = level
            .split(',')
            .map(str::trim)
            .filter(|directive| !directive.is_empty())
            .map(|directive| {
                if directive.eq_ignore_ascii_case(NONE_LEVEL_ALIAS) {
                    OFF_LEVEL
                } else {
                    directive
                }
            })
            .collect();
        if directives.is_empty() {
            return Err(LogFilterError::Empty);
        }
        if let Some(word) = directives.iter().copied().find(|directive| {
            !directive.contains(['=', '[']) && directive.parse::<LevelFilter>().is_err()
        }) {
            return Err(LogFilterError::NotALevel(word.to_owned()));
        }
        Ok(Self(EnvFilter::builder().parse(directives.join(","))?))
    }
}

impl From<LogFilter> for EnvFilter {
    fn from(filter: LogFilter) -> Self {
        filter.0
    }
}

#[derive(Debug, Clone)]
pub struct TelemetrySettings {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryEndpointSettings,
    pub traces: TelemetryEndpointSettings,
}

#[derive(Debug, Clone)]
pub struct TelemetryEndpointSettings {
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryTransport {
    #[display("grpc")]
    GRPC,
    #[display("http")]
    HTTP,
}

impl FromStr for TelemetryTransport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "grpc" => Ok(TelemetryTransport::GRPC),
            "http" => Ok(TelemetryTransport::HTTP),
            _ => Err(format!("Invalid telemetry transport: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_levels_and_directives_when_parsing_should_accept() {
        for level in [
            "off",
            "INFO",
            "warn,server=debug,iggy=trace",
            "info,[request]=debug",
        ] {
            assert!(
                level.parse::<LogFilter>().is_ok(),
                "rejected valid filter {level:?}"
            );
        }
    }

    #[test]
    fn given_word_that_is_not_a_level_when_parsing_should_reject() {
        for level in ["inof", "server", "warn,inof", "warn, inof"] {
            assert!(
                matches!(
                    level.parse::<LogFilter>(),
                    Err(LogFilterError::NotALevel(_))
                ),
                "accepted {level:?}"
            );
        }
    }

    #[test]
    fn given_none_when_parsing_should_mean_off() {
        let none: EnvFilter = "none".parse::<LogFilter>().unwrap().into();
        let off: EnvFilter = "off".parse::<LogFilter>().unwrap().into();
        assert_eq!(none.to_string(), off.to_string());
    }

    #[test]
    fn given_spaces_around_directives_when_parsing_should_keep_every_directive() {
        let spaced: EnvFilter = " warn , server=debug ".parse::<LogFilter>().unwrap().into();
        let compact: EnvFilter = "warn,server=debug".parse::<LogFilter>().unwrap().into();
        assert_eq!(spaced.to_string(), compact.to_string());
    }

    #[test]
    fn given_malformed_or_empty_filter_when_parsing_should_reject() {
        assert!(matches!(
            "server=verbose".parse::<LogFilter>(),
            Err(LogFilterError::InvalidDirective(_))
        ));
        for level in ["", ","] {
            assert!(
                matches!(level.parse::<LogFilter>(), Err(LogFilterError::Empty)),
                "accepted {level:?}"
            );
        }
    }
}
