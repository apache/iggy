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
