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

//! API response types for the Connectors Runtime HTTP API.
//!
//! These types are used by the runtime to serialize responses and can be used
//! by clients/tests to deserialize API responses.

use iggy_common::IggyTimestamp;
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

/// Status of a connector.
#[derive(
    Debug,
    Serialize,
    Deserialize,
    PartialEq,
    Clone,
    Copy,
    AsRefStr,
    StrumDisplay,
    EnumString,
    Default,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[repr(u8)]
pub enum ConnectorStatus {
    /// Connector is initializing
    Starting,
    /// Connector is running normally
    Running,
    /// Connector is shutting down
    Stopping,
    /// Connector is stopped (disabled or shut down cleanly)
    #[default]
    Stopped,
    /// Connector has encountered an error
    Error,
}

/// Error information for a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorError {
    pub message: String,
    pub timestamp: IggyTimestamp,
}

impl ConnectorError {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            timestamp: IggyTimestamp::now(),
        }
    }
}

/// Runtime statistics response from `/stats` endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorRuntimeStats {
    /// The version of the connectors runtime.
    pub connectors_runtime_version: String,
    /// The semantic version of the connectors runtime in the numeric format,
    /// e.g. 1.2.3 -> 1002003 (major followed by zero-padded three-digit minor and patch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connectors_runtime_version_semver: Option<u32>,
    /// The unique identifier of the runtime process.
    pub process_id: u32,
    /// The CPU usage of the runtime process.
    pub cpu_usage: f32,
    /// The total CPU usage of the system, scoped to the cores this process may run on
    /// when confined by an affinity/cpuset mask.
    pub total_cpu_usage: f32,
    /// The memory usage of the runtime process in bytes.
    pub memory_usage: u64,
    /// The total memory of the system in bytes, or the effective cgroup memory limit when
    /// the runtime runs inside a memory-capped cgroup (container, systemd slice).
    pub total_memory: u64,
    /// The available memory of the system in bytes, scoped to the cgroup limit when one applies.
    pub available_memory: u64,
    /// The elapsed time since the runtime started, in microseconds.
    pub run_time: u64,
    /// The time the runtime started, in microseconds since the UNIX epoch.
    pub start_time: u64,
    /// The number of configured source connectors, including disabled and failed ones.
    pub sources_total: u32,
    /// The number of currently running source connectors.
    pub sources_running: u32,
    /// The number of configured sink connectors, including disabled and failed ones.
    pub sinks_total: u32,
    /// The number of currently running sink connectors.
    pub sinks_running: u32,
    /// Per-connector statistics for every configured source and sink.
    pub connectors: Vec<ConnectorStats>,
}

/// Statistics for a single connector.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorStats {
    pub key: String,
    pub name: String,
    pub connector_type: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_semver: Option<u32>,
    pub status: ConnectorStatus,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_produced: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_sent: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_consumed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_processed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages_filtered: Option<u64>,
    #[serde(default)]
    pub errors: u64,
}

/// Sink information response from `/sinks` endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct SinkInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ConnectorError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_config_format: Option<String>,
}

/// Source information response from `/sources` endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct SourceInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ConnectorError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plugin_config_format: Option<String>,
}

/// Health check response from `/health` endpoint.
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
}
