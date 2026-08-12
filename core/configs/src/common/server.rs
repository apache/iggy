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

use configs::ConfigEnv;
use iggy_common::{IggyByteSize, IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use server_common::MemoryPoolConfigOther;
use server_common::log::{TelemetryEndpointSettings, TelemetrySettings};

pub use server_common::log::TelemetryTransport;

/// Configuration for the memory pool.
#[derive(Debug, Deserialize, Serialize, ConfigEnv)]
pub struct MemoryPoolConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    pub size: IggyByteSize,
    pub bucket_capacity: u32,
}

impl MemoryPoolConfig {
    pub fn into_other(&self) -> MemoryPoolConfigOther {
        MemoryPoolConfigOther {
            enabled: self.enabled,
            size: self.size,
            bucket_capacity: self.bucket_capacity,
        }
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct DataMaintenanceConfig {
    pub messages: MessagesMaintenanceConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessagesMaintenanceConfig {
    pub cleaner_enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub enforce_fsync: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PersonalAccessTokenConfig {
    pub max_tokens_per_user: u32,
    pub cleaner: PersonalAccessTokenCleanerConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PersonalAccessTokenCleanerConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct HeartbeatConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ConsumerGroupConfig {
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub rebalancing_timeout: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub rebalancing_check_interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryLogsConfig,
    pub traces: TelemetryTracesConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryLogsConfig {
    #[config_env(leaf)]
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryTracesConfig {
    #[config_env(leaf)]
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

impl From<&TelemetryConfig> for TelemetrySettings {
    fn from(config: &TelemetryConfig) -> Self {
        Self {
            enabled: config.enabled,
            service_name: config.service_name.clone(),
            logs: TelemetryEndpointSettings {
                transport: config.logs.transport,
                endpoint: config.logs.endpoint.clone(),
            },
            traces: TelemetryEndpointSettings {
                transport: config.traces.transport,
                endpoint: config.traces.endpoint.clone(),
            },
        }
    }
}
