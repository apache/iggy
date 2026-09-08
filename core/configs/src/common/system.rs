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
use iggy_common::IggyByteSize;
use iggy_common::IggyDuration;
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use server_common::log::LoggingSettings;

pub const INDEX_EXTENSION: &str = "index";
pub const LOG_EXTENSION: &str = "log";

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct RuntimeConfig {
    pub path: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct LoggingConfig {
    pub path: String,
    pub level: String,
    pub file_enabled: bool,
    #[config_env(leaf)]
    pub max_file_size: IggyByteSize,
    #[config_env(leaf)]
    pub max_total_size: IggyByteSize,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub rotation_check_interval: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub retention: IggyDuration,
}

impl From<&LoggingConfig> for LoggingSettings {
    fn from(config: &LoggingConfig) -> Self {
        Self {
            path: config.path.clone(),
            level: config.level.clone(),
            file_enabled: config.file_enabled,
            max_file_size: config.max_file_size,
            max_total_size: config.max_total_size,
            rotation_check_interval: config.rotation_check_interval,
            retention: config.retention,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct EncryptionConfig {
    pub enabled: bool,
    // skip_serializing keeps the key out of the runtime current_config.toml (and
    // the diagnostic snapshot that cats it). The live key is read from env /
    // on-disk config at boot, never from the snapshot.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encryption_key_is_never_serialized() {
        // current_config.toml (and the diagnostic snapshot that cats it) is
        // produced by serializing this struct, so the key must not survive a
        // serialize. skip_serializing is format-agnostic, so a JSON dump proves
        // the toml path too.
        let config = EncryptionConfig {
            enabled: true,
            key: "encryption-key-MUST-NOT-be-persisted".to_owned(),
        };
        let serialized = serde_json::to_string(&config).expect("serialize encryption config");
        assert!(
            !serialized.contains("MUST-NOT-be-persisted"),
            "encryption key leaked into serialized config: {serialized}"
        );
    }
}
