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

use super::defaults::default_sysinfo_print_interval;
use configs::ConfigEnv;
use iggy_common::IggyByteSize;
use iggy_common::IggyDuration;
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::formats::PreferMany;
use serde_with::{OneOrMany, serde_as};
use server_common::log::LoggingSettings;

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
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_sysinfo_print_interval")]
    pub sysinfo_print_interval: IggyDuration,
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

#[serde_as]
#[derive(Deserialize, Serialize, Clone, ConfigEnv)]
pub struct EncryptionConfig {
    pub enabled: bool,
    // skip_serializing keeps the key out of the runtime current_config.toml (and
    // the diagnostic snapshot that cats it). The live key is read from env /
    // on-disk config at boot, never from the snapshot.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub key: String,
    /// Retired keys a node still unwraps stream keys with during a rotation.
    // A bare string is one key, so a type error can never echo the key into the log.
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub previous_keys: Vec<String>,
    /// Fingerprints of retired keys whose stream key wraps the primary drops.
    #[serde_as(as = "OneOrMany<_, PreferMany>")]
    #[serde(default)]
    pub retired_kek_fingerprints: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    #[test]
    fn encryption_key_is_never_serialized() {
        // current_config.toml (and the diagnostic snapshot that cats it) is
        // produced by serializing this struct, so the key must not survive a
        // serialize. skip_serializing is format-agnostic, so a JSON dump proves
        // the toml path too.
        let config = EncryptionConfig {
            enabled: true,
            key: "encryption-key-MUST-NOT-be-persisted".to_owned(),
            previous_keys: vec!["previous-key-MUST-NOT-be-persisted".to_owned()],
            retired_kek_fingerprints: Vec::new(),
        };
        let serialized = serde_json::to_string(&config).expect("serialize encryption config");
        assert!(
            !serialized.contains("MUST-NOT-be-persisted"),
            "encryption key leaked into serialized config: {serialized}"
        );
        let debugged = format!("{config:?}");
        assert!(
            !debugged.contains("MUST-NOT-be-persisted"),
            "encryption key leaked into debug output: {debugged}"
        );
    }

    #[test]
    fn given_bare_previous_key_string_when_extracting_should_read_one_key_without_echoing_it() {
        let previous_key = "previous-key-MUST-NOT-be-logged";
        let extracted = Figment::from(Toml::string(&format!(
            "enabled = true\nprevious_keys = \"{previous_key}\""
        )))
        .extract::<EncryptionConfig>();

        match extracted {
            Ok(config) => assert_eq!(config.previous_keys, vec![previous_key.to_owned()]),
            Err(error) => panic!(
                "bare previous key string was refused, echoed: {}",
                error.to_string().contains(previous_key)
            ),
        }
    }

    #[test]
    fn given_retired_kek_fingerprints_when_extracting_should_read_one_or_many_and_serialize_them() {
        let fingerprint = "39e4f0f95c3693c7";
        for value in [format!("\"{fingerprint}\""), format!("[\"{fingerprint}\"]")] {
            let config = Figment::from(Toml::string(&format!(
                "enabled = true\nretired_kek_fingerprints = {value}"
            )))
            .extract::<EncryptionConfig>()
            .expect("extracts");
            assert_eq!(
                config.retired_kek_fingerprints,
                vec![fingerprint.to_owned()]
            );
            let serialized = serde_json::to_string(&config).expect("serialize encryption config");
            assert!(serialized.contains(fingerprint), "{serialized}");
        }
        let defaulted = Figment::from(Toml::string("enabled = true"))
            .extract::<EncryptionConfig>()
            .expect("extracts");
        assert!(defaulted.retired_kek_fingerprints.is_empty());
    }
}
