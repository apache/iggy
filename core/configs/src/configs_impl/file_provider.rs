/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! File-based configuration provider.

use super::error::ConfigurationError;
use super::traits::{ConfigProvider, ConfigurationType};
use figment::{
    Figment, Provider,
    providers::{Data, Format, Toml},
};
use std::{env, fs, path::Path};
use tracing::{error, info, warn};

const DISPLAY_CONFIG_ENV: &str = "IGGY_DISPLAY_CONFIG";

/// Migration guard for the pre-3134 nested cluster schema.
///
/// Before the flatten refactor, the roster lived at `cluster.node.current`
/// (the running node) and `cluster.node.others` (its peers). The new schema
/// uses a flat `cluster.nodes` array. Old TOMLs parse silently against the
/// new `ClusterConfig` because `#[serde(default)]` on `nodes` fills the
/// missing field with an empty vec: an operator who only bumped binaries
/// would boot as a single-node deployment with no cluster roster and no
/// warning. Refuse to start with an explicit migration pointer instead.
fn reject_legacy_cluster_schema(raw: &str) -> Result<(), ConfigurationError> {
    let parsed: toml::Value = match toml::from_str(raw) {
        Ok(v) => v,
        // A malformed file will fail later in figment with a clearer
        // parser-level error; don't duplicate that diagnostic here.
        Err(_) => return Ok(()),
    };

    let Some(node_table) = parsed.get("cluster").and_then(|c| c.get("node")) else {
        return Ok(());
    };

    let legacy_key = ["current", "others"]
        .into_iter()
        .find(|k| node_table.get(*k).is_some());
    let Some(key) = legacy_key else {
        return Ok(());
    };

    error!(
        "Legacy cluster schema detected: `cluster.node.{key}` is no longer \
         supported. Migrate to the flat `[[cluster.nodes]]` array: move \
         `cluster.node.current` and every entry in `cluster.node.others` \
         into a single `cluster.nodes` list and set each node's \
         `replica_id` explicitly. The running node is selected at startup \
         via the `--replica-id` CLI flag."
    );
    Err(ConfigurationError::CannotLoadConfiguration)
}

/// File-based configuration provider that combines file, default, and environment configurations.
pub struct FileConfigProvider<P> {
    file_path: String,
    default_config: Option<Data<Toml>>,
    env_provider: P,
    display_config: bool,
}

impl<P: Provider> FileConfigProvider<P> {
    /// Create a new file configuration provider.
    ///
    /// # Arguments
    /// * `file_path` - Path to the configuration file
    /// * `env_provider` - Environment variable provider
    /// * `display_config` - Whether to display the loaded configuration
    /// * `default_config` - Optional default configuration data
    pub fn new(
        file_path: String,
        env_provider: P,
        display_config: bool,
        default_config: Option<Data<Toml>>,
    ) -> Self {
        Self {
            file_path,
            env_provider,
            default_config,
            display_config,
        }
    }
}

impl<P: Provider + Clone> ConfigProvider for FileConfigProvider<P> {
    async fn load_config<T: ConfigurationType>(&self) -> Result<T, ConfigurationError> {
        info!("Loading config from path: '{}'...", self.file_path);

        // Start with the default configuration if provided
        let mut config_builder = Figment::new();
        let has_default = self.default_config.is_some();
        if let Some(default) = &self.default_config {
            config_builder = config_builder.merge(default);
        } else {
            warn!("No default configuration provided.");
        }

        // If the config file exists, merge it into the configuration
        if file_exists(&self.file_path) {
            info!("Found configuration file at path: '{}'.", self.file_path);
            if let Ok(raw) = fs::read_to_string(&self.file_path) {
                reject_legacy_cluster_schema(&raw)?;
            }
            config_builder = config_builder.merge(Toml::file(&self.file_path));
        } else {
            warn!(
                "Configuration file not found at path: '{}'.",
                self.file_path
            );
            if has_default {
                info!(
                    "Using default configuration embedded into server, as no config file was found."
                );
            }
        }

        // Merge environment variables into the configuration
        config_builder = config_builder.merge(self.env_provider.clone());

        // Finally, attempt to extract the final configuration
        let config_result: Result<T, figment::Error> = config_builder.extract();

        match config_result {
            Ok(config) => {
                info!("Config loaded successfully.");
                let display_config = env::var(DISPLAY_CONFIG_ENV)
                    .map(|val| val == "1" || val.to_lowercase() == "true")
                    .unwrap_or(self.display_config);
                if display_config {
                    info!("Using Config: {config}");
                }
                Ok(config)
            }
            Err(e) => {
                error!("Failed to load config: {e}");
                Err(ConfigurationError::CannotLoadConfiguration)
            }
        }
    }
}

fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();

    if path.is_absolute() {
        return path.is_file();
    }

    let cwd = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(_) => return false,
    };

    let mut current_dir = cwd.as_path();
    loop {
        let file_path = current_dir.join(path);
        if file_path.is_file() {
            return true;
        }

        current_dir = match current_dir.parent() {
            Some(parent) => parent,
            None => return false,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_flat_cluster_schema() {
        let raw = r#"
[cluster]
enabled = true
name = "demo"

[[cluster.nodes]]
name = "n0"
ip = "127.0.0.1"
replica_id = 0
ports = { tcp = 8090 }
"#;
        reject_legacy_cluster_schema(raw).expect("flat schema must be accepted");
    }

    #[test]
    fn accepts_file_with_no_cluster_section() {
        let raw = r#"
[tcp]
enabled = true
address = "0.0.0.0:8090"
"#;
        reject_legacy_cluster_schema(raw).expect("no cluster.node: nothing to reject");
    }

    #[test]
    fn rejects_legacy_cluster_node_current() {
        let raw = r#"
[cluster]
enabled = true
name = "demo"

[cluster.node.current]
name = "n0"
ip = "127.0.0.1"
replica_id = 0
"#;
        let err = reject_legacy_cluster_schema(raw).expect_err("legacy schema must fail");
        assert_eq!(err, ConfigurationError::CannotLoadConfiguration);
    }

    #[test]
    fn rejects_legacy_cluster_node_others() {
        let raw = r#"
[cluster]
enabled = true
name = "demo"

[[cluster.node.others]]
name = "n1"
ip = "127.0.0.2"
replica_id = 1
"#;
        let err = reject_legacy_cluster_schema(raw).expect_err("legacy schema must fail");
        assert_eq!(err, ConfigurationError::CannotLoadConfiguration);
    }

    #[test]
    fn ignores_malformed_toml() {
        // Upstream figment emits a parser error on malformed input; the
        // migration guard must not turn that into its own error.
        let raw = "[cluster\nnot valid toml";
        reject_legacy_cluster_schema(raw).expect("malformed toml defers to figment");
    }
}
