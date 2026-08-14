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

//! File-based configuration provider.

use super::error::ConfigurationError;
use super::traits::{ConfigProvider, ConfigurationType};
use figment::{
    Figment, Provider,
    providers::{Data, Format, Toml},
};
use std::{env, path::Path};
use tracing::{error, info, warn};

const DISPLAY_CONFIG_ENV: &str = "IGGY_DISPLAY_CONFIG";

/// Config keys that became per-topic options, paired with the option key that
/// replaced each one.
///
/// Nothing else catches them. Their struct fields are gone and no config table
/// sets `deny_unknown_fields`, so figment drops an unrecognized key without a
/// word: a server still carrying `enforce_fsync = true` would boot reporting
/// success and run without fsync. Every entry here used to change behavior, so
/// the boot is refused rather than warned about.
const RELOCATED_CONFIG_KEYS: &[(&str, &str)] = &[
    ("system.topic.max_size", "max_topic_size"),
    ("system.topic.message_expiry", "message_expiry"),
    ("system.partition.enforce_fsync", "enforce_fsync"),
    (
        "system.partition.messages_required_to_save",
        "messages_required_to_save",
    ),
    (
        "system.partition.size_of_messages_required_to_save",
        "size_of_messages_required_to_save",
    ),
    ("system.segment.size", "segment_size"),
    ("system.segment.preallocate", "preallocate_segments"),
];

/// Refuse a config file that still sets a key which moved to a topic option.
fn reject_relocated_keys(file_path: &str) -> Result<(), ConfigurationError> {
    let file = Figment::from(Toml::file(file_path));
    let mut found = false;
    for (key, option) in RELOCATED_CONFIG_KEYS {
        if file.find_value(key).is_ok() {
            found = true;
            error!(
                "Config key '{key}' no longer exists; it is now the per-topic '{option}' option, \
                 set on CreateTopic. Remove the key to boot."
            );
        }
    }
    if found {
        return Err(ConfigurationError::InvalidConfigurationValue);
    }
    Ok(())
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
            reject_relocated_keys(&self.file_path)?;
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
