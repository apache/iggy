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

/// A config key that no longer exists, and what took over from it.
///
/// Nothing else catches such a key. Its struct field is gone and no config
/// table sets `deny_unknown_fields`, so figment drops an unrecognized key
/// without a word from either source: a server still carrying
/// `enforce_fsync = true` would boot reporting success and run without fsync.
/// Every relocated key used to change behavior, so the boot is refused rather
/// than warned about.
#[derive(Debug, Clone, Copy)]
pub struct RelocatedKey {
    /// Dotted config path, for example `system.segment.size`. A deleted table
    /// matches everything nested under it as well.
    pub path: &'static str,
    /// The per-topic option key that replaced it, or `None` when the feature
    /// it configured was removed outright.
    pub replacement: Option<&'static str>,
}

impl RelocatedKey {
    /// Sentence telling the operator where the setting went.
    fn guidance(&self) -> String {
        match self.replacement {
            Some(option) => {
                format!("it is now the per-topic '{option}' option, set on CreateTopic")
            }
            None => "the feature it configured was removed".to_string(),
        }
    }
}

/// File-based configuration provider that combines file, default, and environment configurations.
pub struct FileConfigProvider<P> {
    file_path: String,
    default_config: Option<Data<Toml>>,
    env_provider: P,
    display_config: bool,
    env_prefix: &'static str,
    relocated_keys: &'static [RelocatedKey],
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
            env_prefix: "",
            relocated_keys: &[],
        }
    }

    /// Refuse to load when any of `keys` is still set, in the config file or in
    /// the environment.
    ///
    /// `env_prefix` is the prefix this config's env provider reads, used to
    /// derive the variable name for each path. The table is per-config on
    /// purpose: a server key left over in a shared container environment must
    /// not stop the connectors runtime or the MCP server from booting.
    pub fn with_relocated_keys(
        mut self,
        env_prefix: &'static str,
        keys: &'static [RelocatedKey],
    ) -> Self {
        self.env_prefix = env_prefix;
        self.relocated_keys = keys;
        self
    }

    fn reject_relocated_keys(&self) -> Result<(), ConfigurationError> {
        if self.relocated_keys.is_empty() {
            return Ok(());
        }
        let file = file_exists(&self.file_path).then(|| Figment::from(Toml::file(&self.file_path)));
        let env_names = env::vars_os().filter_map(|(name, _)| name.into_string().ok());
        let mut found = false;

        for key in self.relocated_keys {
            if file
                .as_ref()
                .is_some_and(|file| file.find_value(key.path).is_ok())
            {
                found = true;
                error!(
                    "Config key '{}' no longer exists; {}. Remove the key to boot.",
                    key.path,
                    key.guidance()
                );
            }
        }
        for (name, key) in relocated_env_vars(env_names, self.env_prefix, self.relocated_keys) {
            found = true;
            error!(
                "Environment variable '{name}' sets config key '{}', which no longer exists; {}. \
                 Unset it to boot.",
                key.path,
                key.guidance()
            );
        }

        if found {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

impl<P: Provider + Clone> ConfigProvider for FileConfigProvider<P> {
    async fn load_config<T: ConfigurationType>(&self) -> Result<T, ConfigurationError> {
        info!("Loading config from path: '{}'...", self.file_path);

        // Both sources are checked before either is merged: the env provider
        // below is just as silent about a key no field reads, and the
        // pure-env container never touches the file branch at all.
        self.reject_relocated_keys()?;

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

/// Pair every name in `names` that addresses a relocated key with that key.
///
/// A key's variable name is its path uppercased with dots turned into
/// underscores, matching what `#[derive(ConfigEnv)]` generates. A deleted table
/// also matches its children, so `system.message_deduplication` catches
/// `IGGY_SYSTEM_MESSAGE_DEDUPLICATION_ENABLED`.
fn relocated_env_vars<'keys>(
    names: impl Iterator<Item = String>,
    prefix: &str,
    keys: &'keys [RelocatedKey],
) -> Vec<(String, &'keys RelocatedKey)> {
    let derived: Vec<(String, &RelocatedKey)> = keys
        .iter()
        .map(|key| {
            (
                format!("{prefix}{}", key.path.replace('.', "_").to_uppercase()),
                key,
            )
        })
        .collect();

    let mut found = Vec::new();
    for name in names {
        for (env_name, key) in &derived {
            let nested = name
                .strip_prefix(env_name.as_str())
                .is_some_and(|rest| rest.starts_with('_'));
            if &name == env_name || nested {
                found.push((name, *key));
                break;
            }
        }
    }
    found
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

    const KEYS: &[RelocatedKey] = &[
        RelocatedKey {
            path: "system.partition.enforce_fsync",
            replacement: Some("enforce_fsync"),
        },
        RelocatedKey {
            path: "system.message_deduplication",
            replacement: None,
        },
    ];

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn given_env_var_for_relocated_leaf_when_matching_then_should_report_it() {
        let found = relocated_env_vars(
            names(&["IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC"]).into_iter(),
            "IGGY_",
            KEYS,
        );

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, "IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC");
        assert_eq!(found[0].1.replacement, Some("enforce_fsync"));
    }

    #[test]
    fn given_env_var_under_removed_table_when_matching_then_should_report_it() {
        let found = relocated_env_vars(
            names(&["IGGY_SYSTEM_MESSAGE_DEDUPLICATION_ENABLED"]).into_iter(),
            "IGGY_",
            KEYS,
        );

        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1.path, "system.message_deduplication");
        assert_eq!(found[0].1.replacement, None);
    }

    #[test]
    fn given_live_env_vars_when_matching_then_should_report_none() {
        let found = relocated_env_vars(
            names(&[
                "IGGY_SYSTEM_PARTITION_PATH",
                "IGGY_SYSTEM_PARTITION_ENFORCE_FSYNCHRONIZATION",
                "IGGY_TCP_ADDRESS",
                "RUST_LOG",
            ])
            .into_iter(),
            "IGGY_",
            KEYS,
        );

        assert!(found.is_empty(), "unexpected matches: {found:?}");
    }

    #[test]
    fn given_another_configs_prefix_when_matching_then_should_report_none() {
        let found = relocated_env_vars(
            names(&["IGGY_SYSTEM_PARTITION_ENFORCE_FSYNC"]).into_iter(),
            "IGGY_CONNECTORS_",
            KEYS,
        );

        assert!(found.is_empty(), "unexpected matches: {found:?}");
    }

    #[test]
    fn given_no_relocated_keys_when_rejecting_then_should_accept() {
        let provider = FileConfigProvider::new(
            "nonexistent-config.toml".to_string(),
            Toml::string(""),
            false,
            None,
        );

        assert!(provider.reject_relocated_keys().is_ok());
    }
}
