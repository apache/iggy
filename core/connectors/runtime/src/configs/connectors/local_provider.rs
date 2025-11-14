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

use crate::configs::connectors::local_provider::ConnectorType::{Sink, Source};
use crate::configs::connectors::{
    ConnectorConfig, ConnectorsConfig, ConnectorsConfigProvider, SinkConfig, SourceConfig,
};
use crate::error::RuntimeError;
use async_trait::async_trait;
use dashmap::DashMap;
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{ConfigProvider, CustomEnvProvider, FileConfigProvider};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use strum::Display;
use tracing::{debug, info, warn};

pub struct LocalConnectorsConfigProvider {
    config_dir: String,
    file_mapping: DashMap<String, String>,
}

impl LocalConnectorsConfigProvider {
    pub fn new(config_dir: &str) -> Self {
        Self {
            config_dir: config_dir.to_owned(),
            file_mapping: DashMap::new(),
        }
    }

    fn create_file_config_provider(
        path: String,
        base_config: &BaseConnectorConfig,
    ) -> FileConfigProvider<ConnectorEnvProvider> {
        FileConfigProvider::new(
            path,
            ConnectorEnvProvider::with_connector_base_config(base_config),
            false,
            None,
        )
    }

    fn read_base_config(path: &Path) -> Result<BaseConnectorConfig, RuntimeError> {
        let config_data = std::fs::read(path)?;
        toml::from_slice(&config_data).map_err(|err| {
            RuntimeError::InvalidConfiguration(format!(
                "parsing TOML file '{}' raised an error: {}",
                path.display(),
                err.message()
            ))
        })
    }

    fn config_dir_exists(&self) -> Result<bool, RuntimeError> {
        if self.config_dir.is_empty() {
            warn!("Connectors configuration directory not provided");
            return Ok(false);
        }
        if !std::fs::exists(&self.config_dir)? {
            warn!(
                "Connectors configuration directory does not exist: {}",
                self.config_dir
            );
            return Ok(false);
        }
        Ok(true)
    }

    fn get_paths_with_key_prefix(&self, prefix: &str) -> Vec<String> {
        self.file_mapping
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>()
    }
}

#[derive(Display, Debug)]
enum ConnectorType {
    Sink,
    Source,
}

#[derive(Debug)]
struct ConnectorConfigFileId {
    connector_type: ConnectorType,
    key: String,
    version: u64,
}

impl ConnectorConfigFileId {
    fn from_sink_key_and_version(key: &str, version: u64) -> Self {
        Self {
            connector_type: Sink,
            key: key.to_owned(),
            version,
        }
    }

    fn from_sink_key(key: &str) -> Self {
        Self {
            connector_type: Sink,
            key: key.to_owned(),
            version: 0,
        }
    }

    fn from_source_key_and_version(key: &str, version: u64) -> Self {
        Self {
            connector_type: Source,
            key: key.to_owned(),
            version,
        }
    }

    fn from_source_key(key: &str) -> Self {
        Self {
            connector_type: Source,
            key: key.to_owned(),
            version: 0,
        }
    }

    fn to_file_mapping_key(&self) -> String {
        format!("{}_{}_{}", self.connector_type, self.key, self.version)
    }

    fn to_file_mapping_key_prefix(&self) -> String {
        format!("{}_{}_", self.connector_type, self.key)
    }
}

impl From<ConnectorConfig> for ConnectorConfigFileId {
    fn from(value: ConnectorConfig) -> Self {
        match value {
            ConnectorConfig::Sink(config) => Self {
                connector_type: Sink,
                key: config.key,
                version: config.version,
            },
            ConnectorConfig::Source(config) => Self {
                connector_type: Source,
                key: config.key,
                version: config.version,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BaseConnectorConfig {
    Sink { key: String },
    Source { key: String },
}

impl BaseConnectorConfig {
    fn key(&self) -> &str {
        match self {
            BaseConnectorConfig::Sink { key, .. } => key,
            BaseConnectorConfig::Source { key, .. } => key,
        }
    }

    fn connector_type(&self) -> &str {
        match self {
            BaseConnectorConfig::Sink { .. } => "sink",
            BaseConnectorConfig::Source { .. } => "source",
        }
    }
}

#[async_trait]
impl ConnectorsConfigProvider for LocalConnectorsConfigProvider {
    async fn get_all_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
        if !self.config_dir_exists()? {
            return Ok(ConnectorsConfig::default());
        }

        let mut sinks: HashMap<String, Vec<SinkConfig>> = HashMap::new();
        let mut sources: HashMap<String, Vec<SourceConfig>> = HashMap::new();
        info!("Loading connectors configuration from: {}", self.config_dir);
        let entries = std::fs::read_dir(&self.config_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                debug!("Loading connector configuration from: {:?}", path);
                let base_config = Self::read_base_config(&path)?;
                debug!("Loaded base configuration: {:?}", base_config);
                let path = path
                    .to_str()
                    .expect("Failed to convert connector configuration path to string")
                    .to_string();
                let connector_config: ConnectorConfig =
                    Self::create_file_config_provider(path.clone(), &base_config)
                        .load_config()
                        .await
                        .expect("Failed to load connector configuration");

                match connector_config.clone() {
                    ConnectorConfig::Sink(sink_config) => {
                        sinks
                            .entry(base_config.key().to_owned())
                            .or_default()
                            .push(sink_config);
                    }
                    ConnectorConfig::Source(source_config) => {
                        sources
                            .entry(base_config.key().to_owned())
                            .or_default()
                            .push(source_config);
                    }
                }

                let file_id: ConnectorConfigFileId = connector_config.into();
                self.file_mapping
                    .insert(file_id.to_file_mapping_key(), path);
            }
        }
        Ok(ConnectorsConfig { sinks, sources })
    }

    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError> {
        if !self.config_dir_exists()? {
            return Ok(Vec::new());
        }

        let file_id = ConnectorConfigFileId::from_sink_key(key);
        let file_mapping_key_prefix = file_id.to_file_mapping_key_prefix();

        let paths = self.get_paths_with_key_prefix(&file_mapping_key_prefix);

        let mut configs = Vec::new();
        for path in paths {
            let base_config = BaseConnectorConfig::Sink {
                key: key.to_owned(),
            };
            let connector_config: ConnectorConfig =
                Self::create_file_config_provider(path, &base_config)
                    .load_config()
                    .await
                    .expect("Failed to load sink configuration");
            if let ConnectorConfig::Sink(sink_config) = connector_config {
                configs.push(sink_config);
            }
        }
        Ok(configs)
    }

    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError> {
        if !self.config_dir_exists()? {
            return Ok(None);
        }

        if let Some(version) = version {
            let file_id = ConnectorConfigFileId::from_sink_key_and_version(key, version);
            if let Some(path) = self.file_mapping.get(&file_id.to_file_mapping_key()) {
                let path = path.value().clone();
                let base_config = BaseConnectorConfig::Sink {
                    key: key.to_owned(),
                };
                let connector_config: ConnectorConfig =
                    Self::create_file_config_provider(path, &base_config)
                        .load_config()
                        .await
                        .expect("Failed to load sink configuration");
                if let ConnectorConfig::Sink(sink_config) = connector_config {
                    Ok(Some(sink_config))
                } else {
                    Err(RuntimeError::InvalidConfiguration(
                        "Configuration is not a sink configuration".to_owned(),
                    ))
                }
            } else {
                debug!("No file mapping found for connector config: {:?}", file_id);
                Ok(None)
            }
        } else {
            Ok(self
                .get_sink_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }

    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError> {
        if !self.config_dir_exists()? {
            return Ok(Vec::new());
        }

        let file_id = ConnectorConfigFileId::from_source_key(key);
        let file_mapping_key_prefix = file_id.to_file_mapping_key_prefix();

        let paths = self.get_paths_with_key_prefix(&file_mapping_key_prefix);

        let mut configs = Vec::new();
        for path in paths {
            let base_config = BaseConnectorConfig::Sink {
                key: key.to_owned(),
            };
            let connector_config: ConnectorConfig =
                Self::create_file_config_provider(path, &base_config)
                    .load_config()
                    .await
                    .expect("Failed to load sink configuration");
            if let ConnectorConfig::Source(sink_config) = connector_config {
                configs.push(sink_config);
            }
        }
        Ok(configs)
    }

    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError> {
        if !self.config_dir_exists()? {
            return Ok(None);
        }

        if let Some(version) = version {
            let file_id = ConnectorConfigFileId::from_source_key_and_version(key, version);

            if let Some(path) = self.file_mapping.get(&file_id.to_file_mapping_key()) {
                let path = path.value().clone();
                let base_config = BaseConnectorConfig::Sink {
                    key: key.to_owned(),
                };
                let connector_config: ConnectorConfig =
                    Self::create_file_config_provider(path, &base_config)
                        .load_config()
                        .await
                        .expect("Failed to load sink configuration");
                if let ConnectorConfig::Source(sink_config) = connector_config {
                    Ok(Some(sink_config))
                } else {
                    Err(RuntimeError::InvalidConfiguration(
                        "Configuration is not a source configuration".to_owned(),
                    ))
                }
            } else {
                debug!("No file mapping found for connector config: {:?}", file_id);
                Ok(None)
            }
        } else {
            Ok(self
                .get_source_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorEnvProvider {
    connector_name: String,
    provider: CustomEnvProvider<ConnectorConfig>,
}

impl ConnectorEnvProvider {
    fn with_connector_base_config(base_config: &BaseConnectorConfig) -> Self {
        let connector_type = base_config.connector_type().to_uppercase();
        let key = base_config.key().to_uppercase();
        let prefix = format!("IGGY_CONNECTORS_{}_{}_", connector_type, key);
        Self {
            connector_name: base_config.key().to_owned(),
            provider: CustomEnvProvider::new(&prefix, &[]),
        }
    }
}

impl Provider for ConnectorEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(format!("iggy-connectors-{}-config", self.connector_name))
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|_| {
            figment::Error::from(format!(
                "Cannot deserialize environment variables for connector config {}",
                self.connector_name
            ))
        })
    }
}
