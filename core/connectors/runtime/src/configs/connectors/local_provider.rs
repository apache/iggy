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

use crate::configs::connectors::{
    ConnectorConfig, ConnectorConfigVersionInfo, ConnectorConfigVersions, ConnectorsConfig,
    ConnectorsConfigProvider, CreateSinkConfig, CreateSourceConfig, SinkConfig, SourceConfig,
};
use crate::error::RuntimeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{ConfigProvider, CustomEnvProvider, FileConfigProvider};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct ConnectorId {
    key: String,
    version: u64,
}

impl ConnectorId {
    fn to_filename_key(&self) -> String {
        format!("{}_{}", self.key, self.version)
    }
}

impl From<&ConnectorConfig> for ConnectorId {
    fn from(value: &ConnectorConfig) -> Self {
        match value {
            ConnectorConfig::Sink(config) => ConnectorId {
                key: config.key.clone(),
                version: config.version,
            },
            ConnectorConfig::Source(config) => ConnectorId {
                key: config.key.clone(),
                version: config.version,
            },
        }
    }
}

#[derive(Clone)]
struct SinkConfigFile {
    config: SinkConfig,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    path: String,
}

#[derive(Clone)]
struct SourceConfigFile {
    config: SourceConfig,
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    path: String,
}

#[derive(Default)]
struct ImportedConfigurations {
    sinks: DashMap<ConnectorId, SinkConfigFile>,
    sources: DashMap<ConnectorId, SourceConfigFile>,
}

impl ImportedConfigurations {
    fn sinks(&self) -> &DashMap<ConnectorId, SinkConfigFile> {
        &self.sinks
    }

    fn sinks_grouped_by_key(&self) -> HashMap<String, Vec<SinkConfigFile>> {
        let mut grouped: HashMap<String, Vec<SinkConfigFile>> = HashMap::new();
        for entry in self.sinks.iter() {
            let key = entry.key().key.clone();
            grouped.entry(key).or_default().push(entry.value().clone());
        }
        grouped
    }

    fn sources(&self) -> &DashMap<ConnectorId, SourceConfigFile> {
        &self.sources
    }

    fn sources_grouped_by_key(&self) -> HashMap<String, Vec<SourceConfigFile>> {
        let mut grouped: HashMap<String, Vec<SourceConfigFile>> = HashMap::new();
        for entry in self.sources.iter() {
            let key = entry.key().key.clone();
            grouped.entry(key).or_default().push(entry.value().clone());
        }
        grouped
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ActiveConfigVersions {
    #[serde(default)]
    sinks: HashMap<String, u64>,
    #[serde(default)]
    sources: HashMap<String, u64>,
}

pub trait ProviderState {}

pub struct Created {}

impl ProviderState for Created {}

#[derive(Default)]
pub struct Initialized {
    connectors_config: ImportedConfigurations,
}

impl ProviderState for Initialized {}

pub struct LocalConnectorsConfigProvider<S: ProviderState> {
    config_dir: String,
    state: S,
}

impl LocalConnectorsConfigProvider<Created> {
    pub fn new(config_dir: &str) -> Self {
        Self {
            config_dir: config_dir.to_owned(),
            state: Created {},
        }
    }

    pub async fn init(&self) -> Result<LocalConnectorsConfigProvider<Initialized>, RuntimeError> {
        if self.config_dir.is_empty() {
            return Err(RuntimeError::InvalidConfiguration(
                "Connectors configuration directory not provided".to_string(),
            ));
        }
        if !std::fs::exists(&self.config_dir)? {
            warn!(
                "Connectors configuration directory does not exist: {}",
                self.config_dir
            );
            std::fs::create_dir_all(&self.config_dir)?;
            return Ok(LocalConnectorsConfigProvider {
                config_dir: self.config_dir.clone(),
                state: Initialized::default(),
            });
        }

        let sinks: DashMap<ConnectorId, SinkConfigFile> = DashMap::new();
        let sources: DashMap<ConnectorId, SourceConfigFile> = DashMap::new();
        info!("Loading connectors configuration from: {}", self.config_dir);
        let entries = std::fs::read_dir(&self.config_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                    && file_name.starts_with('.')
                {
                    debug!("Skipping hidden file: {:?}", path);
                    continue;
                }

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

                let created_at: DateTime<Utc> = entry.metadata()?.created()?.into();
                let connector_id: ConnectorId = (&connector_config).into();
                match connector_config.clone() {
                    ConnectorConfig::Sink(sink_config) => {
                        sinks.insert(
                            connector_id,
                            SinkConfigFile {
                                config: sink_config,
                                created_at,
                                path,
                            },
                        );
                    }
                    ConnectorConfig::Source(source_config) => {
                        sources.insert(
                            connector_id,
                            SourceConfigFile {
                                config: source_config,
                                created_at,
                                path,
                            },
                        );
                    }
                }

                debug!(
                    "Loaded connector configuration with key {}, version {}, created at {}",
                    base_config.key(),
                    connector_config.version(),
                    created_at.to_rfc3339()
                );
            }
        }
        Ok(LocalConnectorsConfigProvider {
            config_dir: self.config_dir.clone(),
            state: Initialized {
                connectors_config: ImportedConfigurations { sinks, sources },
            },
        })
    }
}

impl LocalConnectorsConfigProvider<Initialized> {
    fn active_versions_file_path(&self) -> String {
        format!("{}/.active_versions.toml", self.config_dir)
    }

    fn load_active_versions(&self) -> ActiveConfigVersions {
        let path = self.active_versions_file_path();
        if !Path::new(&path).exists() {
            return ActiveConfigVersions::default();
        }

        match std::fs::read(&path) {
            Ok(data) => toml::from_slice(&data).unwrap_or_else(|err| {
                warn!(
                    "Failed to parse active versions file '{}': {}",
                    path,
                    err.message()
                );
                ActiveConfigVersions::default()
            }),
            Err(err) => {
                warn!("Failed to read active versions file '{}': {}", path, err);
                ActiveConfigVersions::default()
            }
        }
    }

    fn save_active_versions(&self, versions: &ActiveConfigVersions) -> Result<(), RuntimeError> {
        let path = self.active_versions_file_path();
        let content = toml::to_string(versions).map_err(|err| {
            RuntimeError::InvalidConfiguration(format!(
                "Failed to serialize active versions: {}",
                err
            ))
        })?;
        std::fs::write(&path, content)?;
        Ok(())
    }
}

impl<S: ProviderState> LocalConnectorsConfigProvider<S> {
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
impl ConnectorsConfigProvider for LocalConnectorsConfigProvider<Initialized> {
    async fn create_sink_config(
        &self,
        key: &str,
        cmd: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError> {
        let sinks = self.state.connectors_config.sinks();
        let next_version = sinks
            .iter()
            .filter(|entry| entry.key().key == key)
            .max_by_key(|entry| entry.config.version)
            .map(|entry| entry.config.version + 1)
            .unwrap_or(0);

        let config = cmd.to_sink_config(key, next_version);
        let connector_config = ConnectorConfig::Sink(config.clone());
        let connector_id: ConnectorId = (&connector_config).into();

        let path = format!(
            "{}/sink_{}.toml",
            self.config_dir,
            connector_id.to_filename_key()
        );
        std::fs::write(&path, toml::to_string(&connector_config).unwrap())?;
        sinks.insert(
            connector_id,
            SinkConfigFile {
                config: config.clone(),
                created_at: Utc::now(),
                path: path.clone(),
            },
        );

        Ok(config)
    }

    async fn create_source_config(
        &self,
        key: &str,
        cmd: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError> {
        let sources = &self.state.connectors_config.sources;
        let next_version = sources
            .iter()
            .filter(|entry| entry.key().key == key)
            .max_by_key(|entry| entry.config.version)
            .map(|entry| entry.config.version + 1)
            .unwrap_or(0);

        let config = cmd.to_source_config(key, next_version);
        let connector_config = ConnectorConfig::Source(config.clone());
        let connector_id: ConnectorId = (&connector_config).into();

        let path = format!(
            "{}/source_{}.toml",
            self.config_dir,
            connector_id.to_filename_key()
        );
        std::fs::write(&path, toml::to_string(&connector_config).unwrap())?;
        sources.insert(
            connector_id,
            SourceConfigFile {
                config: config.clone(),
                created_at: Utc::now(),
                path: path.clone(),
            },
        );

        Ok(config)
    }

    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
        let all_configs = &self.state.connectors_config;
        let active_versions = self.load_active_versions();

        let sinks = all_configs
            .sinks_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let active_config = if let Some(&version) = active_versions.sinks.get(key) {
                    config_files
                        .iter()
                        .find(|c| c.config.version == version)
                        .cloned()
                } else {
                    config_files
                        .iter()
                        .max_by_key(|c| c.config.version)
                        .cloned()
                };
                active_config.map(|config_file| (key.clone(), config_file.config.clone()))
            })
            .collect();

        let sources = all_configs
            .sources_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let active_config = if let Some(&version) = active_versions.sources.get(key) {
                    config_files
                        .iter()
                        .find(|c| c.config.version == version)
                        .cloned()
                } else {
                    config_files
                        .iter()
                        .max_by_key(|c| c.config.version)
                        .cloned()
                };
                active_config.map(|config_file| (key.clone(), config_file.config.clone()))
            })
            .collect();

        Ok(ConnectorsConfig::new(sinks, sources))
    }

    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError> {
        let all_configs = &self.state.connectors_config;
        let active_versions = self.load_active_versions();

        let sinks = all_configs
            .sinks_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let latest_version = config_files
                    .iter()
                    .map(|c| c.config.version)
                    .max()
                    .expect("At least one config version must exist");
                let active_version = active_versions
                    .sinks
                    .get(key)
                    .copied()
                    .unwrap_or(latest_version);

                config_files
                    .iter()
                    .find(|config_file| config_file.config.version == active_version)
                    .map(|config_file| ConnectorConfigVersionInfo {
                        version: config_file.config.version,
                        created_at: config_file.created_at,
                    })
                    .map(|config| (key.clone(), config))
            })
            .collect();

        let sources = all_configs
            .sources_grouped_by_key()
            .iter()
            .filter_map(|(key, config_files)| {
                if config_files.is_empty() {
                    return None;
                }
                let latest_version = config_files
                    .iter()
                    .map(|c| c.config.version)
                    .max()
                    .expect("At least one config version must exist");
                let active_version = active_versions
                    .sources
                    .get(key)
                    .copied()
                    .unwrap_or(latest_version);

                config_files
                    .iter()
                    .find(|config_file| config_file.config.version == active_version)
                    .map(|config_file| ConnectorConfigVersionInfo {
                        version: config_file.config.version,
                        created_at: config_file.created_at,
                    })
                    .map(|config| (key.clone(), config))
            })
            .collect();

        Ok(ConnectorConfigVersions { sinks, sources })
    }

    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let connector_id = ConnectorId {
            key: key.to_owned(),
            version,
        };
        if self
            .state
            .connectors_config
            .sinks()
            .get(&connector_id)
            .is_none()
        {
            return Err(RuntimeError::SinkConfigNotFound(key.to_owned(), version));
        }

        let mut active_versions = self.load_active_versions();
        active_versions.sinks.insert(key.to_owned(), version);
        self.save_active_versions(&active_versions)
    }

    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError> {
        let connector_id = ConnectorId {
            key: key.to_owned(),
            version,
        };
        if self
            .state
            .connectors_config
            .sources()
            .get(&connector_id)
            .is_none()
        {
            return Err(RuntimeError::SourceConfigNotFound(key.to_owned(), version));
        }

        let mut active_versions = self.load_active_versions();
        active_versions.sources.insert(key.to_owned(), version);
        self.save_active_versions(&active_versions)
    }

    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError> {
        Ok(self
            .state
            .connectors_config
            .sinks_grouped_by_key()
            .get(key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|config_file| config_file.config)
            .collect())
    }

    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError> {
        if let Some(version) = version {
            let connector_id = ConnectorId {
                key: key.to_owned(),
                version,
            };
            Ok(self
                .state
                .connectors_config
                .sinks()
                .get(&connector_id)
                .map(|entry| entry.config.clone()))
        } else {
            Ok(self
                .get_sink_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }

    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError> {
        Ok(self
            .state
            .connectors_config
            .sources_grouped_by_key()
            .get(key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|config_file| config_file.config)
            .collect())
    }

    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError> {
        if let Some(version) = version {
            let connector_id = ConnectorId {
                key: key.to_owned(),
                version,
            };
            Ok(self
                .state
                .connectors_config
                .sources()
                .get(&connector_id)
                .map(|entry| entry.config.clone()))
        } else {
            Ok(self
                .get_source_configs(key)
                .await?
                .into_iter()
                .max_by_key(|config| config.version))
        }
    }

    async fn delete_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        debug!("Deleting sink config: {}@{:?}", &key, &version);
        let sinks = self.state.connectors_config.sinks();
        let active_versions = self.load_active_versions();

        let version_to_delete = version
            .or(active_versions.sinks.get(key).copied())
            .ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_owned(), 0))?;

        let connector_id = ConnectorId {
            key: key.to_owned(),
            version: version_to_delete,
        };

        let config_file = {
            sinks
                .get(&connector_id)
                .ok_or_else(|| RuntimeError::SinkConfigNotFound(key.to_owned(), version_to_delete))?
                .value()
                .clone()
        };

        std::fs::remove_file(&config_file.path)?;
        sinks.remove(&connector_id);

        let mut active_versions = self.load_active_versions();
        let remaining_versions: Vec<u64> = sinks
            .iter()
            .filter(|entry| entry.key().key == key)
            .map(|entry| entry.key().version)
            .collect();

        if remaining_versions.is_empty() {
            active_versions.sinks.remove(key);
        } else if Some(version_to_delete) == active_versions.sinks.get(key).copied() {
            let latest_version = remaining_versions
                .into_iter()
                .max()
                .expect("At least one version must exist");
            active_versions.sinks.insert(key.to_owned(), latest_version);
        }

        self.save_active_versions(&active_versions)?;
        debug!("Deleted sink configuration: {}@{:?}", &key, &version);
        Ok(())
    }

    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError> {
        debug!("Deleting source config: {}@{:?}", &key, &version);
        let sources = self.state.connectors_config.sources();
        let active_versions = self.load_active_versions();

        let version_to_delete = version
            .or(active_versions.sources.get(key).copied())
            .ok_or_else(|| RuntimeError::SourceConfigNotFound(key.to_owned(), 0))?;

        let connector_id = ConnectorId {
            key: key.to_owned(),
            version: version_to_delete,
        };

        let config_file = {
            sources
                .get(&connector_id)
                .ok_or_else(|| {
                    RuntimeError::SourceConfigNotFound(key.to_owned(), version_to_delete)
                })?
                .value()
                .clone()
        };

        std::fs::remove_file(&config_file.path)?;
        sources.remove(&connector_id);

        let mut active_versions = self.load_active_versions();
        let remaining_versions: Vec<u64> = sources
            .iter()
            .filter(|entry| entry.key().key == key)
            .map(|entry| entry.key().version)
            .collect();

        if remaining_versions.is_empty() {
            active_versions.sources.remove(key);
        } else if Some(version_to_delete) == active_versions.sources.get(key).copied() {
            let latest_version = remaining_versions
                .into_iter()
                .max()
                .expect("At least one version must exist");
            active_versions
                .sources
                .insert(key.to_owned(), latest_version);
        }

        self.save_active_versions(&active_versions)?;
        debug!("Deleted source configuration: {}@{:?}", &key, &version);
        Ok(())
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
