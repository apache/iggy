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

use crate::configs::connectors::{ConnectorConfig, ConnectorsConfig, ConnectorsConfigProvider};
use crate::error::RuntimeError;
use async_trait::async_trait;
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{ConfigProvider, CustomEnvProvider, FileConfigProvider};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, info, warn};

pub struct LocalConnectorsConfigProvider {
    config_dir: String,
}

impl LocalConnectorsConfigProvider {
    pub fn new(config_dir: &str) -> Self {
        Self {
            config_dir: config_dir.to_owned(),
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
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BaseConnectorConfig {
    Sink { id: String },
    Source { id: String },
}

impl BaseConnectorConfig {
    fn id(&self) -> &str {
        match self {
            BaseConnectorConfig::Sink { id } => id,
            BaseConnectorConfig::Source { id } => id,
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
    async fn load_configs(&self) -> Result<ConnectorsConfig, RuntimeError> {
        if self.config_dir.is_empty() {
            info!("Connectors configuration directory not provided, skipping initialization");
            return Ok(ConnectorsConfig::default());
        }
        if !std::fs::exists(&self.config_dir)? {
            warn!(
                "Connectors configuration directory does not exist: {}",
                self.config_dir
            );
            return Ok(ConnectorsConfig::default());
        }
        let mut sinks = HashMap::new();
        let mut sources = HashMap::new();
        info!("Loading connectors configuration from: {}", self.config_dir);
        let entries = std::fs::read_dir(&self.config_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                debug!("Loading connector configuration from: {:?}", path);
                let base_config = Self::read_base_config(&path)?;
                debug!("Loaded base configuration: {:?}", base_config);
                let connector_config: ConnectorConfig = Self::create_file_config_provider(
                    path.to_str()
                        .expect("Failed to convert connector configuration path to string")
                        .to_string(),
                    &base_config,
                )
                .load_config()
                .await
                .expect("Failed to load connector configuration");

                match connector_config {
                    ConnectorConfig::Sink(sink_config) => {
                        sinks.insert(base_config.id().to_owned(), sink_config);
                    }
                    ConnectorConfig::Source(source_config) => {
                        sources.insert(base_config.id().to_owned(), source_config);
                    }
                }
            }
        }
        Ok(ConnectorsConfig { sinks, sources })
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
        let id = base_config.id().to_uppercase();
        let prefix = format!("IGGY_CONNECTORS_{}_{}_", connector_type, id);
        Self {
            connector_name: base_config.id().to_owned(),
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
