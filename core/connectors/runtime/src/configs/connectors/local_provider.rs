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
use std::collections::HashMap;
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
        connector_name: &str,
    ) -> FileConfigProvider<ConnectorEnvProvider> {
        FileConfigProvider::new(
            path,
            ConnectorEnvProvider::with_connector_name(connector_name),
            false,
            None,
        )
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
        let mut configs = HashMap::new();
        info!("Loading connectors configuration from: {}", self.config_dir);
        let entries = std::fs::read_dir(&self.config_dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                debug!("Loading connector configuration from: {:?}", path);
                let connector_name = path
                    .file_stem()
                    .expect("Failed to get connector configuration name")
                    .to_string_lossy()
                    .to_string();
                let connector_config = Self::create_file_config_provider(
                    path.to_str()
                        .expect("Failed to convert connector configuration path to string")
                        .to_string(),
                    &connector_name,
                )
                .load_config()
                .await
                .expect("Failed to load connector configuration");
                configs.insert(connector_name, connector_config);
            }
        }
        Ok(ConnectorsConfig { configs })
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorEnvProvider {
    connector_name: String,
    provider: CustomEnvProvider<ConnectorConfig>,
}

impl ConnectorEnvProvider {
    fn with_connector_name(connector_name: &str) -> Self {
        let prefix = format!("IGGY_CONNECTORS_{}_", connector_name.to_uppercase());
        Self {
            connector_name: connector_name.to_owned(),
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
