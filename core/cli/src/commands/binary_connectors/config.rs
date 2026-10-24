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

use super::client::{ConnectorEndpoint, ConnectorKind};
use super::{bool_field, str_field};
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::Client;
use std::path::PathBuf;
use tracing::{Level, event};

fn configs_path(kind: ConnectorKind, key: &str) -> String {
    format!("/{}/{key}/configs", kind.path())
}

pub struct GetConnectorConfigsCmd {
    kind: ConnectorKind,
    key: String,
    endpoint: ConnectorEndpoint,
}

impl GetConnectorConfigsCmd {
    pub fn new(kind: ConnectorKind, key: String, endpoint: ConnectorEndpoint) -> Self {
        Self {
            kind,
            key,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for GetConnectorConfigsCmd {
    fn explain(&self) -> String {
        format!(
            "list configurations of {} {}",
            self.kind.singular(),
            self.key
        )
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let configs = api
            .get_value(&configs_path(self.kind, &self.key))
            .await
            .with_context(|| {
                format!(
                    "problem getting configurations of {} {}",
                    self.kind.singular(),
                    self.key
                )
            })?;
        let configs = configs.as_array().cloned().unwrap_or_default();

        if configs.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO,
                "No configurations found for {} {}!", self.kind.singular(), self.key);
            return Ok(());
        }

        let mut table = Table::new();
        table.set_header(vec!["Version", "Active", "Enabled", "Name"]);
        configs.iter().for_each(|config| {
            table.add_row(vec![
                config.get("version").map(super::cell).unwrap_or_default(),
                bool_field(config, "active").to_string(),
                bool_field(config, "enabled").to_string(),
                str_field(config, "name").to_string(),
            ]);
        });
        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}

pub struct GetConnectorConfigCmd {
    kind: ConnectorKind,
    key: String,
    version: Option<u64>,
    endpoint: ConnectorEndpoint,
}

impl GetConnectorConfigCmd {
    pub fn new(
        kind: ConnectorKind,
        key: String,
        version: Option<u64>,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            key,
            version,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for GetConnectorConfigCmd {
    fn explain(&self) -> String {
        match self.version {
            Some(version) => format!(
                "get configuration version {version} of {} {}",
                self.kind.singular(),
                self.key
            ),
            None => format!(
                "get active configuration of {} {}",
                self.kind.singular(),
                self.key
            ),
        }
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let path = match self.version {
            Some(version) => format!("{}/{version}", configs_path(self.kind, &self.key)),
            None => format!("{}/active", configs_path(self.kind, &self.key)),
        };
        let config = api.get_value(&path).await.with_context(|| {
            format!(
                "problem getting configuration of {} {}",
                self.kind.singular(),
                self.key
            )
        })?;

        let pretty = serde_json::to_string_pretty(&config)
            .with_context(|| "problem formatting configuration as JSON")?;
        event!(target: PRINT_TARGET, Level::INFO, "{pretty}");

        Ok(())
    }
}

pub struct GetConnectorPluginConfigCmd {
    kind: ConnectorKind,
    key: String,
    format: Option<String>,
    endpoint: ConnectorEndpoint,
}

impl GetConnectorPluginConfigCmd {
    pub fn new(
        kind: ConnectorKind,
        key: String,
        format: Option<String>,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            key,
            format,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for GetConnectorPluginConfigCmd {
    fn explain(&self) -> String {
        format!(
            "get plugin configuration of {} {}",
            self.kind.singular(),
            self.key
        )
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let path = format!("{}/plugin", configs_path(self.kind, &self.key));
        let query: Vec<(&str, String)> = self
            .format
            .clone()
            .map(|format| vec![("format", format)])
            .unwrap_or_default();
        let plugin = api.get_text(&path, &query).await.with_context(|| {
            format!(
                "problem getting plugin configuration of {} {}",
                self.kind.singular(),
                self.key
            )
        })?;

        event!(target: PRINT_TARGET, Level::INFO, "{plugin}");

        Ok(())
    }
}

pub struct CreateConnectorConfigCmd {
    kind: ConnectorKind,
    key: String,
    file: Option<PathBuf>,
    endpoint: ConnectorEndpoint,
}

impl CreateConnectorConfigCmd {
    pub fn new(
        kind: ConnectorKind,
        key: String,
        file: Option<PathBuf>,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            key,
            file,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for CreateConnectorConfigCmd {
    fn explain(&self) -> String {
        format!(
            "create configuration for {} {}",
            self.kind.singular(),
            self.key
        )
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let raw = match &self.file {
            Some(file) => std::fs::read_to_string(file).with_context(|| {
                format!("problem reading configuration file {}", file.display())
            })?,
            None => std::io::read_to_string(std::io::stdin())
                .with_context(|| "problem reading configuration from stdin")?,
        };
        let body: serde_json::Value =
            serde_json::from_str(&raw).with_context(|| "problem parsing configuration as JSON")?;

        let mut api = self.endpoint.connect()?;
        let created = api
            .post_json(&configs_path(self.kind, &self.key), &body)
            .await
            .with_context(|| {
                format!(
                    "problem creating configuration for {} {}",
                    self.kind.singular(),
                    self.key
                )
            })?;

        let pretty = serde_json::to_string_pretty(&created)
            .with_context(|| "problem formatting created configuration as JSON")?;
        event!(target: PRINT_TARGET, Level::INFO, "{pretty}");

        Ok(())
    }
}

pub struct DeleteConnectorConfigCmd {
    kind: ConnectorKind,
    key: String,
    version: Option<u64>,
    endpoint: ConnectorEndpoint,
}

impl DeleteConnectorConfigCmd {
    pub fn new(
        kind: ConnectorKind,
        key: String,
        version: Option<u64>,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            key,
            version,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for DeleteConnectorConfigCmd {
    fn explain(&self) -> String {
        match self.version {
            Some(version) => format!(
                "delete configuration version {version} of {} {}",
                self.kind.singular(),
                self.key
            ),
            None => format!(
                "delete active configuration of {} {}",
                self.kind.singular(),
                self.key
            ),
        }
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let query: Vec<(&str, String)> = self
            .version
            .map(|version| vec![("version", version.to_string())])
            .unwrap_or_default();
        api.delete(&configs_path(self.kind, &self.key), &query)
            .await
            .with_context(|| {
                format!(
                    "problem deleting configuration of {} {}",
                    self.kind.singular(),
                    self.key
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "configuration deleted successfully for {} {}", self.kind.singular(), self.key);

        Ok(())
    }
}

pub struct ActivateConnectorConfigCmd {
    kind: ConnectorKind,
    key: String,
    version: u64,
    endpoint: ConnectorEndpoint,
}

impl ActivateConnectorConfigCmd {
    pub fn new(
        kind: ConnectorKind,
        key: String,
        version: u64,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            key,
            version,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for ActivateConnectorConfigCmd {
    fn explain(&self) -> String {
        format!(
            "activate configuration version {} of {} {}",
            self.version,
            self.kind.singular(),
            self.key
        )
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let path = format!("{}/active", configs_path(self.kind, &self.key));
        let body = serde_json::json!({ "version": self.version });
        api.put_json(&path, &body).await.with_context(|| {
            format!(
                "problem activating configuration version {} of {} {}",
                self.version,
                self.kind.singular(),
                self.key
            )
        })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "configuration version {} activated for {} {}, takes effect on restart",
            self.version, self.kind.singular(), self.key);

        Ok(())
    }
}
