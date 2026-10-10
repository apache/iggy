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
use tracing::{Level, event};

pub enum GetConnectorsOutput {
    Table,
    List,
}

pub struct GetConnectorsCmd {
    kind: ConnectorKind,
    output: GetConnectorsOutput,
    endpoint: ConnectorEndpoint,
}

impl GetConnectorsCmd {
    pub fn new(
        kind: ConnectorKind,
        output: GetConnectorsOutput,
        endpoint: ConnectorEndpoint,
    ) -> Self {
        Self {
            kind,
            output,
            endpoint,
        }
    }

    fn path(&self) -> String {
        format!("/{}", self.kind.path())
    }
}

#[async_trait]
impl CliCommand for GetConnectorsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetConnectorsOutput::Table => "table",
            GetConnectorsOutput::List => "list",
        };
        format!("list {} in {mode} mode", self.kind.path())
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let connectors = api
            .get_value(&self.path())
            .await
            .with_context(|| format!("problem getting list of {}", self.kind.path()))?;
        let connectors = connectors.as_array().cloned().unwrap_or_default();

        if connectors.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No {} found!", self.kind.path());
            return Ok(());
        }

        match self.output {
            GetConnectorsOutput::Table => {
                let mut table = Table::new();
                table.set_header(vec!["Key", "Name", "Status", "Enabled"]);
                connectors.iter().for_each(|connector| {
                    table.add_row(vec![
                        str_field(connector, "key").to_string(),
                        str_field(connector, "name").to_string(),
                        str_field(connector, "status").to_string(),
                        bool_field(connector, "enabled").to_string(),
                    ]);
                });
                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetConnectorsOutput::List => {
                connectors.iter().for_each(|connector| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}",
                        str_field(connector, "key"),
                        str_field(connector, "name"),
                        str_field(connector, "status"),
                        bool_field(connector, "enabled")
                    );
                });
            }
        }

        Ok(())
    }
}
