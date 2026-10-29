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

use super::client::ConnectorEndpoint;
use super::{bool_field, cell, str_field};
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::Client;
use tracing::{Level, event};

pub struct GetConnectorStatsCmd {
    endpoint: ConnectorEndpoint,
}

impl GetConnectorStatsCmd {
    pub fn new(endpoint: ConnectorEndpoint) -> Self {
        Self { endpoint }
    }
}

#[async_trait]
impl CliCommand for GetConnectorStatsCmd {
    fn explain(&self) -> String {
        "get connectors runtime statistics".to_string()
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let stats = api
            .get_value("/stats")
            .await
            .with_context(|| "problem getting connectors runtime statistics")?;

        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);
        for key in [
            "connectors_runtime_version",
            "process_id",
            "cpu_usage",
            "total_cpu_usage",
            "memory_usage",
            "total_memory",
            "available_memory",
            "run_time",
            "start_time",
            "sources_total",
            "sources_running",
            "sinks_total",
            "sinks_running",
        ] {
            if let Some(value) = stats.get(key) {
                table.add_row(vec![key.to_string(), cell(value)]);
            }
        }
        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        let connectors = stats
            .get("connectors")
            .and_then(|connectors| connectors.as_array())
            .cloned()
            .unwrap_or_default();
        if connectors.is_empty() {
            return Ok(());
        }

        let mut table = Table::new();
        table.set_header(vec![
            "Key",
            "Type",
            "Status",
            "Enabled",
            "Produced",
            "Sent",
            "Consumed",
            "Processed",
            "Filtered",
            "Errors",
        ]);
        connectors.iter().for_each(|connector| {
            table.add_row(vec![
                str_field(connector, "key").to_string(),
                str_field(connector, "connector_type").to_string(),
                str_field(connector, "status").to_string(),
                bool_field(connector, "enabled").to_string(),
                connector
                    .get("messages_produced")
                    .map(cell)
                    .unwrap_or_default(),
                connector.get("messages_sent").map(cell).unwrap_or_default(),
                connector
                    .get("messages_consumed")
                    .map(cell)
                    .unwrap_or_default(),
                connector
                    .get("messages_processed")
                    .map(cell)
                    .unwrap_or_default(),
                connector
                    .get("messages_filtered")
                    .map(cell)
                    .unwrap_or_default(),
                connector.get("errors").map(cell).unwrap_or_default(),
            ]);
        });
        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
