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

use super::cell;
use super::client::{ConnectorEndpoint, ConnectorKind};
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::Client;
use tracing::{Level, event};

pub struct GetConnectorCmd {
    kind: ConnectorKind,
    key: String,
    endpoint: ConnectorEndpoint,
}

impl GetConnectorCmd {
    pub fn new(kind: ConnectorKind, key: String, endpoint: ConnectorEndpoint) -> Self {
        Self {
            kind,
            key,
            endpoint,
        }
    }
}

#[async_trait]
impl CliCommand for GetConnectorCmd {
    fn explain(&self) -> String {
        format!("get {} {}", self.kind.singular(), self.key)
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut api = self.endpoint.connect()?;
        let path = format!("/{}/{}", self.kind.path(), self.key);
        let details = api
            .get_value(&path)
            .await
            .with_context(|| format!("problem getting {} {}", self.kind.singular(), self.key))?;

        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);
        if let Some(object) = details.as_object() {
            object.iter().for_each(|(key, value)| {
                table.add_row(vec![key.clone(), cell(value)]);
            });
        }
        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
