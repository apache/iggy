/* Licensed to the Apache Software Foundation (ASF) under one
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

use anyhow::bail;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{Level, event};

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use iggy_common::Client;

use super::common::{ContextConfig, ContextManager};

const MASKED_VALUE: &str = "********";

pub struct ShowContextCmd {
    context_name: String,
}

impl ShowContextCmd {
    pub fn new(context_name: String) -> Self {
        Self { context_name }
    }

    fn build_table(name: &str, is_active: bool, config: &ContextConfig) -> Table {
        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);

        let display_name = if is_active {
            format!("{name}*")
        } else {
            name.to_string()
        };
        table.add_row(vec!["Name", &display_name]);

        if let Some(ref transport) = config.iggy.transport {
            table.add_row(vec!["Transport", transport]);
        }
        if let Some(ref addr) = config.iggy.tcp_server_address {
            table.add_row(vec!["TCP Server Address", addr]);
        }
        if let Some(ref url) = config.iggy.http_api_url {
            table.add_row(vec!["HTTP API URL", url]);
        }
        if let Some(ref addr) = config.iggy.quic_server_address {
            table.add_row(vec!["QUIC Server Address", addr]);
        }
        if let Some(tls) = config.iggy.tcp_tls_enabled {
            table.add_row(vec!["TCP TLS Enabled", &tls.to_string()]);
        }
        if let Some(ref username) = config.username {
            table.add_row(vec!["Username", username]);
        }
        if config.password.is_some() {
            table.add_row(vec!["Password", MASKED_VALUE]);
        }
        if config.token.is_some() {
            table.add_row(vec!["Token", MASKED_VALUE]);
        }
        if let Some(ref token_name) = config.token_name {
            table.add_row(vec!["Token Name", token_name]);
        }

        table
    }
}

#[async_trait]
impl CliCommand for ShowContextCmd {
    fn explain(&self) -> String {
        format!("show context \"{}\"", self.context_name)
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut context_mgr = ContextManager::default();
        let contexts_map = context_mgr.get_contexts().await?;
        let active_context_key = context_mgr.get_active_context_key().await?;

        let config = match contexts_map.get(&self.context_name) {
            Some(config) => config,
            None => bail!("context '{}' not found", self.context_name),
        };

        let is_active = self.context_name == active_context_key;
        let table = Self::build_table(&self.context_name, is_active, config);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
