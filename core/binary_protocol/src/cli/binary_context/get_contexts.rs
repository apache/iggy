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

use async_trait::async_trait;
use comfy_table::Table;
use tracing::{Level, event};

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};

use super::common::ContextManager;

pub enum GetContextsOutput {
    Table,
    List,
}

pub struct GetContextsCmd {
    output: GetContextsOutput,
}

impl GetContextsCmd {
    pub fn new(output: GetContextsOutput) -> Self {
        Self { output }
    }

    fn format_name(name: &str, active_context_key: &str) -> String {
        if name.eq(active_context_key) {
            format!("{}*", name)
        } else {
            name.to_string()
        }
    }
}

impl Default for GetContextsCmd {
    fn default() -> Self {
        GetContextsCmd {
            output: GetContextsOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetContextsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetContextsOutput::Table => "table",
            GetContextsOutput::List => "list",
        };
        format!("list contexts in {mode} mode")
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

        match self.output {
            GetContextsOutput::Table => {
                let mut table = Table::new();
                table.set_header(vec!["Name"]);

                contexts_map.iter().for_each(|(name, _)| {
                    let printed_name = GetContextsCmd::format_name(name, &active_context_key);
                    table.add_row(vec![printed_name]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetContextsOutput::List => contexts_map.iter().for_each(|(name, _)| {
                let printed_name = GetContextsCmd::format_name(name, &active_context_key);
                event!(target: PRINT_TARGET, Level::INFO, printed_name);
            }),
        }

        return Ok(());
    }
}
