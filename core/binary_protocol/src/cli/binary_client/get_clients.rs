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

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::get_clients::GetClients;
use tracing::{Level, event};

pub enum GetClientsOutput {
    Table,
    List,
}

pub struct GetClientsCmd {
    _get_clients: GetClients,
    output: GetClientsOutput,
}

impl GetClientsCmd {
    pub fn new(output: GetClientsOutput) -> Self {
        GetClientsCmd {
            _get_clients: GetClients {},
            output,
        }
    }
}

impl Default for GetClientsCmd {
    fn default() -> Self {
        GetClientsCmd {
            _get_clients: GetClients {},
            output: GetClientsOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetClientsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetClientsOutput::Table => "table",
            GetClientsOutput::List => "list",
        };
        format!("list clients in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let clients = client
            .get_clients()
            .await
            .with_context(|| String::from("Problem getting list of clients"))?;

        if clients.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No clients found!");
            return Ok(());
        }

        match self.output {
            GetClientsOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "Client ID",
                    "User ID",
                    "Address",
                    "Transport",
                    "Consumer Groups",
                ]);

                clients.iter().for_each(|client_info| {
                    table.add_row(vec![
                        format!("{}", client_info.client_id),
                        match client_info.user_id {
                            Some(user_id) => format!("{user_id}"),
                            None => String::from(""),
                        },
                        format!("{}", client_info.address),
                        format!("{}", client_info.transport),
                        format!("{}", client_info.consumer_groups_count),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetClientsOutput::List => {
                clients.iter().for_each(|client_info| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}|{}",
                        client_info.client_id,
                        match client_info.user_id {
                            Some(user_id) => format!("{user_id}"),
                            None => String::from(""),
                        },
                        client_info.address,
                        client_info.transport,
                        client_info.consumer_groups_count
                    );
                });
            }
        }

        Ok(())
    }
}
