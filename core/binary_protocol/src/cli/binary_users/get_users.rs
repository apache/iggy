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

use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::Client;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::get_users::GetUsers;
use tracing::{event, Level};

pub enum GetUsersOutput {
    Table,
    List,
}

pub struct GetUsersCmd {
    _get_users: GetUsers,
    output: GetUsersOutput,
}

impl GetUsersCmd {
    pub fn new(output: GetUsersOutput) -> Self {
        GetUsersCmd {
            _get_users: GetUsers {},
            output,
        }
    }
}

impl Default for GetUsersCmd {
    fn default() -> Self {
        GetUsersCmd {
            _get_users: GetUsers {},
            output: GetUsersOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetUsersCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetUsersOutput::Table => "table",
            GetUsersOutput::List => "list",
        };
        format!("list users in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let users = client
            .get_users()
            .await
            .with_context(|| String::from("Problem getting list of users"))?;

        if users.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No users found!");
            return Ok(());
        }

        match self.output {
            GetUsersOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec!["ID", "Created", "Status", "Username"]);

                users.iter().for_each(|user| {
                    table.add_row(vec![
                        format!("{}", user.id),
                        user.created_at.to_local_string("%Y-%m-%d %H:%M:%S"),
                        user.status.clone().to_string(),
                        user.username.clone(),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetUsersOutput::List => {
                users.iter().for_each(|user| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}",
                        user.id,
                        user.created_at.to_local_string("%Y-%m-%d %H:%M:%S"),
                        user.status.clone().to_string(),
                        user.username.clone(),
                    );
                });
            }
        }

        Ok(())
    }
}
