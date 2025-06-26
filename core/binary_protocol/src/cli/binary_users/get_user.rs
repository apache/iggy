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
use iggy_common::Identifier;
use iggy_common::get_user::GetUser;
use tracing::{Level, event};

pub struct GetUserCmd {
    get_user: GetUser,
}

impl GetUserCmd {
    pub fn new(user_id: Identifier) -> Self {
        Self {
            get_user: GetUser { user_id },
        }
    }
}

#[async_trait]
impl CliCommand for GetUserCmd {
    fn explain(&self) -> String {
        format!("get user with ID: {}", self.get_user.user_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let user = client
            .get_user(&self.get_user.user_id)
            .await
            .with_context(|| format!("Problem getting user with ID: {}", self.get_user.user_id))?;

        if user.is_none() {
            event!(
                target: PRINT_TARGET,
                Level::INFO,
                "User with ID: {} was not found",
                self.get_user.user_id
            );
            return Ok(());
        }

        let user = user.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["User ID", format!("{}", user.id).as_str()]);
        table.add_row(vec![
            "Created",
            user.created_at
                .to_local_string("%Y-%m-%d %H:%M:%S")
                .as_str(),
        ]);
        table.add_row(vec!["Status", format!("{}", user.status).as_str()]);
        table.add_row(vec!["Username", user.username.as_str()]);

        if let Some(permissions) = user.permissions {
            let global_permissions: Table = permissions.global.into();
            table.add_row(vec!["Global", format!("{global_permissions}").as_str()]);

            if let Some(streams) = permissions.streams {
                streams.iter().for_each(|(stream_id, stream_permissions)| {
                    let stream_permissions: Table = stream_permissions.into();
                    table.add_row(vec![
                        format!("Stream: {stream_id}").as_str(),
                        format!("{stream_permissions}").as_str(),
                    ]);
                });
            }
        };

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
