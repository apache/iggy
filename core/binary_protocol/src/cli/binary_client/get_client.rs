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
use comfy_table::{Table, presets::ASCII_NO_BORDERS};
use iggy_common::get_client::GetClient;
use tracing::{Level, event};

pub struct GetClientCmd {
    get_client: GetClient,
}

impl GetClientCmd {
    pub fn new(client_id: u32) -> Self {
        Self {
            get_client: GetClient { client_id },
        }
    }
}

#[async_trait]
impl CliCommand for GetClientCmd {
    fn explain(&self) -> String {
        format!("get client with ID: {}", self.get_client.client_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let client_details = client
            .get_client(self.get_client.client_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting client with ID: {}",
                    self.get_client.client_id
                )
            })?;

        if client_details.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Client with ID: {} was not found", self.get_client.client_id);
            return Ok(());
        }

        let client_details = client_details.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Client ID",
            format!("{}", client_details.client_id).as_str(),
        ]);

        let user = match client_details.user_id {
            Some(user_id) => format!("{user_id}"),
            None => String::from("None"),
        };
        table.add_row(vec!["User ID", user.as_str()]);

        table.add_row(vec!["Address", client_details.address.as_str()]);
        table.add_row(vec!["Transport", client_details.transport.as_str()]);
        table.add_row(vec![
            "Consumer Groups Count",
            format!("{}", client_details.consumer_groups_count).as_str(),
        ]);

        if client_details.consumer_groups_count > 0 {
            let mut consumer_groups = Table::new();
            consumer_groups.load_preset(ASCII_NO_BORDERS);
            consumer_groups.set_header(vec!["Stream ID", "Topic ID", "Consumer Group ID"]);
            for consumer_group in client_details.consumer_groups {
                consumer_groups.add_row(vec![
                    format!("{}", consumer_group.stream_id).as_str(),
                    format!("{}", consumer_group.topic_id).as_str(),
                    format!("{}", consumer_group.group_id).as_str(),
                ]);
            }

            table.add_row(vec![
                "Consumer Groups Details",
                consumer_groups.to_string().as_str(),
            ]);
        }

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
