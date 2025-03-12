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

use crate::cli_command::{CliCommand, PRINT_TARGET};
use crate::client::Client;
use crate::system::get_me::GetMe;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub struct GetMeCmd {
    _get_me: GetMe,
}

impl GetMeCmd {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for GetMeCmd {
    fn default() -> Self {
        Self { _get_me: GetMe {} }
    }
}

#[async_trait]
impl CliCommand for GetMeCmd {
    fn explain(&self) -> String {
        "me command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let client_info = client
            .get_me()
            .await
            .with_context(|| "Problem sending get_me command".to_owned())?;

        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec![
            "Client ID",
            format!("{}", client_info.client_id).as_str(),
        ]);
        if let Some(user_id) = client_info.user_id {
            table.add_row(vec!["User ID", format!("{}", user_id).as_str()]);
        }
        table.add_row(vec!["Address", client_info.address.as_str()]);
        table.add_row(vec!["Transport", client_info.transport.as_str()]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
