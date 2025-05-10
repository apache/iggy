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
use iggy_common::get_stream::GetStream;
use tracing::{Level, event};

pub struct GetStreamCmd {
    get_stream: GetStream,
}

impl GetStreamCmd {
    pub fn new(stream_id: Identifier) -> Self {
        Self {
            get_stream: GetStream { stream_id },
        }
    }
}

#[async_trait]
impl CliCommand for GetStreamCmd {
    fn explain(&self) -> String {
        format!("get stream with ID: {}", self.get_stream.stream_id)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let stream = client
            .get_stream(&self.get_stream.stream_id)
            .await
            .with_context(|| {
                format!(
                    "Problem getting stream with ID: {}",
                    self.get_stream.stream_id
                )
            })?;

        if stream.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Stream with ID: {} was not found", self.get_stream.stream_id);
            return Ok(());
        }

        let stream = stream.unwrap();
        let mut table = Table::new();

        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Stream ID", format!("{}", stream.id).as_str()]);
        table.add_row(vec!["Created", format!("{}", stream.created_at).as_str()]);
        table.add_row(vec!["Stream name", stream.name.as_str()]);
        table.add_row(vec!["Stream size", format!("{}", stream.size).as_str()]);
        table.add_row(vec![
            "Stream message count",
            format!("{}", stream.messages_count).as_str(),
        ]);
        table.add_row(vec![
            "Stream topics count",
            format!("{}", stream.topics_count).as_str(),
        ]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}
