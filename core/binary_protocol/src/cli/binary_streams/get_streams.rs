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
use iggy_common::get_streams::GetStreams;
use tracing::{Level, event};

pub enum GetStreamsOutput {
    Table,
    List,
}

pub struct GetStreamsCmd {
    _get_streams: GetStreams,
    output: GetStreamsOutput,
}

impl GetStreamsCmd {
    pub fn new(output: GetStreamsOutput) -> Self {
        GetStreamsCmd {
            _get_streams: GetStreams {},
            output,
        }
    }
}

impl Default for GetStreamsCmd {
    fn default() -> Self {
        GetStreamsCmd {
            _get_streams: GetStreams {},
            output: GetStreamsOutput::Table,
        }
    }
}

#[async_trait]
impl CliCommand for GetStreamsCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetStreamsOutput::Table => "table",
            GetStreamsOutput::List => "list",
        };
        format!("list streams in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let streams = client
            .get_streams()
            .await
            .with_context(|| String::from("Problem getting list of streams"))?;

        if streams.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO, "No streams found!");
            return Ok(());
        }

        match self.output {
            GetStreamsOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec![
                    "ID", "Created", "Name", "Size (B)", "Messages", "Topics",
                ]);

                streams.iter().for_each(|stream| {
                    table.add_row(vec![
                        format!("{}", stream.id),
                        format!("{}", stream.created_at),
                        stream.name.clone(),
                        format!("{}", stream.size),
                        format!("{}", stream.messages_count),
                        format!("{}", stream.topics_count),
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetStreamsOutput::List => {
                streams.iter().for_each(|stream| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}|{}|{}|{}|{}",
                        stream.id,
                        stream.created_at,
                        stream.name,
                        stream.size,
                        stream.messages_count,
                        stream.topics_count
                    );
                });
            }
        }

        Ok(())
    }
}
