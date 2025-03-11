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
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use tracing::{event, Level};

pub enum GetPersonalAccessTokensOutput {
    Table,
    List,
}

pub struct GetPersonalAccessTokensCmd {
    _get_tokens: GetPersonalAccessTokens,
    output: GetPersonalAccessTokensOutput,
}

impl GetPersonalAccessTokensCmd {
    pub fn new(output: GetPersonalAccessTokensOutput) -> Self {
        Self {
            _get_tokens: GetPersonalAccessTokens {},
            output,
        }
    }
}

#[async_trait]
impl CliCommand for GetPersonalAccessTokensCmd {
    fn explain(&self) -> String {
        let mode = match self.output {
            GetPersonalAccessTokensOutput::Table => "table",
            GetPersonalAccessTokensOutput::List => "list",
        };
        format!("list personal access tokens in {mode} mode")
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let tokens = client
            .get_personal_access_tokens()
            .await
            .with_context(|| String::from("Problem getting list of personal access tokens"))?;

        match self.output {
            GetPersonalAccessTokensOutput::Table => {
                let mut table = Table::new();

                table.set_header(vec!["Name", "Token Expiry Time"]);

                tokens.iter().for_each(|token| {
                    table.add_row(vec![
                        format!("{}", token.name.clone()),
                        match token.expiry_at {
                            None => String::from("unlimited"),
                            Some(value) => value.to_local_string("%Y-%m-%d %H:%M:%S"),
                        },
                    ]);
                });

                event!(target: PRINT_TARGET, Level::INFO, "{table}");
            }
            GetPersonalAccessTokensOutput::List => {
                tokens.iter().for_each(|token| {
                    event!(target: PRINT_TARGET, Level::INFO,
                        "{}|{}",
                        token.name,
                        match token.expiry_at {
                            None => String::from("unlimited"),
                            Some(value) => value.to_local_string("%Y-%m-%d %H:%M:%S"),
                        },
                    );
                });
            }
        }

        Ok(())
    }
}
