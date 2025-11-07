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
use iggy_common::PersonalAccessTokenExpiry;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use keyring::Entry;
use tracing::{Level, event};

pub struct CreatePersonalAccessTokenCmd {
    create_token: CreatePersonalAccessToken,
    token_expiry: Option<PersonalAccessTokenExpiry>,
    quiet_mode: bool,
    store_token: bool,
    server_address: String,
}

impl CreatePersonalAccessTokenCmd {
    pub fn new(
        name: String,
        pat_expiry: Option<PersonalAccessTokenExpiry>,
        quiet_mode: bool,
        store_token: bool,
        server_address: String,
    ) -> Self {
        Self {
            create_token: CreatePersonalAccessToken {
                name,
                expiry: match &pat_expiry {
                    None => PersonalAccessTokenExpiry::NeverExpire,
                    Some(value) => *value,
                },
            },
            token_expiry: pat_expiry,
            quiet_mode,
            store_token,
            server_address,
        }
    }
}

#[async_trait]
impl CliCommand for CreatePersonalAccessTokenCmd {
    fn explain(&self) -> String {
        let expiry_text = match &self.token_expiry {
            Some(value) => format!("token expire time: {value}"),
            None => String::from("without token expire time"),
        };
        format!(
            "create personal access token with name: {} and {}",
            self.create_token.name, expiry_text
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let token = client
            .create_personal_access_token(&self.create_token.name, self.create_token.expiry)
            .await
            .with_context(|| {
                format!(
                    "Problem creating personal access token with name: {}",
                    self.create_token.name
                )
            })?;

        if self.store_token {
            let server_address = format!("iggy:{}", self.server_address);
            let entry = Entry::new(&server_address, &self.create_token.name)?;
            entry.set_password(&token.token)?;
            event!(target: PRINT_TARGET, Level::DEBUG,"Stored token under service: {} and name: {}", server_address,
                    self.create_token.name);
            event!(target: PRINT_TARGET, Level::INFO,
                "Personal access token with name: {} and {} created",
                self.create_token.name,
                match &self.token_expiry {
                    Some(value) => format!("token expire time: {value}"),
                    None => String::from("without token expire time"),
                },
            );
        } else if self.quiet_mode {
            println!("{}", token.token);
        } else {
            event!(target: PRINT_TARGET, Level::INFO,
                "Personal access token with name: {} and {} created",
                self.create_token.name,
                match &self.token_expiry {
                    Some(value) => format!("token expire time: {value}"),
                    None => String::from("without token expire time"),
                },
            );
            event!(target: PRINT_TARGET, Level::INFO,"Token: {}",
                            token.token);
        }

        Ok(())
    }
}
