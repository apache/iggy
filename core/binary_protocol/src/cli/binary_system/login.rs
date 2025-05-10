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
use crate::cli::binary_system::session::ServerSession;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use crate::cli::utils::login_session_expiry::LoginSessionExpiry;
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::SEC_IN_MICRO;
use tracing::{Level, event};

const DEFAULT_LOGIN_SESSION_TIMEOUT: u64 = SEC_IN_MICRO * 15 * 60;

pub struct LoginCmd {
    server_session: ServerSession,
    login_session_expiry: Option<LoginSessionExpiry>,
}

impl LoginCmd {
    pub fn new(server_address: String, login_session_expiry: Option<LoginSessionExpiry>) -> Self {
        Self {
            server_session: ServerSession::new(server_address),
            login_session_expiry,
        }
    }
}

#[async_trait]
impl CliCommand for LoginCmd {
    fn explain(&self) -> String {
        "login command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        if self.server_session.is_active() {
            event!(target: PRINT_TARGET, Level::INFO, "Already logged into Iggy server {}", self.server_session.get_server_address());
            return Ok(());
        }

        let tokens = client.get_personal_access_tokens().await.with_context(|| {
            format!(
                "Problem getting personal access tokens from server: {}",
                self.server_session.get_server_address()
            )
        })?;

        // If local keyring is empty and server has the token for login session, then something
        // went wrong, and we should delete the token from the server and recreate login session
        // from scratch - token on the server and local keyring should be in sync.
        if let Some(token) = tokens
            .iter()
            .find(|pat| pat.name == self.server_session.get_token_name())
        {
            client
                .delete_personal_access_token(&token.name)
                .await
                .with_context(|| {
                    format!(
                        "Problem deleting old personal access token with name: {}",
                        self.server_session.get_token_name()
                    )
                })?;
        }

        let token = client
            .create_personal_access_token(
                &self.server_session.get_token_name(),
                match &self.login_session_expiry {
                    None => Some(DEFAULT_LOGIN_SESSION_TIMEOUT).into(),
                    Some(value) => *value,
                },
            )
            .await
            .with_context(|| {
                format!(
                    "Problem creating personal access token with name: {}",
                    self.server_session.get_token_name()
                )
            })?;

        self.server_session.store(&token.token)?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Successfully logged into Iggy server {}",
            self.server_session.get_server_address(),
        );

        Ok(())
    }
}
