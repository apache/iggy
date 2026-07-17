// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::commands::binary_system::session::ServerSession;
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use crate::commands::utils::login_session_expiry::LoginSessionExpiry;
use anyhow::Context;
use async_trait::async_trait;
use iggy_common::Client;
use iggy_common::SEC_IN_MICRO;
use iggy_common::{IggyTimestamp, PersonalAccessTokenInfo};
use tracing::{Level, event};

const DEFAULT_LOGIN_SESSION_TIMEOUT: u64 = SEC_IN_MICRO * 15 * 60;

/// A stored login session is dead once the server-side PAT backing it is gone
/// or past its expiry. The server lists expired tokens until a background
/// cleaner prunes them, so a freshly expired session is caught here rather than
/// waiting for the token to disappear from the listing.
fn session_pat_expired(pat: &PersonalAccessTokenInfo) -> bool {
    match pat.expiry_at {
        None => false,
        Some(expiry) => expiry.as_micros() <= IggyTimestamp::now().as_micros(),
    }
}

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

    fn prefer_explicit_credentials(&self) -> bool {
        true
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let tokens = client.get_personal_access_tokens().await.with_context(|| {
            format!(
                "Problem getting personal access tokens from server: {}",
                self.server_session.get_server_address()
            )
        })?;

        let server_token = tokens
            .iter()
            .find(|pat| pat.name == self.server_session.get_token_name());

        // A local keyring entry proves a token was stored once, not that it
        // still authenticates. Report "already logged in" only when the server
        // still holds a matching, unexpired PAT; a keyring entry left behind by
        // an expired session is dropped so login recreates it below.
        if self.server_session.is_active() {
            if server_token.is_some_and(|pat| !session_pat_expired(pat)) {
                event!(target: PRINT_TARGET, Level::INFO, "Already logged into Iggy server {}", self.server_session.get_server_address());
                return Ok(());
            }
            self.server_session.delete()?;
        }

        // Local keyring is empty (or was just cleared for a dead session). If the
        // server still has the token for the login session, delete it so token on
        // the server and local keyring stay in sync, then recreate from scratch.
        if let Some(token) = server_token {
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
