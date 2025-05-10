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
use anyhow::Context;
use async_trait::async_trait;
use tracing::{Level, event};

pub struct LogoutCmd {
    server_session: ServerSession,
}

impl LogoutCmd {
    pub fn new(server_address: String) -> Self {
        Self {
            server_session: ServerSession::new(server_address),
        }
    }
}

#[async_trait]
impl CliCommand for LogoutCmd {
    fn explain(&self) -> String {
        "logout command".to_owned()
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        if self.server_session.is_active() {
            client
                .delete_personal_access_token(&self.server_session.get_token_name())
                .await
                .with_context(|| {
                    format!(
                        "Problem deleting personal access token with name: {}",
                        self.server_session.get_token_name()
                    )
                })?;

            self.server_session.delete()?;
        }
        event!(target: PRINT_TARGET, Level::INFO, "Successfully logged out from Iggy server {}", self.server_session.get_server_address());

        Ok(())
    }
}
