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

use crate::commands::binary_system::session::ServerSession;
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::Client;
use tracing::{Level, event};

pub struct SessionStatusCmd {
    server_session: ServerSession,
}

impl SessionStatusCmd {
    pub fn new(server_address: String) -> Self {
        Self {
            server_session: ServerSession::new(server_address),
        }
    }
}

#[async_trait]
impl CliCommand for SessionStatusCmd {
    fn explain(&self) -> String {
        "session status command".to_owned()
    }

    fn login_required(&self) -> bool {
        false
    }

    fn connection_required(&self) -> bool {
        false
    }

    async fn execute_cmd(&mut self, _client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let is_active = self.server_session.is_active();
        let server_address = self.server_session.get_server_address();

        let mut table = Table::new();
        table.set_header(vec!["Property", "Value"]);
        table.add_row(vec!["Server Address", server_address]);
        let active = if is_active {
            "Yes (token presence only, freshness not verified)"
        } else {
            "No"
        };
        table.add_row(vec!["Session Active", active]);

        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_explain_message() {
        let cmd = SessionStatusCmd::new("127.0.0.1:8090".to_string());
        assert_eq!(cmd.explain(), "session status command");
    }

    #[test]
    fn should_not_require_login() {
        let cmd = SessionStatusCmd::new("127.0.0.1:8090".to_string());
        assert!(!cmd.login_required());
    }

    #[test]
    fn should_not_require_connection() {
        let cmd = SessionStatusCmd::new("127.0.0.1:8090".to_string());
        assert!(!cmd.connection_required());
    }
}
