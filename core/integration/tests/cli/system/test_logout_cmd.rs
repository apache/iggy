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

use crate::cli::common::{IggyCmdCommand, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::cli::system::session::ServerSession;
use iggy::client::Client;
use predicates::str::diff;

#[derive(Debug)]
pub(super) struct TestLogoutCmd {
    server_address: String,
}

impl TestLogoutCmd {
    pub(super) fn new(server_address: String) -> Self {
        Self { server_address }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLogoutCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(login_session.is_active());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().arg("logout")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(diff(format!(
            "Executing logout command\nSuccessfully logged out from Iggy server {}\n",
            self.server_address
        )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(!login_session.is_active());

        let pats = client.get_personal_access_tokens().await.unwrap();
        assert_eq!(pats.len(), 0);
    }
}
