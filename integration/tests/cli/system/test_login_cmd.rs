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
use iggy::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use predicates::str::diff;

#[derive(Debug)]
pub enum TestLoginCmdType {
    Success,
    SuccessWithTimeout(u64),
    AlreadyLoggedIn,
    AlreadyLoggedInWithToken,
}

#[derive(Debug)]
pub(super) struct TestLoginCmd {
    server_address: String,
    login_type: TestLoginCmdType,
}

impl TestLoginCmd {
    pub(super) fn new(server_address: String, login_type: TestLoginCmdType) -> Self {
        Self {
            server_address,
            login_type,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        match self.login_type {
            TestLoginCmdType::Success | TestLoginCmdType::SuccessWithTimeout(_) => {
                assert!(!login_session.is_active());

                let pats = client.get_personal_access_tokens().await.unwrap();
                assert_eq!(pats.len(), 0);
            }
            TestLoginCmdType::AlreadyLoggedIn => {
                assert!(login_session.is_active());

                let pats = client.get_personal_access_tokens().await.unwrap();
                assert_eq!(pats.len(), 1);
            }
            TestLoginCmdType::AlreadyLoggedInWithToken => {
                // Local keyring must be empty
                assert!(!login_session.is_active());

                let pat = client
                    .create_personal_access_token(
                        &login_session.get_token_name(),
                        PersonalAccessTokenExpiry::NeverExpire,
                    )
                    .await;
                assert!(pat.is_ok());
            }
        }
    }

    fn get_command(&self) -> IggyCmdCommand {
        let command = IggyCmdCommand::new().with_cli_credentials().arg("login");

        if let TestLoginCmdType::SuccessWithTimeout(timeout) = self.login_type {
            command.arg(format!("{timeout}s"))
        } else {
            command
        }
    }

    fn verify_command(&self, command_state: Assert) {
        match self.login_type {
            TestLoginCmdType::Success
            | TestLoginCmdType::AlreadyLoggedInWithToken
            | TestLoginCmdType::SuccessWithTimeout(_) => {
                command_state.success().stdout(diff(format!(
                    "Executing login command\nSuccessfully logged into Iggy server {}\n",
                    self.server_address
                )));
            }
            TestLoginCmdType::AlreadyLoggedIn => {
                command_state.success().stdout(diff(format!(
                    "Executing login command\nAlready logged into Iggy server {}\n",
                    self.server_address
                )));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let login_session = ServerSession::new(self.server_address.clone());
        assert!(login_session.is_active());

        let pats = client.get_personal_access_tokens().await.unwrap();
        assert_eq!(pats.len(), 1);
        assert_eq!(pats[0].name, login_session.get_token_name());
    }
}
