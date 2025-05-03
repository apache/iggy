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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use keyring::Entry;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::fmt::{Display, Formatter, Result};

const IGGY_SERVICE: &str = "iggy";

#[derive(Debug)]
enum UsingToken {
    Value,
    Name,
}

impl Display for UsingToken {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            UsingToken::Value => write!(f, "--token"),
            UsingToken::Name => write!(f, "--token-name"),
        }
    }
}

#[derive(Debug)]
struct TestLoginOptions {
    token_name: String,
    token_value: Option<String>,
    using_token: UsingToken,
    keyring: Entry,
}

impl TestLoginOptions {
    fn new(token_name: String, using_token: UsingToken, server_address: String) -> Self {
        Self {
            token_name: token_name.clone(),
            token_value: None,
            using_token,
            keyring: Entry::new(
                format!("{IGGY_SERVICE}:{server_address}").as_str(),
                &token_name,
            )
            .unwrap_or_else(|_| {
                panic!(
                    "Failed to get keyring service data for {} service and {} token name",
                    IGGY_SERVICE, token_name
                )
            }),
        }
    }

    fn to_opts(&self) -> Vec<String> {
        match self.using_token {
            UsingToken::Value => vec![
                self.using_token.to_string(),
                self.token_value.clone().unwrap(),
            ],
            UsingToken::Name => vec![self.using_token.to_string(), self.token_name.clone()],
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginOptions {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let token = client
            .create_personal_access_token(&self.token_name, PersonalAccessTokenExpiry::NeverExpire)
            .await;
        assert!(token.is_ok());
        let token = token.unwrap();
        let token_value = token.token.clone();
        self.token_value = Some(token.token);
        self.keyring
            .set_password(token_value.as_str())
            .expect("Failed to set token");
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new().opts(self.to_opts()).arg("me")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing me command\n"))
            .stdout(contains(String::from("Transport | TCP")));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let token = client.delete_personal_access_token(&self.token_name).await;
        assert!(token.is_ok());

        if let Err(e) = self.keyring.delete_credential() {
            panic!("Failed to delete token from keyring due to {}", e);
        };
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    let server_address = iggy_cmd_test.get_tcp_server_address();
    assert!(server_address.is_some());
    let server_address = server_address.unwrap();
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(
            String::from("sample-token"),
            UsingToken::Value,
            server_address.clone(),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(
            String::from("access-token"),
            UsingToken::Name,
            server_address,
        ))
        .await;
}
