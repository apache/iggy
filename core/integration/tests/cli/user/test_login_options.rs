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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::PersonalAccessTokenExpiry;
use iggy::prelude::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use predicates::str::{contains, starts_with};
use serial_test::parallel;

#[derive(Debug, Default)]
enum UseCredentials {
    #[default]
    CliOptions,
    StdinInput,
}

#[derive(Debug, Default)]
struct TestLoginOptions {
    use_credentials: UseCredentials,
}

impl TestLoginOptions {
    fn new(use_credentials: UseCredentials) -> Self {
        Self { use_credentials }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginOptions {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        match self.use_credentials {
            UseCredentials::CliOptions => IggyCmdCommand::new().with_cli_credentials().arg("me"),
            UseCredentials::StdinInput => IggyCmdCommand::new()
                .opt("--username")
                .opt(DEFAULT_ROOT_USERNAME)
                .arg("me"),
        }
    }

    fn provide_stdin_input(&self) -> Option<Vec<String>> {
        match self.use_credentials {
            UseCredentials::StdinInput => Some(vec![DEFAULT_ROOT_PASSWORD.to_string()]),
            _ => None,
        }
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with("Executing me command\n"))
            .stdout(contains(String::from("Transport | TCP")));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(UseCredentials::CliOptions))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptions::new(UseCredentials::StdinInput))
        .await;
}

#[derive(Debug)]
enum EnvTokenScenario {
    // Only IGGY_TOKEN is set, holding a valid personal access token.
    TokenOnly,
    // IGGY_TOKEN holds an invalid token and --token a valid one: the flag wins.
    FlagTokenOverridesEnvToken,
    // IGGY_TOKEN holds a valid token and the IGGY_USERNAME/IGGY_PASSWORD pair
    // a wrong password: the token wins.
    EnvTokenOverridesEnvCredentials,
    // IGGY_TOKEN holds an invalid token and the pair is valid: the token
    // still wins, so the login fails.
    InvalidEnvTokenOverridesEnvCredentials,
}

#[derive(Debug)]
struct TestLoginOptionsWithEnvToken {
    scenario: EnvTokenScenario,
    token_name: String,
    token_value: Option<String>,
}

impl TestLoginOptionsWithEnvToken {
    fn new(scenario: EnvTokenScenario, token_name: &str) -> Self {
        Self {
            scenario,
            token_name: token_name.to_string(),
            token_value: None,
        }
    }

    fn needs_valid_token(&self) -> bool {
        !matches!(
            self.scenario,
            EnvTokenScenario::InvalidEnvTokenOverridesEnvCredentials
        )
    }
}

#[async_trait]
impl IggyCmdTestCase for TestLoginOptionsWithEnvToken {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        if self.needs_valid_token() {
            let token = client
                .create_personal_access_token(
                    &self.token_name,
                    PersonalAccessTokenExpiry::NeverExpire,
                )
                .await;
            assert!(token.is_ok());
            self.token_value = Some(token.unwrap().token);
        }
    }

    fn get_command(&self) -> IggyCmdCommand {
        let valid_token = self.token_value.clone().unwrap_or_default();
        match self.scenario {
            EnvTokenScenario::TokenOnly => IggyCmdCommand::new()
                .env("IGGY_TOKEN", valid_token)
                .arg("me"),
            EnvTokenScenario::FlagTokenOverridesEnvToken => IggyCmdCommand::new()
                .env("IGGY_TOKEN", "invalid-token")
                .opts(vec!["--token".to_string(), valid_token])
                .arg("me"),
            EnvTokenScenario::EnvTokenOverridesEnvCredentials => IggyCmdCommand::new()
                .env("IGGY_TOKEN", valid_token)
                .env("IGGY_USERNAME", DEFAULT_ROOT_USERNAME)
                .env("IGGY_PASSWORD", "wrong-password")
                .arg("me"),
            EnvTokenScenario::InvalidEnvTokenOverridesEnvCredentials => IggyCmdCommand::new()
                .env("IGGY_TOKEN", "invalid-token")
                .env("IGGY_USERNAME", DEFAULT_ROOT_USERNAME)
                .env("IGGY_PASSWORD", DEFAULT_ROOT_PASSWORD)
                .arg("me"),
        }
    }

    fn verify_command(&self, command_state: Assert) {
        match self.scenario {
            EnvTokenScenario::InvalidEnvTokenOverridesEnvCredentials => {
                command_state.failure();
            }
            _ => {
                command_state
                    .success()
                    .stdout(starts_with("Executing me command\n"))
                    .stdout(contains(String::from("Transport | TCP")));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if self.needs_valid_token() {
            let token = client.delete_personal_access_token(&self.token_name).await;
            assert!(token.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_resolve_iggy_token_with_documented_precedence() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestLoginOptionsWithEnvToken::new(
            EnvTokenScenario::TokenOnly,
            "env-token-only",
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptionsWithEnvToken::new(
            EnvTokenScenario::FlagTokenOverridesEnvToken,
            "env-token-flag-override",
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptionsWithEnvToken::new(
            EnvTokenScenario::EnvTokenOverridesEnvCredentials,
            "env-token-pair-override",
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestLoginOptionsWithEnvToken::new(
            EnvTokenScenario::InvalidEnvTokenOverridesEnvCredentials,
            "env-token-invalid",
        ))
        .await;
}
