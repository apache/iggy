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
use iggy::prelude::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::prelude::Client;
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
