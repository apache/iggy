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

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use humantime::Duration as HumanDuration;
use humantime::format_duration;
use iggy::prelude::Client;
use predicates::str::starts_with;
use serial_test::parallel;
use std::time::Duration;

struct TestPatCreateCmd {
    name: String,
    expiry: Option<Vec<String>>,
}

impl TestPatCreateCmd {
    fn new(name: String, expiry: Option<Vec<String>>) -> Self {
        Self { name, expiry }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = vec![self.name.clone()];
        if let Some(expiry) = &self.expiry {
            args.extend(expiry.clone());
        }

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestPatCreateCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("pat")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let expiry = match &self.expiry {
            Some(value) => {
                let duration: Duration = *value.join(" ").parse::<HumanDuration>().unwrap();

                format!("token expire time: {}", format_duration(duration))
            }
            None => String::from("without token expire time"),
        };

        let message = format!(
            "Executing create personal access token with name: {} and {}\nPersonal access token with name: {} and {} created\nToken: ",
            self.name, expiry, self.name, expiry
        );

        command_state.success().stdout(starts_with(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let tokens = client.get_personal_access_tokens().await;

        assert!(tokens.is_ok());
        let tokens = tokens.unwrap();
        let token = tokens.into_iter().find(|t| t.name == self.name);
        assert!(token.is_some());
        let token = token.unwrap();
        if self.expiry.is_none() {
            assert!(token.expiry_at.is_none())
        }

        let delete = client.delete_personal_access_token(&self.name).await;
        assert!(delete.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestPatCreateCmd::new(String::from("main"), None))
        .await;
    iggy_cmd_test
        .execute_test(TestPatCreateCmd::new(
            String::from("client"),
            Some(vec!["1day".to_owned()]),
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestPatCreateCmd::new(
            String::from("sensor"),
            Some(vec!["3weeks".to_owned()]),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "create", "--help"],
            format!(
                r#"Create personal access token

Create personal access token which allow authenticating the clients using
a token, instead of the regular credentials (username and password)
In quiet mode only the personal access token name is printed

Examples
 iggy pat create name
 iggy pat create client 1day
 iggy pat create sensor 3weeks

{USAGE_PREFIX} pat create [OPTIONS] <NAME> [EXPIRY]...

Arguments:
  <NAME>
          Name of the personal access token

  [EXPIRY]...
          Personal access token expiry time in human-readable format
{CLAP_INDENT}
          Expiry time must be expressed in human-readable format like 15days 2min 2s ("none" or skipping parameter disables personal access token expiry)

Options:
  -s, --store-token
          Store token in an underlying platform-specific secure store
{CLAP_INDENT}
          Generated token is stored in a platform-specific secure storage without revealing its content to the user. It can be used to authenticate on iggy server using associated name and
          -n/--token-name command line option instead of -u/--username and -p/--password or -t/--token. In quiet mode only the token name is printed. This option can only be used for creating tokens
          which does not have expiry time set.

  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "create", "-h"],
            format!(
                r#"Create personal access token

{USAGE_PREFIX} pat create [OPTIONS] <NAME> [EXPIRY]...

Arguments:
  <NAME>       Name of the personal access token
  [EXPIRY]...  Personal access token expiry time in human-readable format

Options:
  -s, --store-token  Store token in an underlying platform-specific secure store
  -h, --help         Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
