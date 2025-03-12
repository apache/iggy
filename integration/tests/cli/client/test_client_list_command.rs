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
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd, CLAP_INDENT,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestClientListCmd {
    output: OutputFormat,
    client_id: Option<u32>,
}

impl TestClientListCmd {
    fn new(output: OutputFormat) -> Self {
        Self {
            output,
            client_id: None,
        }
    }

    fn to_args(&self) -> Vec<&str> {
        self.output.to_args()
    }
}

#[async_trait]
impl IggyCmdTestCase for TestClientListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let client_info = client.get_me().await;
        assert!(client_info.is_ok());
        self.client_id = Some(client_info.unwrap().client_id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("client")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list clients in {} mode",
                self.output
            )))
            .stdout(contains(format!("{}", self.client_id.unwrap_or(0))));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestClientListCmd::new(OutputFormat::Default))
        .await;
    iggy_cmd_test
        .execute_test(TestClientListCmd::new(OutputFormat::List))
        .await;
    iggy_cmd_test
        .execute_test(TestClientListCmd::new(OutputFormat::Table))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["client", "list", "--help"],
            format!(
                r#"List all currently connected clients to iggy server

Clients shall not to be confused with the users

Examples:
 iggy client list
 iggy client list --list-mode table
 iggy client list -l table

{USAGE_PREFIX} client list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

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
            vec!["client", "list", "-h"],
            format!(
                r#"List all currently connected clients to iggy server

{USAGE_PREFIX} client list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
