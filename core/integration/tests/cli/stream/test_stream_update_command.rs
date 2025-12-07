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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use predicates::str::diff;
use serial_test::parallel;

struct TestStreamUpdateCmd {
    stream_id: u32,
    stream_name: String,
    new_name: String,
    using_identifier: TestStreamId,
}

impl TestStreamUpdateCmd {
    fn new(stream_id: u32, name: String, new_name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            stream_name: name,
            new_name,
            using_identifier,
        }
    }

    fn to_args(&self) -> Vec<String> {
        match self.using_identifier {
            TestStreamId::Named => vec![self.stream_name.clone(), self.new_name.clone()],
            TestStreamId::Numeric => {
                vec![format!("{}", self.stream_id), self.new_name.clone()]
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamUpdateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("update")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestStreamId::Named => format!(
                "Executing update stream with ID: {} and name: {}\nStream with ID: {} updated name: {}\n",
                self.stream_name, self.new_name, self.stream_name, self.new_name
            ),
            TestStreamId::Numeric => format!(
                "Executing update stream with ID: {} and name: {}\nStream with ID: {} updated name: {}\n",
                self.stream_id, self.new_name, self.stream_id, self.new_name
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream = client
            .get_stream(&self.new_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap().expect("Stream not found");
        assert_eq!(stream.name, self.new_name);

        let delete = client
            .delete_stream(&self.new_name.clone().try_into().unwrap())
            .await;
        assert!(delete.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamUpdateCmd::new(
            0,
            String::from("testing"),
            String::from("development"),
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamUpdateCmd::new(
            0,
            String::from("production"),
            String::from("prototype"),
            TestStreamId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "update", "--help"],
            format!(
                r#"Update stream name for given stream ID

Stream ID can be specified as a stream name or ID

Examples:
 iggy stream update 1 production
 iggy stream update test development

{USAGE_PREFIX} stream update <STREAM_ID> <NAME>

Arguments:
  <STREAM_ID>
          Stream ID to update
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <NAME>
          New name for the stream

Options:
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
            vec!["stream", "update", "-h"],
            format!(
                r#"Update stream name for given stream ID

{USAGE_PREFIX} stream update <STREAM_ID> <NAME>

Arguments:
  <STREAM_ID>  Stream ID to update
  <NAME>       New name for the stream

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
