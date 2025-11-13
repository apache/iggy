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

struct TestStreamDeleteCmd {
    stream_id: u32,
    stream_name: String,
    using_identifier: TestStreamId,
}

impl TestStreamDeleteCmd {
    fn new(stream_id: u32, name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            stream_name: name,
            using_identifier,
        }
    }

    fn to_arg(&self) -> String {
        match self.using_identifier {
            TestStreamId::Named => self.stream_name.clone(),
            TestStreamId::Numeric => format!("{}", self.stream_id),
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamDeleteCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("delete")
            .arg(self.to_arg())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = match self.using_identifier {
            TestStreamId::Named => format!(
                "Executing delete stream with ID: {}\nStream with ID: {} deleted\n",
                self.stream_name, self.stream_name
            ),
            TestStreamId::Numeric => format!(
                "Executing delete stream with ID: {}\nStream with ID: {} deleted\n",
                self.stream_id, self.stream_id
            ),
        };

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let streams = client.get_streams().await;
        assert!(streams.is_ok());
        let streams = streams.unwrap();
        assert_eq!(streams.len(), 0);
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamDeleteCmd::new(
            0,
            String::from("testing"),
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamDeleteCmd::new(
            0,
            String::from("production"),
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
            vec!["stream", "delete", "--help"],
            format!(
                r"Delete stream with given ID

Stream ID can be specified as a stream name or ID

Examples:
 iggy stream delete 1
 iggy stream delete test

{USAGE_PREFIX} stream delete <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to delete
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

Options:
  -h, --help
          Print help (see a summary with '-h')
",
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
            vec!["stream", "delete", "-h"],
            format!(
                r#"Delete stream with given ID

{USAGE_PREFIX} stream delete <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to delete

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
