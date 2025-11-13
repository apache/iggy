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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use predicates::str::diff;
use serial_test::parallel;

struct TestStreamCreateCmd {
    stream_id: Option<u32>,
    stream_name: String,
}

impl TestStreamCreateCmd {
    fn new(stream_id: Option<u32>, name: String) -> Self {
        Self {
            stream_id,
            stream_name: name,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = Vec::new();

        if let Some(stream_id) = self.stream_id {
            args.push("-s".to_string());
            args.push(format!("{stream_id}"));
        }

        args.push(self.stream_name.clone());

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStreamCreateCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("create")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = "ID auto incremented";

        let message = format!(
            "Executing create stream with name: {} and {}\nStream with name: {} and {} created\n",
            self.stream_name, stream_id, self.stream_name, stream_id
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream = client
            .get_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap().expect("Stream not found");
        assert_eq!(stream.name, self.stream_name);

        let delete = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
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
        .execute_test(TestStreamCreateCmd::new(None, String::from("main")))
        .await;

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamCreateCmd::new(None, String::from("prod")))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stream", "create", "--help"],
            format!(
                r#"Create stream with given name

If stream ID is not provided then the server will automatically assign it

Examples:
 iggy stream create prod
 iggy stream create -s 1 test

{USAGE_PREFIX} stream create [OPTIONS] <NAME>

Arguments:
  <NAME>
          Name of the stream

Options:
  -s, --stream-id <STREAM_ID>
          Stream ID to create

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
            vec!["stream", "create", "-h"],
            format!(
                r#"Create stream with given name

{USAGE_PREFIX} stream create [OPTIONS] <NAME>

Arguments:
  <NAME>  Name of the stream

Options:
  -s, --stream-id <STREAM_ID>  Stream ID to create
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
