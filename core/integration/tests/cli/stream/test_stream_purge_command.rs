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
use iggy::prelude::*;
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;

struct TestStreamPurgeCmd {
    stream_id: u32,
    stream_name: String,
    using_identifier: TestStreamId,
    topic_name: String,
}

impl TestStreamPurgeCmd {
    fn new(stream_id: u32, name: String, using_identifier: TestStreamId) -> Self {
        Self {
            stream_id,
            stream_name: name,
            using_identifier,
            topic_name: String::from("test_topic"),
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
impl IggyCmdTestCase for TestStreamPurgeCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                10,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());

        let mut messages = (1..100)
            .map(|n| format!("message {n}"))
            .filter_map(|s| IggyMessage::from_str(s.as_str()).ok())
            .collect::<Vec<_>>();

        let send_status = client
            .send_messages(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
                &Partitioning::default(),
                &mut messages,
            )
            .await;
        assert!(send_status.is_ok());

        let stream_state = client
            .get_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream_state.is_ok());
        let stream_state = stream_state.unwrap().expect("Stream not found");
        assert!(stream_state.size > 0);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stream")
            .arg("purge")
            .arg(self.to_arg())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_identifier {
            TestStreamId::Named => self.stream_name.clone(),
            TestStreamId::Numeric => format!("{}", self.stream_id),
        };

        let start_message = format!(
            "Executing purge stream with ID: {stream_id}\nStream with ID: {stream_id} purged\n"
        );

        command_state.success().stdout(diff(start_message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let stream_state = client
            .get_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream_state.is_ok());
        let stream_state = stream_state.unwrap().expect("Stream not found");
        assert_eq!(stream_state.size, 0);

        let stream_delete = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream_delete.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStreamPurgeCmd::new(
            0,
            String::from("production"),
            TestStreamId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestStreamPurgeCmd::new(
            1,
            String::from("testing"),
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
            vec!["stream", "purge", "--help"],
            format!(
                r#"Purge all topics in given stream ID

Command removes all messages from given stream
Stream ID can be specified as a stream name or ID

Examples:
 iggy stream purge 1
 iggy stream purge test

{USAGE_PREFIX} stream purge <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to purge
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

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
            vec!["stream", "purge", "-h"],
            format!(
                r#"Purge all topics in given stream ID

{USAGE_PREFIX} stream purge <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to purge

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
