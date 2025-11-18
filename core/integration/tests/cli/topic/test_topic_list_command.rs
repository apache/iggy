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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, OutputFormat, TestHelpCmd,
    TestStreamId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicListCmd {
    stream_id: u32,
    stream_name: String,
    topic_name: String,
    using_stream_id: TestStreamId,
    output: OutputFormat,
}

impl TestTopicListCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_name: String,
        using_stream_id: TestStreamId,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_name,
            using_stream_id,
            output,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        args.extend(self.output.to_args().into_iter().map(String::from));

        args
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list topics from stream with ID: {} in {} mode",
                stream_id, self.output
            )))
            .stdout(contains(self.topic_name.clone()));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_name.clone().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            0,
            String::from("main"),
            String::from("sync"),
            TestStreamId::Named,
            OutputFormat::Default,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            1,
            String::from("customer"),
            String::from("topic"),
            TestStreamId::Named,
            OutputFormat::List,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicListCmd::new(
            2,
            String::from("production"),
            String::from("data"),
            TestStreamId::Named,
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "list", "--help"],
            format!(
                r#"List all topics in given stream ID

Stream ID can be specified as a stream name or ID

Examples
 iggy topic list 1
 iggy topic list prod

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list topics
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

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
            vec!["topic", "list", "-h"],
            format!(
                r#"List all topics in given stream ID

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to list topics

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
