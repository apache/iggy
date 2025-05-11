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
    TestStreamId, TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestConsumerGroupListCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    consumer_group_id: u32,
    consumer_group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    output: OutputFormat,
}

impl TestConsumerGroupListCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        consumer_group_id: u32,
        consumer_group_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            consumer_group_id,
            consumer_group_name,
            using_stream_id,
            using_topic_id,
            output,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command.extend(self.output.to_args().into_iter().map(String::from));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, self.stream_id.into())
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                Some(self.topic_id),
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());

        let consumer_group = client
            .create_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.consumer_group_name,
                self.consumer_group_id.into(),
            )
            .await;
        assert!(consumer_group.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let topic_id = match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        };

        let start_message = format!(
            "Executing list consumer groups for stream with ID: {} and topic with ID: {} in {} mode",
            stream_id, topic_id, self.output
        );

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(self.consumer_group_name.clone()));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_group = client
            .delete_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.consumer_group_id.try_into().unwrap(),
            )
            .await;
        assert!(consumer_group.is_ok());

        let topic = client
            .delete_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_id.try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            OutputFormat::Default,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            OutputFormat::Default,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            OutputFormat::Default,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            OutputFormat::Default,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            OutputFormat::List,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            OutputFormat::List,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            OutputFormat::List,
        ),
        (TestStreamId::Named, TestTopicId::Named, OutputFormat::List),
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            OutputFormat::Table,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            OutputFormat::Table,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            OutputFormat::Table,
        ),
        (TestStreamId::Named, TestTopicId::Named, OutputFormat::Table),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, output_format) in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerGroupListCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                3,
                String::from("consumer-group"),
                using_stream_id,
                using_topic_id,
                output_format,
            ))
            .await;
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "list", "--help"],
            format!(
                r#"List all consumer groups for given stream ID and topic ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy consumer-group list 1 1
 iggy consumer-group list stream 2 --list-mode table
 iggy consumer-group list 3 topic -l table
 iggy consumer-group list production sensor -l table

{USAGE_PREFIX} consumer-group list [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list consumer groups
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to list consumer groups
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

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
            vec!["consumer-group", "list", "-h"],
            format!(
                r#"List all consumer groups for given stream ID and topic ID

{USAGE_PREFIX} consumer-group list [OPTIONS] <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to list consumer groups
  <TOPIC_ID>   Topic ID to list consumer groups

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
