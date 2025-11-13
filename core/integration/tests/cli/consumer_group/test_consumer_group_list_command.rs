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
    USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestConsumerGroupListCmd {
    stream_name: String,
    topic_name: String,
    consumer_group_name: String,
    output: OutputFormat,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
    actual_consumer_group_id: Option<u32>,
}

impl TestConsumerGroupListCmd {
    fn new(
        stream_name: String,
        topic_name: String,
        consumer_group_name: String,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_name,
            topic_name,
            consumer_group_name,
            output,
            actual_stream_id: None,
            actual_topic_id: None,
            actual_consumer_group_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = vec![];

        // Use actual stream ID if available, otherwise use stream name as fallback
        if let Some(stream_id) = self.actual_stream_id {
            command.push(format!("{}", stream_id));
        } else {
            command.push(self.stream_name.clone());
        }

        // Use actual topic ID if available, otherwise use topic name as fallback
        if let Some(topic_id) = self.actual_topic_id {
            command.push(format!("{}", topic_id));
        } else {
            command.push(self.topic_name.clone());
        }

        command.extend(self.output.to_args().into_iter().map(String::from));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        // Create stream and capture its actual ID
        let stream = client
            .create_stream(&self.stream_name)
            .await
            .expect("Failed to create stream");
        self.actual_stream_id = Some(stream.id);

        // Create topic and capture its actual ID
        let topic = client
            .create_topic(
                &stream.id.try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
            .expect("Failed to create topic");
        self.actual_topic_id = Some(topic.id);

        // Create consumer group and capture its actual ID
        let consumer_group = client
            .create_consumer_group(
                &stream.id.try_into().unwrap(),
                &topic.id.try_into().unwrap(),
                &self.consumer_group_name,
            )
            .await
            .expect("Failed to create consumer group");
        self.actual_consumer_group_id = Some(consumer_group.id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = if let Some(stream_id) = self.actual_stream_id {
            format!("{}", stream_id)
        } else {
            self.stream_name.clone()
        };

        let topic_id = if let Some(topic_id) = self.actual_topic_id {
            format!("{}", topic_id)
        } else {
            self.topic_name.clone()
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
        // Use the actual IDs that were generated during resource creation
        if let Some(consumer_group_id) = self.actual_consumer_group_id {
            let consumer_group = client
                .delete_consumer_group(
                    &self.actual_stream_id.unwrap().try_into().unwrap(),
                    &self.actual_topic_id.unwrap().try_into().unwrap(),
                    &consumer_group_id.try_into().unwrap(),
                )
                .await;
            assert!(consumer_group.is_ok());
        }

        if let Some(topic_id) = self.actual_topic_id {
            let topic = client
                .delete_topic(
                    &self.actual_stream_id.unwrap().try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                )
                .await;
            assert!(topic.is_ok());
        }

        if let Some(stream_id) = self.actual_stream_id {
            let stream = client.delete_stream(&stream_id.try_into().unwrap()).await;
            assert!(stream.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![
        OutputFormat::Default,
        OutputFormat::List,
        OutputFormat::Table,
    ];

    iggy_cmd_test.setup().await;
    for output_format in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerGroupListCmd::new(
                String::from("stream"),
                String::from("topic"),
                String::from("consumer-group"),
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
