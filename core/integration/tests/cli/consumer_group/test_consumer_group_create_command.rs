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
    TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::{Client, IggyExpiry, MaxTopicSize};
use predicates::str::diff;
use serial_test::parallel;

struct TestConsumerGroupCreateCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    group_id: Option<u32>,
    group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
}

impl TestConsumerGroupCreateCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        group_id: Option<u32>,
        group_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            group_id,
            group_name,
            using_stream_id,
            using_topic_id,
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

        if let Some(group_id) = self.group_id {
            command.push("-g".to_string());
            command.push(format!("{group_id}"));
        }

        command.push(self.group_name.clone());

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupCreateCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());
        let stream_details = stream.unwrap();
        self.stream_id = stream_details.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap();
        self.topic_id = topic_details.id;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("create")
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

        let group_id = match self.group_id {
            Some(group_id) => format!("ID: {group_id}"),
            None => "ID auto incremented".to_string(),
        };

        let message = format!(
            "Executing create consumer group: {}, name: {} for topic with ID: {} and stream with ID: {}\nConsumer group: {}, name: {} created for topic with ID: {} and stream with ID: {}\n",
            group_id,
            self.group_name,
            topic_id,
            stream_id,
            group_id,
            self.group_name,
            topic_id,
            stream_id
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_group = client
            .get_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.as_str().try_into().unwrap(),
            )
            .await;
        assert!(consumer_group.is_ok());
        let consumer_group_details = consumer_group
            .unwrap()
            .expect("Failed to get consumer group");
        assert_eq!(consumer_group_details.name, self.group_name);
        if let Some(group_id) = self.group_id {
            assert_eq!(consumer_group_details.id, group_id);
        }

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

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            None,
            String::from("group1"),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            2,
            String::from("stream"),
            3,
            String::from("topic"),
            None,
            String::from("group3"),
            TestStreamId::Named,
            TestTopicId::Numeric,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            4,
            String::from("development"),
            1,
            String::from("probe"),
            None,
            String::from("group7"),
            TestStreamId::Numeric,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupCreateCmd::new(
            2,
            String::from("production"),
            5,
            String::from("test"),
            None,
            String::from("group4"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "create", "--help"],
            format!(
                r#"Create consumer group with given ID and name for given stream ID and topic ID.

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID
If group ID is not provided then the server will automatically assign it

Examples:
 iggy consumer-group create 1 1 prod
 iggy consumer-group create stream 2 test
 iggy consumer-group create 2 topic receiver
 iggy consumer-group create -g 4 stream topic group

{USAGE_PREFIX} consumer-group create [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME>

Arguments:
  <STREAM_ID>
          Stream ID to create consumer group
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to create consumer group
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <NAME>
          Consumer group name to create

Options:
  -g, --group-id <GROUP_ID>
          Consumer group ID to create

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
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["consumer-group", "create", "-h"],
            format!(
                r#"Create consumer group with given ID and name for given stream ID and topic ID.

{USAGE_PREFIX} consumer-group create [OPTIONS] <STREAM_ID> <TOPIC_ID> <NAME>

Arguments:
  <STREAM_ID>  Stream ID to create consumer group
  <TOPIC_ID>   Topic ID to create consumer group
  <NAME>       Consumer group name to create

Options:
  -g, --group-id <GROUP_ID>  Consumer group ID to create
  -h, --help                 Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
