// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::cli::common::{
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestConsumerGroupId, TestHelpCmd,
    TestStreamId, TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::IggyExpiry;
use iggy::prelude::StreamCreateOptions;
use iggy::prelude::TopicCreateOptions;
use iggy::prelude::{Consumer, IggyMessage, Partitioning};
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::str::FromStr;
use std::time::{Duration, Instant};

struct TestConsumerGroupGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    group_id: u32,
    group_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    using_group_id: TestConsumerGroupId,
}

impl TestConsumerGroupGetCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        group_id: u32,
        group_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
        using_group_id: TestConsumerGroupId,
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
            using_group_id,
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

        command.push(match self.using_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.group_id),
            TestConsumerGroupId::Named => self.group_name.clone(),
        });

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());
        let stream_details = stream.unwrap();
        self.stream_id = stream_details.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap();
        self.topic_id = topic_details.id;

        let consumer_group = client
            .create_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name,
            )
            .await;
        assert!(consumer_group.is_ok());
        let consumer_group_details = consumer_group.unwrap();
        self.group_id = consumer_group_details.id;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("get")
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

        let group_id = match self.using_group_id {
            TestConsumerGroupId::Numeric => format!("{}", self.group_id),
            TestConsumerGroupId::Named => self.group_name.clone(),
        };

        let start_message = format!(
            "Executing get consumer group with ID: {group_id} for topic with ID: {topic_id} and stream with ID: {stream_id}"
        );

        command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!("Consumer group id   | {}", self.group_id)))
            .stdout(contains(format!(
                "Consumer group name | {}",
                self.group_name
            )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer_group = client
            .delete_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.clone().try_into().unwrap(),
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

struct TestConsumerGroupGetLagWithoutStoredOffsetCmd {
    stream_name: String,
    topic_name: String,
    group_name: String,
    messages_count: u32,
    stream_id: u32,
    topic_id: u32,
    group_id: u32,
}

impl TestConsumerGroupGetLagWithoutStoredOffsetCmd {
    fn new() -> Self {
        Self {
            stream_name: String::from("cg-get-lag-no-offset-stream"),
            topic_name: String::from("cg-get-lag-no-offset-topic"),
            group_name: String::from("cg-get-lag-no-offset-group"),
            messages_count: 10,
            stream_id: 0,
            topic_id: 0,
            group_id: 0,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupGetLagWithoutStoredOffsetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await.unwrap();
        self.stream_id = stream.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .unwrap();
        self.topic_id = topic.id;

        let consumer_group = client
            .create_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name,
            )
            .await
            .unwrap();
        self.group_id = consumer_group.id;

        let mut messages = (1..=self.messages_count)
            .filter_map(|id| IggyMessage::from_str(format!("Test message {id}").as_str()).ok())
            .collect::<Vec<_>>();
        client
            .send_messages(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("get")
            .arg(self.stream_name.clone())
            .arg(self.topic_name.clone())
            .arg(self.group_name.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let current_offset = self.messages_count - 1;
        command_state
            .success()
            .stdout(contains("Stored offset"))
            .stdout(contains(format!(
                "{current_offset}              | -             | {}",
                self.messages_count
            )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        client
            .delete_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.clone().try_into().unwrap(),
            )
            .await
            .unwrap();
        client
            .delete_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await
            .unwrap();
        client
            .delete_stream(&self.stream_id.try_into().unwrap())
            .await
            .unwrap();
    }
}

struct TestConsumerGroupGetLagWithStoredOffsetCmd {
    stream_name: String,
    topic_name: String,
    group_name: String,
    messages_count: u32,
    stored_offset: u64,
    stream_id: u32,
    topic_id: u32,
    group_id: u32,
}

impl TestConsumerGroupGetLagWithStoredOffsetCmd {
    fn new() -> Self {
        Self {
            stream_name: String::from("cg-get-lag-stored-stream"),
            topic_name: String::from("cg-get-lag-stored-topic"),
            group_name: String::from("cg-get-lag-stored-group"),
            messages_count: 10,
            stored_offset: 3,
            stream_id: 0,
            topic_id: 0,
            group_id: 0,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerGroupGetLagWithStoredOffsetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await.unwrap();
        self.stream_id = stream.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .unwrap();
        self.topic_id = topic.id;

        let consumer_group = client
            .create_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name,
            )
            .await
            .unwrap();
        self.group_id = consumer_group.id;

        let join_result = client
            .join_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.clone().try_into().unwrap(),
            )
            .await;
        assert!(
            join_result.is_ok(),
            "join_consumer_group failed: {:?}",
            join_result.err()
        );

        // The server fences a group offset write to the calling connection's
        // membership. Join returns before that assignment is visible.
        let deadline = Instant::now() + Duration::from_secs(10);
        let stream_id = self.stream_id.try_into().unwrap();
        let topic_id = self.topic_id.try_into().unwrap();
        let group_id = self.group_id.try_into().unwrap();
        loop {
            let group = client
                .get_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .unwrap()
                .unwrap();
            let owns_partition = group
                .members
                .iter()
                .any(|member| member.partitions.contains(&0));
            if group.members_count == 1 && owns_partition {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "partition 0 was not assigned: {group:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let mut messages = (1..=self.messages_count)
            .filter_map(|id| IggyMessage::from_str(format!("Test message {id}").as_str()).ok())
            .collect::<Vec<_>>();
        client
            .send_messages(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();

        let group_consumer = Consumer::group(self.group_name.clone().try_into().unwrap());
        client
            .store_consumer_offset(
                &group_consumer,
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                Some(0),
                self.stored_offset,
            )
            .await
            .unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-group")
            .arg("get")
            .arg(self.stream_name.clone())
            .arg(self.topic_name.clone())
            .arg(self.group_name.clone())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let current_offset = self.messages_count - 1;
        let lag = current_offset as u64 - self.stored_offset;
        command_state.success().stdout(contains(format!(
            "{current_offset}              | {}             | {lag}",
            self.stored_offset
        )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let _ = client
            .leave_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.clone().try_into().unwrap(),
            )
            .await;
        client
            .delete_consumer_group(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &self.group_name.clone().try_into().unwrap(),
            )
            .await
            .unwrap();
        client
            .delete_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await
            .unwrap();
        client
            .delete_stream(&self.stream_id.try_into().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
#[parallel]
pub async fn should_show_lag_without_stored_offset() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupGetLagWithoutStoredOffsetCmd::new())
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_lag_with_stored_offset() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestConsumerGroupGetLagWithStoredOffsetCmd::new())
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Numeric,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Numeric,
            TestConsumerGroupId::Named,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Numeric,
        ),
        (
            TestStreamId::Named,
            TestTopicId::Named,
            TestConsumerGroupId::Named,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, using_consumer_group_id) in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerGroupGetCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                3,
                String::from("consumer-group"),
                using_stream_id,
                using_topic_id,
                using_consumer_group_id,
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
            vec!["consumer-group", "get", "--help"],
            format!(
                r#"Get details of a single consumer group with given ID for given stream ID and topic ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID
Consumer group ID can be specified as a consumer group name or ID

Examples:
 iggy consumer-group get 1 2 3
 iggy consumer-group get stream 2 3
 iggy consumer-group get 1 topic 3
 iggy consumer-group get 1 2 group
 iggy consumer-group get stream topic 3
 iggy consumer-group get 1 topic group
 iggy consumer-group get stream 2 group
 iggy consumer-group get stream topic group

{USAGE_PREFIX} consumer-group get <STREAM_ID> <TOPIC_ID> <GROUP_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get consumer group
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get consumer group
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <GROUP_ID>
          Consumer group ID to get
{CLAP_INDENT}
          Consumer group ID can be specified as a consumer group name or ID

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
            vec!["consumer-group", "get", "-h"],
            format!(
                r#"Get details of a single consumer group with given ID for given stream ID and topic ID

{USAGE_PREFIX} consumer-group get <STREAM_ID> <TOPIC_ID> <GROUP_ID>

Arguments:
  <STREAM_ID>  Stream ID to get consumer group
  <TOPIC_ID>   Topic ID to get consumer group
  <GROUP_ID>   Consumer group ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
