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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::cargo_bin_path;
use predicates::str::contains;
use serial_test::parallel;

const STREAM_NAME: &str = "stream";
const TOPIC_NAME: &str = "topic";
const GROUP_NAME: &str = "group";

struct TestMessagePollConsumerGroupCmd {
    messages: Vec<String>,
    message_count: usize,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl TestMessagePollConsumerGroupCmd {
    fn new(messages: &[String], message_count: usize) -> Self {
        assert!(message_count <= messages.len());
        Self {
            messages: messages.to_owned(),
            message_count,
            actual_stream_id: None,
            actual_topic_id: None,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessagePollConsumerGroupCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(STREAM_NAME, &StreamCreateOptions::default())
            .await
            .unwrap();
        self.actual_stream_id = Some(stream.id);
        let stream_id: Identifier = stream.id.try_into().unwrap();

        let topic = client
            .create_topic(
                &stream_id,
                TOPIC_NAME,
                &TopicCreateOptions {
                    partitions_count: Some(1),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .unwrap();
        self.actual_topic_id = Some(topic.id);
        let topic_id: Identifier = topic.id.try_into().unwrap();

        client
            .create_consumer_group(&stream_id, &topic_id, GROUP_NAME)
            .await
            .unwrap();

        let mut messages = self
            .messages
            .iter()
            .map(|message| {
                IggyMessage::builder()
                    .payload(Bytes::from(message.as_bytes().to_vec()))
                    .build()
                    .expect("Failed to create message")
            })
            .collect::<Vec<_>>();
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("poll")
            .args(vec![
                "--consumer-group".into(),
                GROUP_NAME.into(),
                "--first".into(),
                "--message-count".into(),
                format!("{}", self.message_count),
                STREAM_NAME.into(),
                TOPIC_NAME.into(),
            ])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let mut status = command_state.success().stdout(contains(format!(
            "Polled {} of total size",
            match self.message_count {
                1 => "1 message".to_owned(),
                count => format!("{count} messages"),
            }
        )));
        for message in self.messages.iter().take(self.message_count) {
            status = status.stdout(contains(message));
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if let (Some(stream_id), Some(topic_id)) = (self.actual_stream_id, self.actual_topic_id) {
            let topic = client
                .delete_topic(
                    &stream_id.try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                )
                .await;
            assert!(topic.is_ok());

            let stream = client.delete_stream(&stream_id.try_into().unwrap()).await;
            assert!(stream.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_poll_messages_as_consumer_group_member() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_messages: Vec<String> = vec![
        "Lorem ipsum dolor sit amet".into(),
        "consectetur adipiscing elit".into(),
        "sed do eiusmod tempor".into(),
    ];

    iggy_cmd_test.setup().await;
    for message_count in [1, 2] {
        iggy_cmd_test
            .execute_test(TestMessagePollConsumerGroupCmd::new(
                &test_messages,
                message_count,
            ))
            .await;
    }
}

#[test]
#[parallel]
fn given_http_transport_when_polling_consumer_group_should_reject_before_connecting() {
    let cli_home = tempfile::tempdir().unwrap();

    let mut command = assert_cmd::Command::new(cargo_bin_path("iggy").unwrap());
    command
        .env("IGGY_HOME", cli_home.path())
        .args([
            "--transport",
            "http",
            "--http-api-url",
            "http://127.0.0.1:0",
            "message",
            "poll",
            "--consumer-group",
            GROUP_NAME,
            "--first",
            STREAM_NAME,
            TOPIC_NAME,
        ])
        .assert()
        .failure()
        .stderr(contains("--consumer-group is not supported over HTTP"));
}

async fn create_stream_topic_and_group(client: &dyn Client, partitions_count: u32) -> (u32, u32) {
    let stream = client
        .create_stream(STREAM_NAME, &StreamCreateOptions::default())
        .await
        .unwrap();
    let stream_id: Identifier = stream.id.try_into().unwrap();
    let topic = client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(partitions_count),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    let topic_id: Identifier = topic.id.try_into().unwrap();
    client
        .create_consumer_group(&stream_id, &topic_id, GROUP_NAME)
        .await
        .unwrap();
    (stream.id, topic.id)
}

async fn send_one_message_per_partition(
    client: &dyn Client,
    stream_id: u32,
    topic_id: u32,
    partitions_count: u32,
) {
    let stream_id: Identifier = stream_id.try_into().unwrap();
    let topic_id: Identifier = topic_id.try_into().unwrap();
    for partition_id in 0..partitions_count {
        let mut messages = vec![
            IggyMessage::builder()
                .payload(Bytes::from(format!("partition-{partition_id}")))
                .build()
                .expect("Failed to create message"),
        ];
        client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }
}

struct TestMessagePollAutoCommitCmd {
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

#[async_trait]
impl IggyCmdTestCase for TestMessagePollAutoCommitCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let (stream_id, topic_id) = create_stream_topic_and_group(client, 2).await;
        self.actual_stream_id = Some(stream_id);
        self.actual_topic_id = Some(topic_id);
        send_one_message_per_partition(client, stream_id, topic_id, 2).await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("poll")
            .args(vec![
                "--consumer-group",
                GROUP_NAME,
                "--next",
                "--auto-commit",
                "--message-count",
                "2",
                STREAM_NAME,
                TOPIC_NAME,
            ])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(contains("Polled 2 messages"))
            .stdout(contains("partition-0"))
            .stdout(contains("partition-1"));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let (Some(stream_id), Some(topic_id)) = (self.actual_stream_id, self.actual_topic_id)
        else {
            return;
        };
        let stream_id: Identifier = stream_id.try_into().unwrap();
        let topic_id: Identifier = topic_id.try_into().unwrap();
        let group = Consumer::group(Identifier::named(GROUP_NAME).unwrap());
        for partition_id in 0..2 {
            let offset = client
                .get_consumer_offset(&group, &stream_id, &topic_id, Some(partition_id))
                .await
                .unwrap()
                .expect("the group offset of every assigned partition is stored");
            assert_eq!(offset.stored_offset, 0, "partition {partition_id}");
        }

        client.delete_topic(&stream_id, &topic_id).await.unwrap();
        client.delete_stream(&stream_id).await.unwrap();
    }
}

#[tokio::test]
#[parallel]
pub async fn given_two_partitions_when_polling_with_auto_commit_should_store_offset_per_partition()
{
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMessagePollAutoCommitCmd {
            actual_stream_id: None,
            actual_topic_id: None,
        })
        .await;
}

#[cfg(target_os = "linux")]
struct TestMessagePollFailedWriteCmd {
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

#[cfg(target_os = "linux")]
#[async_trait]
impl IggyCmdTestCase for TestMessagePollFailedWriteCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let (stream_id, topic_id) = create_stream_topic_and_group(client, 1).await;
        self.actual_stream_id = Some(stream_id);
        self.actual_topic_id = Some(topic_id);
        send_one_message_per_partition(client, stream_id, topic_id, 1).await;
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("poll")
            .args(vec![
                "--consumer-group",
                GROUP_NAME,
                "--first",
                "--output-file",
                "/dev/full",
                STREAM_NAME,
                TOPIC_NAME,
            ])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .failure()
            .stderr(contains("Problem writing message to file"));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let (Some(stream_id), Some(topic_id)) = (self.actual_stream_id, self.actual_topic_id)
        else {
            return;
        };
        let stream_id: Identifier = stream_id.try_into().unwrap();
        let topic_id: Identifier = topic_id.try_into().unwrap();
        let group = Identifier::named(GROUP_NAME).unwrap();
        let details = client
            .get_consumer_group(&stream_id, &topic_id, &group)
            .await
            .unwrap()
            .expect("the consumer group still exists");
        assert_eq!(
            details.members_count, 0,
            "a failed poll must leave the group"
        );

        client.delete_topic(&stream_id, &topic_id).await.unwrap();
        client.delete_stream(&stream_id).await.unwrap();
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[parallel]
pub async fn given_failed_output_write_after_join_when_polling_should_leave_group() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMessagePollFailedWriteCmd {
            actual_stream_id: None,
            actual_topic_id: None,
        })
        .await;
}
