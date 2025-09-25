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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestConsumerId, TestHelpCmd,
    TestStreamId, TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::*;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::str::FromStr;

struct TestConsumerOffsetGetCmd {
    consumer_id: u32,
    consumer_name: String,
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partition_id: u32,
    using_consumer_id: TestConsumerId,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    messages_count: u32,
    stored_offset: u64,
}

impl TestConsumerOffsetGetCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        consumer_id: u32,
        consumer_name: String,
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partition_id: u32,
        using_consumer_id: TestConsumerId,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            consumer_id,
            consumer_name,
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partition_id,
            using_consumer_id,
            using_stream_id,
            using_topic_id,
            messages_count: 100,
            stored_offset: 66,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_consumer_id {
            TestStreamId::Numeric => vec![format!("{}", self.consumer_id)],
            TestStreamId::Named => vec![self.consumer_name.clone()],
        };

        command.push(match self.using_stream_id {
            TestTopicId::Numeric => format!("{}", self.stream_id),
            TestTopicId::Named => self.stream_name.clone(),
        });

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

        command.push(format!("{}", self.partition_id));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerOffsetGetCmd {
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

        let mut messages = (1..=self.messages_count)
            .filter_map(|id| IggyMessage::from_str(format!("Test message {id}").as_str()).ok())
            .collect::<Vec<_>>();

        let send_status = client
            .send_messages(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &Partitioning::partition_id(self.partition_id),
                &mut messages,
            )
            .await;
        assert!(send_status.is_ok());

        let offset = client
            .store_consumer_offset(
                &Consumer {
                    kind: ConsumerKind::Consumer,
                    id: match self.using_consumer_id {
                        TestConsumerId::Numeric => Identifier::numeric(self.consumer_id),
                        TestConsumerId::Named => Identifier::named(self.consumer_name.as_str()),
                    }
                    .unwrap(),
                },
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                Some(self.partition_id),
                self.stored_offset,
            )
            .await;
        assert!(offset.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-offset")
            .arg("get")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let consumer_id = match self.using_consumer_id {
            TestConsumerId::Numeric => format!("{}", self.consumer_id),
            TestConsumerId::Named => self.consumer_name.clone(),
        };

        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        let topic_id = match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        };

        let message = format!(
            "Executing get consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {}",
            consumer_id, stream_id, topic_id, self.partition_id,
        );

        command_state
            .success()
            .stdout(starts_with(message))
            .stdout(contains(format!("Stored offset  | {}", self.stored_offset)))
            .stdout(contains(format!(
                "Current offset | {}",
                self.messages_count - 1
            )));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
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
            TestConsumerId::Numeric,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ),
        (
            TestConsumerId::Named,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ),
        (
            TestConsumerId::Numeric,
            TestStreamId::Named,
            TestTopicId::Numeric,
        ),
        (
            TestConsumerId::Numeric,
            TestStreamId::Numeric,
            TestTopicId::Named,
        ),
        (
            TestConsumerId::Named,
            TestStreamId::Named,
            TestTopicId::Numeric,
        ),
        (
            TestConsumerId::Named,
            TestStreamId::Numeric,
            TestTopicId::Named,
        ),
        (
            TestConsumerId::Numeric,
            TestStreamId::Named,
            TestTopicId::Named,
        ),
        (
            TestConsumerId::Named,
            TestStreamId::Named,
            TestTopicId::Named,
        ),
        (
            TestConsumerId::Numeric,
            TestStreamId::Numeric,
            TestTopicId::Numeric,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, using_consumer_id) in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerOffsetGetCmd::new(
                1,
                String::from("consumer"),
                2,
                String::from("stream"),
                3,
                String::from("topic"),
                1,
                using_consumer_id,
                using_stream_id,
                using_topic_id,
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
            vec!["consumer-offset", "get", "--help"],
            format!(
                r#"Retrieve the offset of a consumer for a given partition from the server

Consumer ID can be specified as a consumer name or ID
Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy consumer-offset get 1 3 5 1
 iggy consumer-offset get consumer stream 5 1
 iggy consumer-offset get 1 3 topic 1
 iggy consumer-offset get consumer stream 5 1
 iggy consumer-offset get consumer 3 topic 1
 iggy consumer-offset get 1 stream topic 1
 iggy consumer-offset get consumer stream topic 1
 iggy consumer-offset get cg-1 3000001 1 1 --kind consumer-group
 iggy consumer-offset get cg-1 3000001 1 1 -k consumer-group

{USAGE_PREFIX} consumer-offset get [OPTIONS] <CONSUMER_ID> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <CONSUMER_ID>
          Consumer for which the offset is retrieved
{CLAP_INDENT}
          Consumer ID can be specified as a consumer name or ID

  <STREAM_ID>
          Stream ID for which consumer offset is retrieved
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID for which consumer offset is retrieved
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITION_ID>
          Partitions ID for which consumer offset is retrieved

Options:
  -k, --kind <KIND>
          Consumer kind: "consumer" for regular consumer, "consumer_group" for consumer group

          Possible values:
          - consumer:       `Consumer` represents a regular consumer
          - consumer-group: `ConsumerGroup` represents a consumer group
{CLAP_INDENT}
          [default: consumer]

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
            vec!["consumer-offset", "get", "-h"],
            format!(
                r#"Retrieve the offset of a consumer for a given partition from the server

{USAGE_PREFIX} consumer-offset get [OPTIONS] <CONSUMER_ID> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <CONSUMER_ID>   Consumer for which the offset is retrieved
  <STREAM_ID>     Stream ID for which consumer offset is retrieved
  <TOPIC_ID>      Topic ID for which consumer offset is retrieved
  <PARTITION_ID>  Partitions ID for which consumer offset is retrieved

Options:
  -k, --kind <KIND>  Consumer kind: "consumer" for regular consumer, "consumer_group" for consumer group [default: consumer] [possible values: consumer, consumer-group]
  -h, --help         Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
