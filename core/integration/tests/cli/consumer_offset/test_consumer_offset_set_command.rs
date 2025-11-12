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
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;

struct TestConsumerOffsetSetCmd {
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
    stored_offset: u64,
}

impl TestConsumerOffsetSetCmd {
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
        stored_offset: u64,
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
            stored_offset,
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
        command.push(format!("{}", self.stored_offset));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerOffsetSetCmd {
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

        let mut messages = (1..=self.stored_offset + 1)
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
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("consumer-offset")
            .arg("set")
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
            "Executing set consumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} to {}\nConsumer offset for consumer with ID: {} for stream with ID: {} and topic with ID: {} and partition with ID: {} set to {}\n",
            consumer_id,
            stream_id,
            topic_id,
            self.partition_id,
            self.stored_offset,
            consumer_id,
            stream_id,
            topic_id,
            self.partition_id,
            self.stored_offset
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let consumer = match self.using_consumer_id {
            TestConsumerId::Numeric => Consumer {
                kind: ConsumerKind::Consumer,
                id: Identifier::numeric(self.consumer_id).unwrap(),
            },
            TestConsumerId::Named => Consumer {
                kind: ConsumerKind::Consumer,
                id: Identifier::named(self.consumer_name.as_str()).unwrap(),
            },
        };

        let offset = client
            .get_consumer_offset(
                &consumer,
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                Some(self.partition_id),
            )
            .await;
        assert!(offset.is_ok());
        let offset = offset.unwrap().expect("Failed to get consumer offset");
        assert_eq!(offset.stored_offset, self.stored_offset);

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
            .execute_test(TestConsumerOffsetSetCmd::new(
                1,
                String::from("consumer"),
                2,
                String::from("stream"),
                3,
                String::from("topic"),
                0,
                using_consumer_id,
                using_stream_id,
                using_topic_id,
                100,
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
            vec!["consumer-offset", "set", "--help"],
            format!(
                r#"Set the offset of a consumer for a given partition on the server

Consumer ID can be specified as a consumer name or ID
Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy consumer-offset set 1 3 5 1 100
 iggy consumer-offset set consumer 3 5 1 100
 iggy consumer-offset set 1 stream 5 1 100
 iggy consumer-offset set 1 3 topic 1 100
 iggy consumer-offset set consumer stream 5 1 100
 iggy consumer-offset set consumer 3 topic 1 100
 iggy consumer-offset set 1 stream topic 1 100
 iggy consumer-offset set consumer stream topic 1 100

{USAGE_PREFIX} consumer-offset set [OPTIONS] <CONSUMER_ID> <STREAM_ID> <TOPIC_ID> <PARTITION_ID> <OFFSET>

Arguments:
  <CONSUMER_ID>
          Consumer for which the offset is set
{CLAP_INDENT}
          Consumer ID can be specified as a consumer name or ID

  <STREAM_ID>
          Stream ID for which consumer offset is set
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID for which consumer offset is set
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITION_ID>
          Partitions ID for which consumer offset is set

  <OFFSET>
          Offset to set

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
            vec!["consumer-offset", "set", "-h"],
            format!(
                r#"Set the offset of a consumer for a given partition on the server

{USAGE_PREFIX} consumer-offset set [OPTIONS] <CONSUMER_ID> <STREAM_ID> <TOPIC_ID> <PARTITION_ID> <OFFSET>

Arguments:
  <CONSUMER_ID>   Consumer for which the offset is set
  <STREAM_ID>     Stream ID for which consumer offset is set
  <TOPIC_ID>      Topic ID for which consumer offset is set
  <PARTITION_ID>  Partitions ID for which consumer offset is set
  <OFFSET>        Offset to set

Options:
  -k, --kind <KIND>  Consumer kind: "consumer" for regular consumer, "consumer_group" for consumer group [default: consumer] [possible values: consumer, consumer-group]
  -h, --help         Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
