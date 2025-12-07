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
    USAGE_PREFIX,
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
    stream_name: String,
    topic_name: String,
    partition_id: u32,
    using_consumer_id: TestConsumerId,
    messages_count: u32,
    stored_offset: u64,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl TestConsumerOffsetGetCmd {
    fn new(
        consumer_id: u32,
        consumer_name: String,
        stream_name: String,
        topic_name: String,
        partition_id: u32,
        using_consumer_id: TestConsumerId,
    ) -> Self {
        Self {
            consumer_id,
            consumer_name,
            stream_name,
            topic_name,
            partition_id,
            using_consumer_id,
            messages_count: 100,
            stored_offset: 66,
            actual_stream_id: None,
            actual_topic_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.using_consumer_id {
            TestConsumerId::Numeric => vec![format!("{}", self.consumer_id)],
            TestConsumerId::Named => vec![self.consumer_name.clone()],
        };

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

        command.push(format!("{}", self.partition_id));

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestConsumerOffsetGetCmd {
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

        let mut messages = (1..=self.messages_count)
            .filter_map(|id| IggyMessage::from_str(format!("Test message {id}").as_str()).ok())
            .collect::<Vec<_>>();

        let send_status = client
            .send_messages(
                &stream.id.try_into().unwrap(),
                &topic.id.try_into().unwrap(),
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
                &stream.id.try_into().unwrap(),
                &topic.id.try_into().unwrap(),
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

    let test_parameters = vec![TestConsumerId::Numeric, TestConsumerId::Named];

    iggy_cmd_test.setup().await;
    for using_consumer_id in test_parameters {
        iggy_cmd_test
            .execute_test(TestConsumerOffsetGetCmd::new(
                1,
                String::from("consumer"),
                String::from("stream"),
                String::from("topic"),
                0,
                using_consumer_id,
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
