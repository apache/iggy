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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId,
    TestTopicId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::Durability;
use iggy::prelude::IggyExpiry;
use iggy::prelude::TopicCreateOptions;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct ExpectedPartitionRow {
    id: u32,
    created_at: String,
    segments_count: u32,
    current_offset: u64,
    size: String,
    messages_count: u64,
}

struct TestTopicGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    partitions_count: u32,
    durability: Durability,
    expected_partitions: Vec<ExpectedPartitionRow>,
}

impl TestTopicGetCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            using_stream_id,
            using_topic_id,
            partitions_count: 1,
            durability: Durability::Replicated,
            expected_partitions: Vec::new(),
        }
    }

    fn with_topic_options(mut self, partitions_count: u32, durability: Durability) -> Self {
        self.partitions_count = partitions_count;
        self.durability = durability;
        self
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

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestTopicGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(self.partitions_count),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    durability: self.durability,
                    ..TopicCreateOptions::default()
                },
            )
            .await;
        assert!(topic.is_ok());

        let details = client
            .get_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name.clone().try_into().unwrap(),
            )
            .await
            .expect("topic details should be retrievable")
            .expect("created topic should exist");
        self.expected_partitions = details
            .partitions
            .iter()
            .map(|partition| ExpectedPartitionRow {
                id: partition.id,
                created_at: partition.created_at.to_utc_string("%Y-%m-%d %H:%M:%S"),
                segments_count: partition.segments_count,
                current_offset: partition.current_offset,
                size: partition.size.to_string(),
                messages_count: partition.messages_count,
            })
            .collect();
        assert_eq!(self.expected_partitions.len(), self.partitions_count as usize);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("topic")
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

        let start_message =
            format!("Executing get topic with ID: {topic_id} from stream with ID: {stream_id}");

        let mut assertion = command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains("Topic id            | 0"))
            .stdout(contains(format!(
                "Topic name          | {}",
                self.topic_name
            )))
            .stdout(contains("Topic size          | 0"))
            .stdout(contains("Message expiry      | unlimited"))
            .stdout(contains("Max topic size      | unlimited"))
            .stdout(contains("Topic message count | 0"))
            .stdout(contains(format!(
                "Partitions count    | {}",
                self.partitions_count
            )))
            .stdout(contains("Partition id"))
            .stdout(contains("Created"))
            .stdout(contains("Segments count"))
            .stdout(contains("Current offset"))
            .stdout(contains("Size"))
            .stdout(contains("Messages count"))
            .stdout(contains("durability"))
            .stdout(
                predicates::str::is_match(format!(r"durability\s+{}", self.durability))
                    .expect("valid durability regex"),
            );

        for partition in &self.expected_partitions {
            // Check every field in every row against the topic details returned by the server.
            let row = format!(
                r"\b{}\s+{}\s+{}\s+{}\s+{}\s+{}\b",
                partition.id,
                partition.created_at,
                partition.segments_count,
                partition.current_offset,
                partition.size,
                partition.messages_count,
            );
            assertion = assertion
                .stdout(predicates::str::is_match(row).expect("valid partition row regex"));
        }
        assertion.stdout(contains("Options"));
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
        .execute_test(TestTopicGetCmd::new(
            0,
            String::from("main"),
            0,
            String::from("sync"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            1,
            String::from("customer"),
            3,
            String::from("probe"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            1,
            String::from("development"),
            2,
            String::from("testing"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            6,
            String::from("other"),
            1,
            String::from("topic"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_show_multiple_partitions_and_persisted_durability() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(
            TestTopicGetCmd::new(
                7,
                String::from("durable-stream"),
                4,
                String::from("multi-partition-topic"),
                TestStreamId::Named,
                TestTopicId::Named,
            )
            .with_topic_options(3, Durability::Persisted),
        )
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "get", "--help"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy topic get 1 1
 iggy topic get prod 2
 iggy topic get test debugs
 iggy topic get 2 debugs

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>
          Stream ID to get topic
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          Topic ID to get
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

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
            vec!["topic", "get", "-h"],
            format!(
                r#"Get topic detail for given topic ID and stream ID

{USAGE_PREFIX} topic get <STREAM_ID> <TOPIC_ID>

Arguments:
  <STREAM_ID>  Stream ID to get topic
  <TOPIC_ID>   Topic ID to get

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
