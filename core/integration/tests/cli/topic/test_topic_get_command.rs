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
    TestTopicId, USAGE_PREFIX, clear_session_entry,
};
use assert_cmd::Command;
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::Durability;
use iggy::prelude::IggyExpiry;
use iggy::prelude::defaults::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy::prelude::{IggyMessage, Partitioning, StreamClient, TopicClient, TopicCreateOptions};
use integration::harness::{TestHarnessBuilder, TestServerConfig};
use predicates::prelude::PredicateBooleanExt;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicGetCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    partitions_count: u32,
    durability: Durability,
    consumer_offset_durability: Durability,
    partition_ids: Vec<u32>,
    populate_partitions: bool,
}

impl TestTopicGetCmd {
    fn new(
        stream_name: String,
        topic_name: String,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
    ) -> Self {
        Self {
            stream_id: 0,
            stream_name,
            topic_id: 0,
            topic_name,
            using_stream_id,
            using_topic_id,
            partitions_count: 1,
            durability: Durability::Replicated,
            consumer_offset_durability: Durability::Replicated,
            partition_ids: Vec::new(),
            populate_partitions: false,
        }
    }

    fn with_topic_options(mut self, partitions_count: u32, durability: Durability) -> Self {
        self.partitions_count = partitions_count;
        self.durability = durability;
        self.consumer_offset_durability = match durability {
            Durability::Persisted => Durability::Replicated,
            Durability::Replicated => Durability::Persisted,
        };
        self.populate_partitions = true;
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
        let stream = client
            .create_stream(&self.stream_name)
            .await
            .expect("stream should be created");
        self.stream_id = stream.id;

        let topic = client
            .create_topic(
                &self.stream_name.clone().try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(self.partitions_count),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    durability: self.durability,
                    consumer_offset_durability: self.consumer_offset_durability,
                    preallocate_segments: Some(false),
                    ..TopicCreateOptions::default()
                },
            )
            .await
            .expect("topic should be created");
        self.topic_id = topic.id;

        if self.populate_partitions {
            for (index, partition) in topic.partitions.iter().enumerate() {
                let mut messages = (0..=index)
                    .map(|_| "topic get test payload".parse::<IggyMessage>().unwrap())
                    .collect::<Vec<_>>();
                client
                    .send_messages(
                        &self.stream_id.try_into().unwrap(),
                        &self.topic_id.try_into().unwrap(),
                        &Partitioning::partition_id(partition.id),
                        &mut messages,
                    )
                    .await
                    .expect("messages should be sent");
            }
        }

        self.partition_ids = topic
            .partitions
            .iter()
            .map(|partition| partition.id)
            .collect();
        assert_eq!(self.partition_ids.len(), self.partitions_count as usize);
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

        let expected_messages_count = if self.populate_partitions {
            (1..=u64::from(self.partitions_count)).sum::<u64>()
        } else {
            0
        };
        let assertion = command_state
            .success()
            .stdout(starts_with(start_message))
            .stdout(contains(format!("Topic id            | {}", self.topic_id)))
            .stdout(contains(format!(
                "Topic name          | {}",
                self.topic_name
            )))
            .stdout(contains("Topic size          | "))
            .stdout(contains("Message expiry      | unlimited"))
            .stdout(contains("Max topic size      | unlimited"))
            .stdout(contains(format!(
                "Topic message count | {}",
                expected_messages_count
            )))
            .stdout(contains(format!(
                "Partitions count    | {}",
                self.partitions_count
            )));

        let mut assertion = assertion
            .stdout(contains("Partition id | Created"))
            .stdout(contains("Segments count | Current offset | Size"))
            .stdout(contains("Messages count"))
            .stdout(contains("Option                            | Value"))
            .stdout(contains("Source"))
            .stdout(contains(
                "preallocate_segments              | false                | explicit",
            ))
            .stdout(contains(
                "messages_required_to_save         | 1024                 | default",
            ));
        assertion = match self.durability {
            Durability::Persisted => assertion.stdout(contains(
                "|  durability                        | persisted            | explicit",
            )),
            Durability::Replicated => assertion.stdout(contains(
                "|  durability                        | replicated           | explicit",
            )),
        };
        assertion = match self.consumer_offset_durability {
            Durability::Persisted => assertion.stdout(contains(
                "consumer_offset_durability        | persisted            | explicit",
            )),
            Durability::Replicated => assertion.stdout(contains(
                "consumer_offset_durability        | replicated           | explicit",
            )),
        };
        if self.populate_partitions {
            assertion = assertion.stdout(contains("Topic size          | 0 B").not());
        } else {
            assertion = assertion.stdout(contains("Topic size          | 0 B"));
        }
        let stdout = std::str::from_utf8(&assertion.get_output().stdout)
            .expect("CLI output should be UTF-8");
        let rows = stdout
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>();
        for (index, partition_id) in self.partition_ids.iter().enumerate() {
            let prefix = format!("| | {partition_id} |");
            let row = rows
                .iter()
                .find(|line| line.starts_with(&prefix))
                .expect("partition should have a row in CLI output");
            if self.populate_partitions {
                assert!(row.contains(&format!("| 1 | {index} |")), "{row}");
                assert!(row.contains(" B |") && !row.contains("| 0 B |"), "{row}");
                assert!(row.ends_with(&format!("| {} |", index + 1)), "{row}");
            } else {
                assert!(row.contains("| 1 | 0 | 0 B | 0 |"), "{row}");
            }
        }
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
            String::from("main"),
            String::from("sync"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("customer"),
            String::from("probe"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("development"),
            String::from("testing"),
            TestStreamId::Named,
            TestTopicId::Named,
        ))
        .await;
    iggy_cmd_test
        .execute_test(TestTopicGetCmd::new(
            String::from("other"),
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
                String::from("durable-stream"),
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
pub async fn should_show_multiple_partitions_using_numeric_identifiers() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(
            TestTopicGetCmd::new(
                String::from("numeric-stream"),
                String::from("numeric-topic"),
                TestStreamId::Numeric,
                TestTopicId::Numeric,
            )
            .with_topic_options(3, Durability::Replicated),
        )
        .await;
}

struct TestMissingTopicGetCmd;

#[async_trait]
impl IggyCmdTestCase for TestMissingTopicGetCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        client.create_stream("missing-topic-stream").await.unwrap();
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .args(vec![
                "topic",
                "get",
                "missing-topic-stream",
                "missing-topic",
            ])
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        command_state
            .success()
            .stdout(contains(
                "Topic with ID: missing-topic in stream missing-topic-stream was not found",
            ))
            .stdout(predicates::str::contains("Partitions").not());
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        client
            .delete_stream(&"missing-topic-stream".try_into().unwrap())
            .await
            .unwrap();
    }
}

#[tokio::test]
#[parallel]
pub async fn should_report_missing_topic() {
    let mut iggy_cmd_test = IggyCmdTest::default();
    iggy_cmd_test.setup().await;
    iggy_cmd_test.execute_test(TestMissingTopicGetCmd).await;
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

#[tokio::test]
#[parallel]
#[ignore = "opt-in end-to-end test of the CLI persisted durability flag"]
pub async fn should_show_persisted_durability_created_through_cli() {
    let mut harness = TestHarnessBuilder::default()
        .server(TestServerConfig::default())
        .build()
        .unwrap();
    harness.start().await.expect("test server should start");
    let client = harness.tcp_root_client().await.unwrap();
    let stream = client.create_stream("cli-persisted-stream").await.unwrap();
    let address = harness.server().raw_tcp_addr().unwrap();
    let socket_address = harness.server().tcp_addr().unwrap();
    clear_session_entry(socket_address);
    let cli = || {
        #[allow(deprecated)]
        let mut command = Command::cargo_bin("iggy").unwrap();
        command.env("IGGY_TOKEN", "").env("COLUMNS", "200").args([
            "--tcp-server-address",
            &address,
            "--username",
            DEFAULT_ROOT_USERNAME,
            "--password",
            DEFAULT_ROOT_PASSWORD,
        ]);
        command
    };
    cli()
        .args([
            "topic",
            "create",
            "cli-persisted-stream",
            "cli-persisted-topic",
            "3",
            "none",
            "--durability",
            "persisted",
            "--set",
            "preallocate_segments=false",
        ])
        .assert()
        .success();

    let details = client
        .get_topic(
            &stream.id.try_into().unwrap(),
            &"cli-persisted-topic".try_into().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(details.partitions_count, 3);
    assert_eq!(details.partitions.len(), 3);
    let option = details
        .options
        .iter()
        .find(|(key, _)| key.as_bytes() == b"durability")
        .unwrap();
    assert_eq!(option.1.value.to_string_value(), "persisted");

    assert!(option.1.explicit);

    let mut assertion = cli()
        .args([
            "topic",
            "get",
            "cli-persisted-stream",
            "cli-persisted-topic",
        ])
        .assert()
        .success()
        .stdout(contains("Partitions count    | 3"))
        .stdout(contains("Partition id | Created"))
        .stdout(contains("Segments count | Current offset | Size"))
        .stdout(contains("Messages count"))
        .stdout(contains("Source"))
        .stdout(contains(
            "|  durability                        | persisted            | explicit",
        ))
        .stdout(contains(
            "consumer_offset_durability        | replicated           | explicit",
        ))
        .stdout(contains(
            "preallocate_segments              | false                | explicit",
        ))
        .stdout(contains(
            "messages_required_to_save         | 1024                 | default",
        ));
    for partition in &details.partitions {
        assertion = assertion.stdout(contains(format!("|  {:<12} |", partition.id)));
    }

    client
        .delete_topic(
            &stream.id.try_into().unwrap(),
            &details.id.try_into().unwrap(),
        )
        .await
        .unwrap();
    client
        .delete_stream(&stream.id.try_into().unwrap())
        .await
        .unwrap();
    clear_session_entry(socket_address);
    harness.stop().await.unwrap();
}
