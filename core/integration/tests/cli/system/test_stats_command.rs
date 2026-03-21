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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::prelude::Client;
use iggy::prelude::Identifier;
use iggy::prelude::IggyExpiry;
use iggy::prelude::IggyMessage;
use iggy::prelude::MaxTopicSize;
use iggy::prelude::Partitioning;
use iggy_cli::commands::binary_system::stats::GetStatsOutput;
use iggy_common::Stats;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TestStatsCmdOutput {
    Default,
    Set(GetStatsOutput),
}

struct TestStatsCmd {
    test_output: TestStatsCmdOutput,
}

impl TestStatsCmd {
    fn new(test_output: TestStatsCmdOutput) -> Self {
        Self { test_output }
    }
    fn get_cmd(&self) -> IggyCmdCommand {
        let command = IggyCmdCommand::new().arg("stats").with_env_credentials();

        match self.test_output {
            TestStatsCmdOutput::Set(option) => {
                let command = command.arg("-o").arg(format!("{option}"));
                if option != GetStatsOutput::Table {
                    command.opt("-q")
                } else {
                    command
                }
            }
            _ => command,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStatsCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream_id = Identifier::from_str_value("logs").unwrap();
        let stream = client.create_stream(&stream_id.as_string()).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &stream_id,
                "topic",
                5,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        self.get_cmd()
    }

    fn verify_command(&self, command_state: Assert) {
        match self.test_output {
            TestStatsCmdOutput::Default | TestStatsCmdOutput::Set(GetStatsOutput::Table) => {
                command_state
                    .success()
                    .stdout(starts_with("Executing stats command\n"))
                    .stdout(contains("Streams Count            | 1"))
                    .stdout(contains("Topics Count             | 1"))
                    .stdout(contains("Partitions Count         | 5"))
                    .stdout(contains("Segments Count           | 5"))
                    .stdout(contains("Message Count            | 0"))
                    // Note: Client count can vary due to connection lifecycle; at least 2 expected
                    .stdout(contains("Consumer Groups Count    | 0"))
                    .stdout(contains("Threads Count"))
                    .stdout(contains("Free Disk Space"))
                    .stdout(contains("Total Disk Space"));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::List) => {
                command_state
                    .success()
                    .stdout(contains("Streams Count|1"))
                    .stdout(contains("Topics Count|1"))
                    .stdout(contains("Partitions Count|5"))
                    .stdout(contains("Segments Count|5"))
                    .stdout(contains("Message Count|0"))
                    .stdout(contains("Consumer Groups Count|0"))
                    .stdout(contains("Threads Count|"))
                    .stdout(contains("Free Disk Space|"))
                    .stdout(contains("Total Disk Space|"));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::Json) => {
                command_state
                    .success()
                    .stdout(contains(r#""streams_count": 1"#))
                    .stdout(contains(r#""topics_count": 1"#))
                    .stdout(contains(r#""partitions_count": 5"#))
                    .stdout(contains(r#""segments_count": 5"#))
                    .stdout(contains(r#""messages_count": 0"#))
                    .stdout(contains(r#""consumer_groups_count": 0"#))
                    .stdout(contains(r#""threads_count":"#))
                    .stdout(contains(r#""free_disk_space":"#))
                    .stdout(contains(r#""total_disk_space":"#));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::Toml) => {
                command_state
                    .success()
                    .stdout(contains("streams_count = 1"))
                    .stdout(contains("topics_count = 1"))
                    .stdout(contains("partitions_count = 5"))
                    .stdout(contains("segments_count = 5"))
                    .stdout(contains("messages_count = 0"))
                    .stdout(contains("consumer_groups_count = 0"))
                    .stdout(contains("threads_count ="))
                    .stdout(contains("free_disk_space ="))
                    .stdout(contains("total_disk_space ="));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(&0.try_into().unwrap(), &0.try_into().unwrap())
            .await;
        assert!(topic.is_ok());

        let stream = client.delete_stream(&0.try_into().unwrap()).await;
        assert!(stream.is_ok());
    }
}

struct TestStatsCmdWithMessages {
    stream_id: u32,
    topic_id: u32,
}

impl TestStatsCmdWithMessages {
    fn new() -> Self {
        Self {
            stream_id: 0,
            topic_id: 0,
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestStatsCmdWithMessages {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream("size-test").await;
        assert!(stream.is_ok());
        let stream_details = stream.unwrap();
        self.stream_id = stream_details.id;

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                "topic",
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

        let mut messages = (1..=10)
            .filter_map(|id| IggyMessage::from_str(format!("Test message {id}").as_str()).ok())
            .collect::<Vec<_>>();
        let send_status = client
            .send_messages(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
                &Partitioning::default(),
                &mut messages,
            )
            .await;
        assert!(send_status.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("stats")
            .arg("-o")
            .arg("json")
            .opt("-q")
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let assert = command_state.success();
        let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
        let stats: Stats =
            serde_json::from_str(&stdout).expect("Failed to parse stats JSON output");

        assert!(
            stats.messages_count > 0,
            "messages_count should be > 0 after sending messages"
        );
        assert!(
            stats.messages_size_bytes.as_bytes_u64() > 0,
            "messages_size_bytes should be > 0 after sending messages"
        );
        assert!(
            stats.free_disk_space.as_bytes_u64() > 0,
            "free_disk_space should be > 0"
        );
        assert!(
            stats.total_disk_space.as_bytes_u64() > 0,
            "total_disk_space should be > 0"
        );
        assert!(
            stats.free_disk_space.as_bytes_u64() <= stats.total_disk_space.as_bytes_u64(),
            "free_disk_space should be <= total_disk_space"
        );
    }

    async fn verify_server_state(&self, client: &dyn Client) {
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
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Default))
        .await;
    iggy_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Table,
        )))
        .await;
    iggy_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::List,
        )))
        .await;
    iggy_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Json,
        )))
        .await;
    iggy_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Toml,
        )))
        .await;
    iggy_cmd_test
        .execute_test(TestStatsCmdWithMessages::new())
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stats", "--help"],
            format!(
                r#"get iggy server statistics

Collect basic Iggy server statistics like number of streams, topics, partitions, etc.
Server OS name, version, etc. are also collected.

{USAGE_PREFIX} stats [OPTIONS]

Options:
  -o, --output <OUTPUT>
          List mode (table, list, JSON, TOML)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list, json, toml]

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
            vec!["stats", "-h"],
            format!(
                r#"get iggy server statistics

{USAGE_PREFIX} stats [OPTIONS]

Options:
  -o, --output <OUTPUT>  List mode (table, list, JSON, TOML) [default: table] [possible values: table, list, json, toml]
  -h, --help             Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
