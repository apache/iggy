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
use iggy::prelude::MaxTopicSize;
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;

struct TestMessageFetchCmd {
    stream_name: String,
    topic_name: String,
    partitions_count: u32,
    partition_id: u32,
    fsync: bool,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl TestMessageFetchCmd {
    fn new(
        stream_name: &str,
        topic_name: &str,
        partitions_count: u32,
        partition_id: u32,
        fsync: bool,
    ) -> Self {
        Self {
            stream_name: stream_name.to_string(),
            topic_name: topic_name.to_string(),
            partitions_count,
            partition_id,
            fsync,
            actual_stream_id: None,
            actual_topic_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = Vec::new();

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

        if self.fsync {
            command.push("--fsync".to_string());
        }

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessageFetchCmd {
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
                self.partitions_count,
                Default::default(),
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await
            .expect("Failed to create topic");
        self.actual_topic_id = Some(topic.id);
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("flush")
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

        let identification_part = format!(
            "from topic with ID: {topic_id} and stream with ID: {stream_id} (partition with ID: {}) {}",
            self.partition_id,
            if self.fsync {
                "with fsync"
            } else {
                "without fsync"
            }
        );

        let message = format!(
            "Executing flush messages {identification_part}\nFlushed messages {identification_part}\n"
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if let Some(topic_id) = self.actual_topic_id {
            let stream_id = Identifier::from_str(&self.stream_name);
            assert!(stream_id.is_ok());
            let stream_id = stream_id.unwrap();

            let topic = client
                .delete_topic(&stream_id, &topic_id.try_into().unwrap())
                .await;
            assert!(topic.is_ok());

            let stream = client.delete_stream(&stream_id).await;
            assert!(stream.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![false, true]; // Test with and without fsync

    iggy_cmd_test.setup().await;
    for fsync in test_parameters {
        iggy_cmd_test
            .execute_test(TestMessageFetchCmd::new("stream", "topic", 1, 0, fsync))
            .await;
    }
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["message", "flush", "--help"],
            format!(
                r#"Flush messages from given topic ID and given stream ID

Command is used to force a flush of unsaved_buffer to disk
for specific stream, topic and partition. If fsync is enabled
then the data is flushed to disk and fsynced, otherwise the
data is only flushed to disk.

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy message flush 1 2 1
 iggy message flush stream 2 1
 iggy message flush 1 topic 1
 iggy message flush stream topic 1

{USAGE_PREFIX} message flush [OPTIONS] <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>
          ID of the stream for which messages will be flushed
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          ID of the topic for which messages will be flushed
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITION_ID>
          Partition ID for which messages will be flushed

Options:
  -f, --fsync
          fsync flushed data to disk
{CLAP_INDENT}
          If option is enabled then the data is flushed to disk and fsynced,
          otherwise the data is only flushed to disk. Default is false.

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
            vec!["message", "flush", "-h"],
            format!(
                r#"Flush messages from given topic ID and given stream ID

{USAGE_PREFIX} message flush [OPTIONS] <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>     ID of the stream for which messages will be flushed
  <TOPIC_ID>      ID of the topic for which messages will be flushed
  <PARTITION_ID>  Partition ID for which messages will be flushed

Options:
  -f, --fsync  fsync flushed data to disk
  -h, --help   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
