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
    IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, TestStreamId, TestTopicId,
    CLAP_INDENT, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use iggy::client::Client;
use iggy::identifier::Identifier;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use predicates::str::diff;
use serial_test::parallel;
use std::str::FromStr;

struct TestMessageFetchCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    partition_id: u32,
    fsync: bool,
}

impl TestMessageFetchCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: &str,
        topic_id: u32,
        topic_name: &str,
        partitions_count: u32,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
        partition_id: u32,
        fsync: bool,
    ) -> Self {
        Self {
            stream_id,
            stream_name: stream_name.to_string(),
            topic_id,
            topic_name: topic_name.to_string(),
            partitions_count,
            using_stream_id,
            using_topic_id,
            partition_id,
            fsync,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = Vec::new();

        command.extend(match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        });

        command.push(match self.using_topic_id {
            TestTopicId::Numeric => format!("{}", self.topic_id),
            TestTopicId::Named => self.topic_name.clone(),
        });

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
        let stream = client.create_stream(&self.stream_name, None).await;
        assert!(stream.is_ok());

        let stream_id = Identifier::from_str(&self.stream_name);
        assert!(stream_id.is_ok());
        let stream_id = stream_id.unwrap();

        let topic = client
            .create_topic(
                &stream_id,
                &self.topic_name,
                self.partitions_count,
                Default::default(),
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("flush")
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

        let identification_part = format!(
            "from topic with ID: {topic_id} and stream with ID: {stream_id} (partition with ID: {}) {}",
            self.partition_id,
            if self.fsync {
                "with fsync"
            } else {
                "without fsync"
            }
        );

        let message = format!("Executing flush messages {identification_part}\nFlushed messages {identification_part}\n");

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic_id = Identifier::from_str(&self.topic_name);
        assert!(topic_id.is_ok());
        let topic_id = topic_id.unwrap();
        let stream_id = Identifier::from_str(&self.stream_name);
        assert!(stream_id.is_ok());
        let stream_id = stream_id.unwrap();

        let topic = client.delete_topic(&stream_id, &topic_id).await;
        assert!(topic.is_ok());

        let stream = client.delete_stream(&stream_id).await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters: Vec<(TestStreamId, TestTopicId, bool)> = vec![
        (TestStreamId::Numeric, TestTopicId::Numeric, false),
        (TestStreamId::Numeric, TestTopicId::Named, false),
        (TestStreamId::Named, TestTopicId::Numeric, false),
        (TestStreamId::Named, TestTopicId::Named, false),
        (TestStreamId::Numeric, TestTopicId::Numeric, true),
        (TestStreamId::Numeric, TestTopicId::Named, true),
        (TestStreamId::Named, TestTopicId::Numeric, true),
        (TestStreamId::Named, TestTopicId::Named, true),
    ];

    iggy_cmd_test.setup().await;
    for (using_stream_id, using_topic_id, fsync) in test_parameters {
        iggy_cmd_test
            .execute_test(TestMessageFetchCmd::new(
                1,
                "stream",
                1,
                "topic",
                1,
                using_stream_id,
                using_topic_id,
                1,
                fsync,
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
