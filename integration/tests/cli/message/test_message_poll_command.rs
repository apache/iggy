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
use bytes::Bytes;
use iggy::client::Client;
use iggy::messages::poll_messages::{PollingKind, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning};
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::collections::HashMap;
use std::str::FromStr;

struct TestMessagePollCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    partitions_count: u32,
    messages: Vec<String>,
    partition_id: u32,
    message_count: usize,
    strategy: PollingStrategy,
    using_stream_id: TestStreamId,
    using_topic_id: TestTopicId,
    show_headers: bool,
    headers: (HeaderKey, HeaderValue),
}

impl TestMessagePollCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        partitions_count: u32,
        messages: &[String],
        partition_id: u32,
        message_count: usize,
        strategy: PollingStrategy,
        using_stream_id: TestStreamId,
        using_topic_id: TestTopicId,
        show_headers: bool,
        headers: (HeaderKey, HeaderValue),
    ) -> Self {
        assert!(partition_id <= partitions_count);
        assert!(partition_id > 0);
        assert!(message_count < messages.len());
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            partitions_count,
            messages: messages.to_owned(),
            partition_id,
            message_count,
            strategy,
            using_stream_id,
            using_topic_id,
            show_headers,
            headers,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.strategy.kind {
            PollingKind::Offset => vec!["--offset".into(), format!("{}", self.strategy.value)],
            PollingKind::Timestamp => {
                todo!("Timestamp based message polling is not supported yet")
            }
            PollingKind::First => vec!["--first".into()],
            PollingKind::Next => vec!["--next".into()],
            PollingKind::Last => vec!["--last".into()],
        };

        command.extend(vec![
            "--message-count".into(),
            format!("{}", self.message_count),
        ]);

        if self.show_headers {
            command.extend(vec!["--show-headers".into()]);
        }

        command.extend(match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
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
impl IggyCmdTestCase for TestMessagePollCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, self.stream_id.into())
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                self.partitions_count,
                Default::default(),
                None,
                Some(self.topic_id),
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());

        let mut messages = self
            .messages
            .iter()
            .map(|s| {
                let payload = Bytes::from(s.as_bytes().to_vec());
                Message::new(None, payload, Some(HashMap::from([self.headers.clone()])))
            })
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
            .arg("message")
            .arg("poll")
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

        let polled_status = match self.message_count {
            1 => "Polled 1 message".into(),
            _ => format!("Polled {} messages", self.message_count),
        };

        let message = format!("Executing poll messages from topic ID: {} and stream with ID: {}\nPolled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})\n{polled_status}",
            topic_id, stream_id, topic_id, stream_id, self.partition_id);

        let mut status = command_state.success().stdout(starts_with(message));

        if self.show_headers {
            status = status
                .stdout(contains(format!("Header: {}", self.headers.0)))
                .stdout(contains(self.headers.1.kind.to_string()))
                .stdout(contains(self.headers.1.value_only_to_string()).count(self.message_count))
        }

        // Check if messages are printed based on the strategy
        match self.strategy.kind {
            PollingKind::Offset => {
                self.messages
                    .iter()
                    .skip(self.strategy.value as usize)
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::First => {
                self.messages
                    .iter()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::Next => {
                self.messages
                    .iter()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            PollingKind::Last => {
                self.messages
                    .iter()
                    .rev()
                    .take(self.message_count)
                    .fold(status, |status, message| status.stdout(contains(message)));
            }
            _ => {}
        }
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

    let test_messages: Vec<String> = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit".into(),
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua".into(),
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris".into(),
        "nisi ut aliquip ex ea commodo consequat".into(),
        "Duis aute irure dolor in reprehenderit in voluptate velit esse".into(),
        "cillum dolore eu fugiat nulla pariatur".into(),
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa".into(),
        "qui officia deserunt mollit anim id est laborum".into(),
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem".into(),
        "accusantium doloremque laudantium, totam rem aperiam, eaque ipsa".into(),
    ];

    let test_headers = (
        HeaderKey::from_str("key1").unwrap(),
        HeaderValue::from_str("value1").unwrap(),
    );

    let test_parameters: Vec<(u32, usize, PollingStrategy, TestStreamId, TestTopicId, bool)> = vec![
        (
            1,
            1,
            PollingStrategy::offset(0),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            true,
        ),
        (
            2,
            5,
            PollingStrategy::offset(0),
            TestStreamId::Numeric,
            TestTopicId::Named,
            true,
        ),
        (
            3,
            3,
            PollingStrategy::offset(3),
            TestStreamId::Named,
            TestTopicId::Numeric,
            true,
        ),
        (
            4,
            5,
            PollingStrategy::first(),
            TestStreamId::Named,
            TestTopicId::Named,
            true,
        ),
        (
            1,
            4,
            PollingStrategy::last(),
            TestStreamId::Numeric,
            TestTopicId::Numeric,
            true,
        ),
        (
            2,
            3,
            PollingStrategy::next(),
            TestStreamId::Numeric,
            TestTopicId::Named,
            false,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (partition_id, message_count, strategy, using_stream_id, using_topic_id, show_headers) in
        test_parameters
    {
        iggy_cmd_test
            .execute_test(TestMessagePollCmd::new(
                1,
                String::from("stream"),
                2,
                String::from("topic"),
                4,
                &test_messages,
                partition_id,
                message_count,
                strategy,
                using_stream_id,
                using_topic_id,
                show_headers,
                test_headers.clone(),
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
            vec!["message", "poll", "--help"],
            format!(
                r#"Poll messages from given topic ID and given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples:
 iggy message poll --offset 0 1 2 1
 iggy message poll --offset 0 stream 2 1
 iggy message poll --offset 0 1 topic 1
 iggy message poll --offset 0 stream topic 1

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>
          ID of the stream from which message will be polled
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          ID of the topic from which message will be polled
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  <PARTITION_ID>
          Partition ID from which message will be polled

Options:
  -m, --message-count <MESSAGE_COUNT>
          Number of messages to poll
{CLAP_INDENT}
          [default: 1]

  -a, --auto-commit
          Auto commit offset
{CLAP_INDENT}
          Flag indicates whether to commit offset on the server automatically
          after polling the messages.

  -o, --offset <OFFSET>
          Polling strategy - offset to start polling messages from
{CLAP_INDENT}
          Offset must be specified as a number

  -f, --first
          Polling strategy - start polling from the first message in the partition

  -l, --last
          Polling strategy - start polling from the last message in the partition

  -n, --next
          Polling strategy - start polling from the next message
{CLAP_INDENT}
          Start polling after the last polled message based
          on the stored consumer offset

  -c, --consumer <CONSUMER>
          Regular consumer which will poll messages
{CLAP_INDENT}
          Consumer ID can be specified as a consumer name or ID
{CLAP_INDENT}
          [default: 1]

  -s, --show-headers
          Include the message headers in the output
{CLAP_INDENT}
          Flag indicates whether to include headers in the output
          after polling the messages.

      --output-file <OUTPUT_FILE>
          Store polled message into file in binary format
{CLAP_INDENT}
          Polled messages will be stored in the file in binary format.
          File can be used to replay the messages later. If the file
          already exists, the messages will be appended to the file.
          If the file does not exist, it will be created.
          If the file is not specified, the messages will be printed
          to the standard output.

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
            vec!["message", "poll", "-h"],
            format!(
                r#"Poll messages from given topic ID and given stream ID

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

Arguments:
  <STREAM_ID>     ID of the stream from which message will be polled
  <TOPIC_ID>      ID of the topic from which message will be polled
  <PARTITION_ID>  Partition ID from which message will be polled

Options:
  -m, --message-count <MESSAGE_COUNT>  Number of messages to poll [default: 1]
  -a, --auto-commit                    Auto commit offset
  -o, --offset <OFFSET>                Polling strategy - offset to start polling messages from
  -f, --first                          Polling strategy - start polling from the first message in the partition
  -l, --last                           Polling strategy - start polling from the last message in the partition
  -n, --next                           Polling strategy - start polling from the next message
  -c, --consumer <CONSUMER>            Regular consumer which will poll messages [default: 1]
  -s, --show-headers                   Include the message headers in the output
      --output-file <OUTPUT_FILE>      Store polled message into file in binary format
  -h, --help                           Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
