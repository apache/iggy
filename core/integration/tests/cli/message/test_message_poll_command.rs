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
    CLAP_INDENT, IggyCmdCommand, IggyCmdTest, IggyCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::*;
use predicates::boolean::PredicateBooleanExt;
use predicates::str::{contains, starts_with};
use serial_test::parallel;
use std::collections::BTreeMap;
use std::str::FromStr;

/// The seeded messages are sent as three separate batches, so the broker stamps
/// three distinct timestamps. A single batch would make a timestamp poll
/// indistinguishable from one that ignores the strategy: every message would
/// share the bound, and "at or after it" would equal "all of them".
const SEED_BATCH_SIZES: [usize; 3] = [3, 4, 3];

struct TestMessagePollCmd {
    stream_name: String,
    topic_name: String,
    partitions_count: u32,
    messages: Vec<String>,
    partition_id: u32,
    message_count: usize,
    strategy: PollingStrategy,
    show_headers: bool,
    headers: (HeaderKey, HeaderValue),
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
    /// Broker append time of the batch the timestamp poll cuts at, read back
    /// after the sends.
    boundary_timestamp: Option<u64>,
}

impl TestMessagePollCmd {
    #[allow(clippy::too_many_arguments)]
    fn new(
        stream_name: String,
        topic_name: String,
        partitions_count: u32,
        messages: &[String],
        partition_id: u32,
        message_count: usize,
        strategy: PollingStrategy,
        show_headers: bool,
        headers: (HeaderKey, HeaderValue),
    ) -> Self {
        assert!(partition_id < partitions_count);
        assert!(message_count < messages.len());
        assert_eq!(
            SEED_BATCH_SIZES.iter().sum::<usize>(),
            messages.len(),
            "seed batch sizes must cover every message"
        );
        Self {
            stream_name,
            topic_name,
            partitions_count,
            messages: messages.to_owned(),
            partition_id,
            message_count,
            strategy,
            show_headers,
            headers,
            actual_stream_id: None,
            actual_topic_id: None,
            boundary_timestamp: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = match self.strategy.kind {
            PollingKind::Offset => vec!["--offset".into(), format!("{}", self.strategy.value)],
            PollingKind::Timestamp => {
                vec!["--timestamp".into(), format!("{}", self.strategy.value)]
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
impl IggyCmdTestCase for TestMessagePollCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, &StreamCreateOptions::default())
            .await;
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        self.actual_stream_id = Some(stream.id);

        let topic = client
            .create_topic(
                &stream.id.try_into().unwrap(),
                &self.topic_name,
                &TopicCreateOptions {
                    partitions_count: Some(self.partitions_count),
                    message_expiry: Some(IggyExpiry::NeverExpire),
                    ..TopicCreateOptions::default()
                },
            )
            .await;
        assert!(topic.is_ok());
        let topic = topic.unwrap();
        self.actual_topic_id = Some(topic.id);

        let mut messages = self
            .messages
            .iter()
            .map(|s| {
                let payload = Bytes::from(s.as_bytes().to_vec());
                IggyMessage::builder()
                    .payload(payload)
                    .user_headers(BTreeMap::from([self.headers.clone()]))
                    .build()
                    .expect("Failed to create message with headers")
            })
            .collect::<Vec<_>>();

        let mut seeded = 0;
        for batch_size in SEED_BATCH_SIZES {
            let send_status = client
                .send_messages(
                    &stream.id.try_into().unwrap(),
                    &topic.id.try_into().unwrap(),
                    &Partitioning::partition_id(self.partition_id),
                    &mut messages[seeded..seeded + batch_size],
                )
                .await;
            assert!(send_status.is_ok());
            seeded += batch_size;
        }

        // The broker stamps `base_timestamp` once per batch, so the exact value a
        // timestamp poll is compared against does not exist until after the send.
        // Read it back rather than guessing a wall-clock bound: a guess never
        // equals a batch, and so cannot tell `>=` from `>`.
        if self.strategy.kind == PollingKind::Timestamp {
            let polled = client
                .poll_messages(
                    &stream.id.try_into().unwrap(),
                    &topic.id.try_into().unwrap(),
                    Some(self.partition_id),
                    &Consumer::default(),
                    &PollingStrategy::first(),
                    u32::try_from(self.messages.len()).expect("seed count fits in u32"),
                    false,
                )
                .await
                .expect("reading back the seeded batches");
            assert_eq!(
                polled.messages.len(),
                self.messages.len(),
                "read-back must return every seeded message"
            );
            let boundary = SEED_BATCH_SIZES[0];
            let boundary_timestamp = polled.messages[boundary].header.timestamp;
            assert!(
                boundary_timestamp > polled.messages[0].header.timestamp,
                "the second batch messages timestamp should bigger than the first batch's"
            );
            self.boundary_timestamp = Some(boundary_timestamp);
            self.strategy.set_value(boundary_timestamp);
        }
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("poll")
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

        let polled_status = match self.message_count {
            1 => "Polled 1 message".into(),
            _ => format!("Polled {} messages", self.message_count),
        };

        let message = format!(
            "Executing poll messages from topic ID: {} and stream with ID: {}\nPolled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})\n{polled_status}",
            topic_id, stream_id, topic_id, stream_id, self.partition_id
        );

        let mut status = command_state.success().stdout(starts_with(message));

        if self.show_headers {
            status = status
                .stdout(contains(format!(
                    "Header: {}",
                    self.headers.0.to_string_value()
                )))
                .stdout(contains(self.headers.1.kind().to_string()))
                .stdout(contains(self.headers.1.to_string_value()).count(self.message_count))
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
            PollingKind::Timestamp => {
                // Left at the `0` placeholder the poll would return every message
                // and pass vacuously, so require the back-filled bound.
                let boundary_timestamp = self
                    .boundary_timestamp
                    .expect("timestamp strategy was not back-filled from the seeded batches");
                assert_eq!(self.strategy.value, boundary_timestamp);
                let boundary = SEED_BATCH_SIZES[0];
                let status = self.messages[boundary..]
                    .iter()
                    .fold(status, |status, message| status.stdout(contains(message)));
                self.messages[..boundary]
                    .iter()
                    .fold(status, |status, message| {
                        status.stdout(contains(message).not())
                    });
            }
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

    let test_parameters: Vec<(u32, usize, PollingStrategy, bool)> = vec![
        (0, 1, PollingStrategy::offset(0), true),
        (1, 5, PollingStrategy::offset(0), true),
        (2, 3, PollingStrategy::offset(3), true),
        (3, 5, PollingStrategy::first(), true),
        (0, 4, PollingStrategy::last(), true),
        (1, 3, PollingStrategy::next(), false),
        // The timestamp value is a placeholder; `prepare_server_state` replaces it
        // with the boundary batch's broker-stamped `base_timestamp`. The count is
        // everything from the cut onwards, derived rather than hand-summed so a
        // change to `SEED_BATCH_SIZES` cannot silently leave it behind.
        (
            3,
            SEED_BATCH_SIZES[1] + SEED_BATCH_SIZES[2],
            PollingStrategy::timestamp(IggyTimestamp::from(0)),
            true,
        ),
    ];

    iggy_cmd_test.setup().await;
    for (partition_id, message_count, strategy, show_headers) in test_parameters {
        iggy_cmd_test
            .execute_test(TestMessagePollCmd::new(
                String::from("stream"),
                String::from("topic"),
                4,
                &test_messages,
                partition_id,
                message_count,
                strategy,
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

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next|--timestamp <TIMESTAMP>> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

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

      --timestamp <TIMESTAMP>
          Polling strategy - start polling at or after this time
{CLAP_INDENT}
          Accepts an RFC 3339 time, Unix microseconds, or a duration relative to
          now such as 1h or 30min.
{CLAP_INDENT}
          [alias: --since]

  -c, --consumer <CONSUMER>
          Regular consumer which will poll messages
{CLAP_INDENT}
          Consumer ID can be specified as a consumer name or ID
{CLAP_INDENT}
          [default: 0]

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

{USAGE_PREFIX} message poll [OPTIONS] <--offset <OFFSET>|--first|--last|--next|--timestamp <TIMESTAMP>> <STREAM_ID> <TOPIC_ID> <PARTITION_ID>

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
      --timestamp <TIMESTAMP>          Polling strategy - start polling at or after this time [alias: --since]
  -c, --consumer <CONSUMER>            Regular consumer which will poll messages [default: 0]
  -s, --show-headers                   Include the message headers in the output
      --output-file <OUTPUT_FILE>      Store polled message into file in binary format
  -h, --help                           Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
