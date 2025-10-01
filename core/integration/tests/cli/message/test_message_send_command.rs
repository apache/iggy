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
use iggy::prelude::*;
use predicates::str::diff;
use serial_test::parallel;
use std::collections::HashMap;
use std::str::from_utf8;
use twox_hash::XxHash32;

#[derive(Debug)]
enum PartitionSelection {
    Balanced,
    Id(u32),
    Key(String),
}

impl PartitionSelection {
    fn to_args(&self) -> Vec<String> {
        match self {
            PartitionSelection::Balanced => vec![],
            PartitionSelection::Id(id) => vec!["-p".into(), format!("{}", id)],
            PartitionSelection::Key(key) => vec!["-m".into(), key.clone()],
        }
    }
}

#[derive(Debug)]
enum ProvideMessages {
    AsArgs,
    ViaStdin,
}

struct TestMessageSendCmd {
    stream_name: String,
    topic_name: String,
    partitions_count: u32,
    messages: Vec<String>,
    message_input: ProvideMessages,
    partitioning: PartitionSelection,
    header: Option<HashMap<HeaderKey, HeaderValue>>,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl TestMessageSendCmd {
    fn new(
        stream_name: String,
        topic_name: String,
        partitions_count: u32,
        messages: Vec<String>,
        message_input: ProvideMessages,
        partitioning: PartitionSelection,
        header: Option<HashMap<HeaderKey, HeaderValue>>,
    ) -> Self {
        Self {
            stream_name,
            topic_name,
            partitions_count,
            messages,
            message_input,
            partitioning,
            header,
            actual_stream_id: None,
            actual_topic_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = self.partitioning.to_args();

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

        if let Some(header) = &self.header {
            command.push("--headers".to_string());
            command.push(
                header
                    .iter()
                    .map(|(k, v)| format!("{k}:{}:{}", v.kind, v.value_only_to_string()))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        match &self.message_input {
            ProvideMessages::AsArgs => {
                command.extend(self.messages.clone());
            }
            ProvideMessages::ViaStdin => {}
        }

        command
    }

    fn calculate_partition_id_from_messages_key(&self, messages_key: &[u8]) -> u32 {
        let messages_key_hash = XxHash32::oneshot(0, messages_key);
        messages_key_hash % self.partitions_count
    }

    fn get_partition_id(&self) -> u32 {
        match &self.partitioning {
            PartitionSelection::Balanced => 1,
            PartitionSelection::Id(id) => *id,
            PartitionSelection::Key(key) => {
                self.calculate_partition_id_from_messages_key(key.clone().as_bytes())
            }
        }
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessageSendCmd {
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
            .arg("send")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn provide_stdin_input(&self) -> Option<Vec<String>> {
        match &self.message_input {
            ProvideMessages::ViaStdin => Some(self.messages.clone()),
            ProvideMessages::AsArgs => None,
        }
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

        let message = format!(
            "Executing send messages to topic with ID: {topic_id} and stream with ID: {stream_id}\nSent messages to topic with ID: {topic_id} and stream with ID: {stream_id}\n"
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .get_topic(
                &self.actual_stream_id.unwrap().try_into().unwrap(),
                &self.actual_topic_id.unwrap().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());
        let topic_details = topic.unwrap().expect("Failed to get topic");
        assert_eq!(topic_details.messages_count, self.messages.len() as u64);

        // For Balanced partitioning, messages are distributed across all partitions
        // so we need to poll from all partitions and collect messages
        let all_messages = match &self.partitioning {
            PartitionSelection::Balanced => {
                let mut collected_messages = Vec::new();
                for partition_id in 0..self.partitions_count {
                    let polled = client
                        .poll_messages(
                            &self.actual_stream_id.unwrap().try_into().unwrap(),
                            &self.actual_topic_id.unwrap().try_into().unwrap(),
                            Some(partition_id),
                            &Consumer::default(),
                            &PollingStrategy::offset(0),
                            self.messages.len() as u32,
                            false,
                        )
                        .await;
                    if let Ok(polled) = polled {
                        collected_messages.extend(polled.messages);
                    }
                }
                collected_messages
            }
            _ => {
                // For specific partition or key-based partitioning
                let polled_messages = client
                    .poll_messages(
                        &self.actual_stream_id.unwrap().try_into().unwrap(),
                        &self.actual_topic_id.unwrap().try_into().unwrap(),
                        Some(self.get_partition_id()),
                        &Consumer::default(),
                        &PollingStrategy::offset(0),
                        self.messages.len() as u32,
                        false,
                    )
                    .await;

                assert!(polled_messages.is_ok());
                polled_messages.unwrap().messages
            }
        };

        assert_eq!(all_messages.len(), self.messages.len());

        // For Balanced partitioning, messages may arrive in different order
        // so we just check that all expected messages are present
        let expected_messages: Vec<String> = self.messages.clone();
        let received_messages: Vec<String> = all_messages
            .iter()
            .map(|m| from_utf8(&m.payload.clone()).unwrap().to_string())
            .collect();

        match &self.partitioning {
            PartitionSelection::Balanced => {
                // For balanced, just check all messages are present (order may vary)
                for expected in &expected_messages {
                    assert!(
                        received_messages.contains(expected),
                        "Expected message '{}' not found in received messages",
                        expected
                    );
                }
            }
            _ => {
                // For specific partition, order should be preserved
                assert_eq!(received_messages, expected_messages);
            }
        }

        if let Some(expected_header) = &self.header {
            all_messages.iter().for_each(|m| {
                assert!(m.user_headers.is_some());
                assert_eq!(expected_header, &m.user_headers_map().unwrap().unwrap());
            })
        };

        let topic = client
            .delete_topic(
                &self.actual_stream_id.unwrap().try_into().unwrap(),
                &self.actual_topic_id.unwrap().try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.actual_stream_id.unwrap().try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_parameters = vec![
        (ProvideMessages::AsArgs, PartitionSelection::Balanced),
        (ProvideMessages::ViaStdin, PartitionSelection::Balanced),
        (ProvideMessages::ViaStdin, PartitionSelection::Id(0)),
        (ProvideMessages::AsArgs, PartitionSelection::Id(1)),
        (
            ProvideMessages::AsArgs,
            PartitionSelection::Key(String::from("some-complex-key")),
        ),
        (
            ProvideMessages::ViaStdin,
            PartitionSelection::Key(String::from("another-key")),
        ),
    ];

    iggy_cmd_test.setup().await;
    for (message_input, using_partitioning) in test_parameters {
        iggy_cmd_test
            .execute_test(TestMessageSendCmd::new(
                String::from("stream"),
                String::from("topic"),
                4,
                vec![
                    String::from("test message1"),
                    String::from("test message2"),
                    String::from("test message3"),
                ],
                message_input,
                using_partitioning,
                Some(HashMap::from([
                    (
                        HeaderKey::new("key1").unwrap(),
                        HeaderValue::from_kind_str_and_value_str("string", "value1").unwrap(),
                    ),
                    (
                        HeaderKey::new("key2").unwrap(),
                        HeaderValue::from_kind_str_and_value_str("int32", "42").unwrap(),
                    ),
                ])),
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
            vec!["message", "send", "--help"],
            format!(
                r#"Send messages to given topic ID and given stream ID

Stream ID can be specified as a stream name or ID
Topic ID can be specified as a topic name or ID

Examples
 iggy message send 1 2 message
 iggy message send stream 2 "long message"
 iggy message send 1 topic message1 message2 message3
 iggy message send stream topic "long message with spaces"

{USAGE_PREFIX} message send [OPTIONS] <STREAM_ID> <TOPIC_ID> [MESSAGES]...

Arguments:
  <STREAM_ID>
          ID of the stream to which the message will be sent
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

  <TOPIC_ID>
          ID of the topic to which the message will be sent
{CLAP_INDENT}
          Topic ID can be specified as a topic name or ID

  [MESSAGES]...
          Messages to be sent
{CLAP_INDENT}
          If no messages are provided, the command will read the messages from the
          standard input and each line will be sent as a separate message.
          If messages are provided, they will be sent as is. If message contains
          spaces, it should be enclosed in quotes. Limit of the messages and size
          of each message is defined by the used shell.

Options:
  -p, --partition-id <PARTITION_ID>
          ID of the partition to which the message will be sent

  -m, --message-key <MESSAGE_KEY>
          Messages key which will be used to partition the messages
{CLAP_INDENT}
          Value of the key will be used by the server to calculate the partition ID

  -H, --headers <HEADERS>
          Comma separated list of key:kind:value, sent as header with the message
{CLAP_INDENT}
          Headers are comma separated key-value pairs that can be sent with the message.
          Kind can be one of the following: raw, string, bool, int8, int16, int32, int64,
          int128, uint8, uint16, uint32, uint64, uint128, float32, float64

      --input-file <INPUT_FILE>
          Input file with messages to be sent
{CLAP_INDENT}
          File should contain messages stored in binary format. If the file does
          not exist, the command will fail. If the file is not specified, the command
          will read the messages from the standard input and each line will
          be sent as a separate message. If the file is specified, the messages
          will be read from the file and sent as is. Option cannot be used
          with the messages option (messages given as command line arguments).

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
            vec!["message", "send", "-h"],
            format!(
                r#"Send messages to given topic ID and given stream ID

{USAGE_PREFIX} message send [OPTIONS] <STREAM_ID> <TOPIC_ID> [MESSAGES]...

Arguments:
  <STREAM_ID>    ID of the stream to which the message will be sent
  <TOPIC_ID>     ID of the topic to which the message will be sent
  [MESSAGES]...  Messages to be sent

Options:
  -p, --partition-id <PARTITION_ID>  ID of the partition to which the message will be sent
  -m, --message-key <MESSAGE_KEY>    Messages key which will be used to partition the messages
  -H, --headers <HEADERS>            Comma separated list of key:kind:value, sent as header with the message
      --input-file <INPUT_FILE>      Input file with messages to be sent
  -h, --help                         Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
