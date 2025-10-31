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

use crate::cli::common::{IggyCmdCommand, IggyCmdTest, IggyCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use bytes::Bytes;
use iggy::prelude::*;
use predicates::str::{contains, is_match, starts_with};
use serial_test::parallel;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

pub(super) struct TestMessagePollToFileCmd<'a> {
    stream_name: String,
    topic_name: String,
    messages: Vec<&'a str>,
    message_count: usize,
    strategy: PollingStrategy,
    headers: HashMap<HeaderKey, HeaderValue>,
    output_file: String,
    cleanup: bool,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl<'a> TestMessagePollToFileCmd<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        stream_name: &str,
        topic_name: &str,
        messages: &[&'a str],
        message_count: usize,
        strategy: PollingStrategy,
        headers: HashMap<HeaderKey, HeaderValue>,
        output_file: &str,
        cleanup: bool,
    ) -> Self {
        assert!(message_count <= messages.len());
        Self {
            stream_name: stream_name.into(),
            topic_name: topic_name.into(),
            messages: messages.to_owned(),
            message_count,
            strategy,
            headers,
            output_file: output_file.into(),
            cleanup,
            actual_stream_id: None,
            actual_topic_id: None,
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

        command.extend(vec!["--output-file".into(), self.output_file.clone()]);

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

        command.push("0".into());

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessagePollToFileCmd<'_> {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client.create_stream(&self.stream_name).await;
        assert!(stream.is_ok());
        let stream = stream.unwrap();
        self.actual_stream_id = Some(stream.id);

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
                    .user_headers(self.headers.clone())
                    .build()
                    .expect("Failed to create message with headers")
            })
            .collect::<Vec<_>>();

        let send_status = client
            .send_messages(
                &stream.id.try_into().unwrap(),
                &topic.id.try_into().unwrap(),
                &Partitioning::partition_id(0),
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
        let polled_status = match self.message_count {
            1 => "Polled 1 message".into(),
            _ => format!("Polled {} messages", self.message_count),
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

        let message_prefix = format!(
            "Executing poll messages from topic ID: {} and stream with ID: {}\nPolled messages from topic with ID: {} and stream with ID: {} (from partition with ID: 0)\n{polled_status}",
            topic_id, stream_id, topic_id, stream_id
        );
        let message_file = format!("Storing messages to {} binary file", self.output_file);
        let message_count = format!(
            "Stored {} of total size [0-9.]+ K?B to {} binary file",
            match self.message_count {
                1 => "1 message".into(),
                _ => format!("{} messages", self.message_count),
            },
            self.output_file
        );

        command_state
            .success()
            .stdout(starts_with(message_prefix))
            .stdout(contains(message_file))
            .stdout(is_match(message_count).unwrap().count(1));
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

        assert!(Path::new(&self.output_file).is_file());
        if self.cleanup {
            let file_removal = std::fs::remove_file(&self.output_file);
            assert!(file_removal.is_ok());
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut iggy_cmd_test = IggyCmdTest::default();

    let test_messages: Vec<&str> = vec![
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris",
        "nisi ut aliquip ex ea commodo consequat",
        "Duis aute irure dolor in reprehenderit in voluptate velit esse",
        "cillum dolore eu fugiat nulla pariatur",
        "Excepteur sint occaecat cupidatat non proident, sunt in culpa",
        "qui officia deserunt mollit anim id est laborum",
        "Sed ut perspiciatis unde omnis iste natus error sit voluptatem",
        "accusantium doloremque laudantium, totam rem aperiam, eaque ipsa",
    ];

    let test_headers = HashMap::from([
        (
            HeaderKey::from_str("HeaderKey1").unwrap(),
            HeaderValue::from_str("HeaderValue1").unwrap(),
        ),
        (
            HeaderKey::from_str("HeaderKey2").unwrap(),
            HeaderValue::from_str("HeaderValue2").unwrap(),
        ),
        (
            HeaderKey::from_str("HeaderKey3").unwrap(),
            HeaderValue::from_str("HeaderValue3").unwrap(),
        ),
    ]);

    let test_parameters: Vec<(usize, PollingStrategy)> = vec![
        (1, PollingStrategy::offset(0)),
        (5, PollingStrategy::offset(0)),
        (3, PollingStrategy::offset(3)),
        (5, PollingStrategy::first()),
        (4, PollingStrategy::last()),
        (3, PollingStrategy::next()),
    ];

    iggy_cmd_test.setup().await;
    for (message_count, strategy) in test_parameters {
        let temp_file = tempfile::Builder::new().tempfile().unwrap();
        let temp_path = temp_file.path().to_path_buf();
        temp_file.close().unwrap();
        let temp_path_str = temp_path.to_str().unwrap();

        iggy_cmd_test
            .execute_test(TestMessagePollToFileCmd::new(
                "stream",
                "topic",
                &test_messages,
                message_count,
                strategy,
                test_headers.clone(),
                temp_path_str,
                true,
            ))
            .await;
    }
}
