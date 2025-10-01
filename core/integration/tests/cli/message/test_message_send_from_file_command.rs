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
use predicates::str::{ends_with, is_match, starts_with};
use serial_test::parallel;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::io::AsyncWriteExt;

pub(super) struct TestMessageSendFromFileCmd<'a> {
    initialize: bool,
    input_file: String,
    stream_name: String,
    topic_name: String,
    messages: Vec<&'a str>,
    message_count: usize,
    headers: HashMap<HeaderKey, HeaderValue>,
    // These will be populated after creating the resources
    actual_stream_id: Option<u32>,
    actual_topic_id: Option<u32>,
}

impl<'a> TestMessageSendFromFileCmd<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        initialize: bool,
        input_file: &str,
        stream_name: &str,
        topic_name: &str,
        messages: &[&'a str],
        message_count: usize,
        headers: HashMap<HeaderKey, HeaderValue>,
    ) -> Self {
        assert!(message_count <= messages.len());
        Self {
            initialize,
            input_file: input_file.into(),
            stream_name: stream_name.into(),
            topic_name: topic_name.into(),
            messages: messages.to_owned(),
            message_count,
            headers,
            actual_stream_id: None,
            actual_topic_id: None,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut command = vec![
            "--input-file".into(),
            self.input_file.clone(),
            "--partition-id".into(),
            "0".into(),
        ];

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

        command
    }
}

#[async_trait]
impl IggyCmdTestCase for TestMessageSendFromFileCmd<'_> {
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

        if self.initialize {
            let file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.input_file)
                .await;
            assert!(
                file.is_ok(),
                "Problem opening file for writing: {}",
                self.input_file
            );
            let mut file = file.unwrap();

            let messages = self
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

            for message in messages.iter() {
                let message = IggyMessage::builder()
                    .id(message.header.id)
                    .payload(message.payload.clone())
                    .user_headers(message.user_headers_map().unwrap().unwrap())
                    .build()
                    .expect("Failed to create message with headers");

                let write_result = file.write_all(&message.to_bytes()).await;
                assert!(
                    write_result.is_ok(),
                    "Problem writing message to file: {}",
                    self.input_file
                );
            }
        }
    }

    fn get_command(&self) -> IggyCmdCommand {
        IggyCmdCommand::new()
            .arg("message")
            .arg("send")
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

        let message_prefix = format!(
            "Executing send messages to topic with ID: {} and stream with ID: {}\n",
            topic_id, stream_id
        );
        let message_read = format!("Read [0-9]+ bytes from {} file", self.input_file);
        let message_created = format!(
            "Created {} using [0-9]+ bytes",
            match self.message_count {
                1 => "1 message".into(),
                _ => format!("{} messages", self.message_count),
            }
        );
        let message_sent = format!(
            "Sent messages to topic with ID: {} and stream with ID: {}\n",
            topic_id, stream_id
        );

        command_state
            .success()
            .stdout(starts_with(message_prefix))
            .stdout(is_match(message_read).unwrap().count(1))
            .stdout(is_match(message_created).unwrap().count(1))
            .stdout(ends_with(message_sent));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        if let (Some(stream_id), Some(topic_id)) = (self.actual_stream_id, self.actual_topic_id) {
            let messages = client
                .poll_messages(
                    &stream_id.try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                    Some(0),
                    &Consumer::new(Identifier::default()),
                    &PollingStrategy::offset(0),
                    self.message_count as u32 * 2,
                    true,
                )
                .await;
            assert!(messages.is_ok());
            let messages = messages.unwrap();

            // Check if there are only the expected number of messages
            assert_eq!(messages.messages.len(), self.message_count);

            // Check message order and content (payload and headers)
            for (i, message) in messages.messages.iter().enumerate() {
                assert_eq!(
                    message.payload,
                    Bytes::from(self.messages[i].as_bytes().to_vec())
                );
                assert_eq!(
                    message.user_headers_map().unwrap().is_some(),
                    !self.headers.is_empty()
                );
                assert_eq!(&message.user_headers_map().unwrap().unwrap(), &self.headers);
            }

            let topic_delete = client
                .delete_topic(
                    &stream_id.try_into().unwrap(),
                    &topic_id.try_into().unwrap(),
                )
                .await;
            assert!(topic_delete.is_ok());

            let stream_delete = client.delete_stream(&stream_id.try_into().unwrap()).await;
            assert!(stream_delete.is_ok());
        }

        let file_removal = std::fs::remove_file(&self.input_file);
        assert!(file_removal.is_ok());
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
    ]);

    let temp_file = tempfile::Builder::new().tempfile().unwrap();
    let temp_path = temp_file.path().to_path_buf();
    temp_file.close().unwrap();
    let temp_path_str = temp_path.to_str().unwrap();

    iggy_cmd_test.setup().await;
    iggy_cmd_test
        .execute_test(TestMessageSendFromFileCmd::new(
            true,
            temp_path_str,
            "stream",
            "topic",
            &test_messages,
            test_messages.len(),
            test_headers,
        ))
        .await;
}
