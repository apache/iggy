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

use crate::Client;
use crate::cli::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::{
    BytesSerializable, HeaderKey, HeaderValue, Identifier, IggyMessage, Partitioning, Sizeable,
};
use std::collections::HashMap;
use std::io::{self, Read};
use tokio::io::AsyncReadExt;
use tracing::{Level, event};

pub struct SendMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partitioning: Partitioning,
    messages: Option<Vec<String>>,
    headers: Vec<(HeaderKey, HeaderValue)>,
    input_file: Option<String>,
}

impl SendMessagesCmd {
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<usize>,
        message_key: Option<String>,
        messages: Option<Vec<String>>,
        headers: Vec<(HeaderKey, HeaderValue)>,
        input_file: Option<String>,
    ) -> Self {
        let partitioning = match (partition_id, message_key) {
            (Some(_), Some(_)) => unreachable!(),
            (Some(partition_id), None) => Partitioning::partition_id(partition_id),
            (None, Some(message_key)) => Partitioning::messages_key_str(message_key.as_str())
                .unwrap_or_else(|_| {
                    panic!("Failed to create Partitioning with {message_key} string message key")
                }),
            (None, None) => Partitioning::default(),
        };
        Self {
            stream_id,
            topic_id,
            partitioning,
            messages,
            headers,
            input_file,
        }
    }

    fn read_message_from_stdin(&self) -> Result<String, io::Error> {
        let mut buffer = String::new();

        io::stdin().read_to_string(&mut buffer)?;

        Ok(buffer)
    }

    fn get_headers(&self) -> Option<HashMap<HeaderKey, HeaderValue>> {
        match self.headers.len() {
            0 => None,
            _ => Some(self.headers.iter().cloned().collect()),
        }
    }
}

#[async_trait]
impl CliCommand for SendMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "send messages to topic with ID: {} and stream with ID: {}",
            self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let mut messages = if let Some(input_file) = &self.input_file {
            let mut file = tokio::fs::OpenOptions::new()
                .read(true)
                .open(input_file)
                .await
                .with_context(|| format!("Problem opening file for reading: {input_file}"))?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)
                .await
                .with_context(|| format!("Problem reading file: {input_file}"))?;

            event!(target: PRINT_TARGET, Level::INFO,
                "Read {} bytes from {} file", buffer.len(), input_file,
            );

            let mut messages: Vec<IggyMessage> = Vec::new();
            let mut bytes_read = 0usize;
            let all_messages_bytes: Bytes = buffer.into();

            while bytes_read < all_messages_bytes.len() {
                let message_bytes = all_messages_bytes.slice(bytes_read..);
                let message = IggyMessage::from_bytes(message_bytes);
                match message {
                    Ok(message) => {
                        let message_size = message.get_size_bytes().as_bytes_usize();
                        messages.push(message);
                        bytes_read += message_size;
                    }
                    Err(e) => {
                        event!(target: PRINT_TARGET, Level::ERROR,
                            "Failed to parse message from bytes: {e} at offset {bytes_read}",
                        );
                        break;
                    }
                }
            }
            event!(target: PRINT_TARGET, Level::INFO,
                "Created {} messages using {bytes_read} bytes", messages.len(),
            );

            messages
        } else {
            let headers = self.get_headers();
            match &self.messages {
                Some(messages) => messages
                    .iter()
                    .map(|s| {
                        IggyMessage::builder()
                            .payload(Bytes::from(s.clone()))
                            .user_headers(headers.clone().unwrap_or_default())
                            .build()
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .with_context(|| "Failed to create messages from provided strings")?,
                None => {
                    let input = self.read_message_from_stdin()?;

                    input
                        .lines()
                        .map(|m| {
                            IggyMessage::builder()
                                .payload(m.to_owned().into())
                                .user_headers(headers.clone().unwrap_or_default())
                                .build()
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .with_context(|| "Failed to create messages from stdin")?
                }
            }
        };

        client
            .send_messages(
                &self.stream_id,
                &self.topic_id,
                &self.partitioning,
                &mut messages,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem sending messages to topic with ID: {} and stream with ID: {}",
                    self.topic_id, self.stream_id
                )
            })?;

        event!(target: PRINT_TARGET, Level::INFO,
            "Sent messages to topic with ID: {} and stream with ID: {}",
            self.topic_id,
            self.stream_id,
        );

        Ok(())
    }
}
