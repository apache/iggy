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
use comfy_table::{Cell, CellAlignment, Row, Table};
use iggy_common::{
    BytesSerializable, Consumer, HeaderKey, HeaderKind, HeaderValue, Identifier, IggyByteSize,
    IggyDuration, IggyMessage, IggyTimestamp, PollMessages, PollingStrategy, Sizeable,
};
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncWriteExt;
use tracing::{Level, event};

pub struct PollMessagesCmd {
    poll_messages: PollMessages,
    show_headers: bool,
    output_file: Option<String>,
}

impl PollMessagesCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: usize,
        message_count: u32,
        auto_commit: bool,
        offset: Option<u64>,
        first: bool,
        last: bool,
        next: bool,
        consumer: Identifier,
        show_headers: bool,
        output_file: Option<String>,
    ) -> Self {
        let strategy = match (offset, first, last, next) {
            (Some(offset), false, false, false) => PollingStrategy::offset(offset),
            (None, true, false, false) => PollingStrategy::first(),
            (None, false, true, false) => PollingStrategy::last(),
            (None, false, false, true) => PollingStrategy::next(),
            _ => unreachable!("Either offset or first, last or next must be specified"),
        };
        Self {
            poll_messages: PollMessages {
                consumer: Consumer::new(consumer),
                stream_id,
                topic_id,
                partition_id: Some(partition_id),
                strategy,
                count: message_count,
                auto_commit,
            },
            show_headers,
            output_file,
        }
    }

    fn create_message_header_keys(
        &self,
        polled_messages: &[IggyMessage],
    ) -> HashSet<(HeaderKey, HeaderKind)> {
        if !self.show_headers {
            return HashSet::new();
        }

        polled_messages
            .iter()
            .flat_map(|m| {
                if let Some(user_headers) = &m.user_headers {
                    match HashMap::<HeaderKey, HeaderValue>::from_bytes(user_headers.clone()) {
                        Ok(headers) => headers
                            .iter()
                            .map(|(k, v)| (k.clone(), v.kind))
                            .collect::<Vec<_>>(),
                        Err(e) => {
                            tracing::error!("Failed to parse user headers, error: {e}");
                            vec![]
                        }
                    }
                } else {
                    vec![]
                }
            })
            .collect::<HashSet<_>>()
    }

    fn create_table_header(header_key_set: &HashSet<(HeaderKey, HeaderKind)>) -> Row {
        let mut table_header = vec![
            Cell::new("Offset"),
            Cell::new("Timestamp"),
            Cell::new("ID"),
            Cell::new("Length"),
            Cell::new("Payload"),
        ];
        let message_headers = header_key_set
            .iter()
            .map(|(key, kind)| {
                Cell::new(format!("Header: {}\n{}", key.as_str(), kind))
                    .set_alignment(CellAlignment::Center)
            })
            .collect::<Vec<_>>();
        table_header.extend(message_headers);
        Row::from(table_header)
    }

    fn create_table_content(
        polled_messages: &[IggyMessage],
        message_header_keys: &HashSet<(HeaderKey, HeaderKind)>,
    ) -> Vec<Row> {
        polled_messages
            .iter()
            .map(|message| {
                let mut row = vec![
                    format!("{}", message.header.offset),
                    IggyTimestamp::from(message.header.timestamp)
                        .to_local_string("%Y-%m-%d %H:%M:%S%.6f"),
                    format!("{}", message.header.id),
                    format!("{}", message.payload.len()),
                    String::from_utf8_lossy(&message.payload).to_string(),
                ];

                let values = message_header_keys
                    .iter()
                    .map(|(key, kind)| {
                        message
                            .user_headers_map()
                            .expect("Failed to parse user headers")
                            .as_ref()
                            .map(|h| {
                                h.get(key)
                                    .filter(|v| v.kind == *kind)
                                    .map(|v| v.value_only_to_string())
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>();
                row.extend(values);
                Row::from(row)
            })
            .collect::<_>()
    }
}

#[async_trait]
impl CliCommand for PollMessagesCmd {
    fn explain(&self) -> String {
        format!(
            "poll messages from topic ID: {} and stream with ID: {}",
            self.poll_messages.topic_id, self.poll_messages.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let start = std::time::Instant::now();
        let polled_messages = client
            .poll_messages(
                &self.poll_messages.stream_id,
                &self.poll_messages.topic_id,
                self.poll_messages.partition_id,
                &self.poll_messages.consumer,
                &self.poll_messages.strategy,
                self.poll_messages.count,
                self.poll_messages.auto_commit,
            )
            .await
            .with_context(|| {
                format!(
                    "Problem polling messages to topic with ID: {} and stream with ID: {}",
                    self.poll_messages.topic_id, self.poll_messages.stream_id
                )
            })?;
        let elapsed = IggyDuration::new(start.elapsed());

        event!(target: PRINT_TARGET, Level::INFO,
            "Polled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})",
            self.poll_messages.topic_id,
            self.poll_messages.stream_id,
            polled_messages.partition_id,
        );

        let polled_size = IggyByteSize::from(
            polled_messages
                .messages
                .iter()
                .map(|m| m.get_size_bytes().as_bytes_u64())
                .sum::<u64>(),
        );

        let message_count_message = match polled_messages.messages.len() {
            1 => "1 message".into(),
            count => format!("{count} messages"),
        };
        event!(target: PRINT_TARGET, Level::INFO, "Polled {message_count_message} of total size {polled_size}, it took {}", elapsed.as_human_time_string());

        if let Some(output_file) = &self.output_file {
            event!(target: PRINT_TARGET, Level::INFO, "Storing messages to {output_file} binary file");

            let mut saved_size = IggyByteSize::default();
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(output_file)
                .await
                .with_context(|| format!("Problem opening file for writing: {output_file}"))?;

            for message in polled_messages.messages.iter() {
                let message = message.to_bytes();
                saved_size += IggyByteSize::from(message.len() as u64);
                file.write_all(&message)
                    .await
                    .with_context(|| format!("Problem writing message to file: {output_file}"))?;
            }

            let saved_size_str = saved_size.as_human_string();
            event!(target: PRINT_TARGET, Level::INFO, "Stored {message_count_message} of total size {saved_size_str} to {output_file} binary file");
        } else {
            let message_header_keys = self.create_message_header_keys(&polled_messages.messages);

            let mut table = Table::new();
            let table_header = Self::create_table_header(&message_header_keys);
            let table_content =
                Self::create_table_content(&polled_messages.messages, &message_header_keys);
            table.set_header(table_header);
            table.add_rows(table_content);

            event!(target: PRINT_TARGET, Level::INFO, "{table}");
        }

        Ok(())
    }
}
