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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::{Cell, CellAlignment, Row, Table};
use iggy_binary_protocol::WireUserHeaders;
use iggy_common::Client;
use iggy_common::{
    CommitPolicy, Consumer, HeaderKey, HeaderKind, Identifier, IggyByteSize, IggyDuration,
    IggyMessage, IggyTimestamp, PollOptions, PollingStrategy, Sizeable,
    wire_conversions::user_headers_from_wire,
};
use std::collections::HashSet;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{Level, event};

pub struct PollMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: u32,
    consumer: Consumer,
    strategy: PollingStrategy,
    options: PollOptions,
    show_headers: bool,
    output_file: Option<String>,
}

impl PollMessagesCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: u32,
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
        let commit = if auto_commit {
            CommitPolicy::OnRead
        } else {
            CommitPolicy::Manual
        };
        Self {
            stream_id,
            topic_id,
            partition_id,
            consumer: Consumer::new(consumer),
            strategy,
            options: PollOptions {
                count: message_count,
                commit,
                ..PollOptions::default()
            },
            show_headers,
            output_file,
        }
    }

    async fn print_batch(
        &self,
        messages: &[IggyMessage],
        elapsed: IggyDuration,
        output: Option<&mut OutputFile<'_>>,
    ) -> anyhow::Result<(), anyhow::Error> {
        let polled_size = IggyByteSize::from(
            messages
                .iter()
                .map(|m| m.get_size_bytes().as_bytes_u64())
                .sum::<u64>(),
        );

        let message_count_message = match messages.len() {
            1 => "1 message".into(),
            count => format!("{count} messages"),
        };
        event!(target: PRINT_TARGET, Level::INFO, "Polled {message_count_message} of total size {polled_size}, it took {}", elapsed.as_human_time_string());

        if let Some(output) = output {
            let output_file = output.path;
            event!(target: PRINT_TARGET, Level::INFO, "Storing messages to {output_file} binary file");

            let saved_size = output
                .store(messages)
                .await
                .with_context(|| format!("Problem writing message to file: {output_file}"))?;

            let saved_size_str = saved_size.as_human_string();
            event!(target: PRINT_TARGET, Level::INFO, "Stored {message_count_message} of total size {saved_size_str} to {output_file} binary file");
        } else {
            let message_header_keys = self.create_message_header_keys(messages);

            let mut table = Table::new();
            let table_header = Self::create_table_header(&message_header_keys);
            let table_content = Self::create_table_content(messages, &message_header_keys);
            table.set_header(table_header);
            table.add_rows(table_content);

            event!(target: PRINT_TARGET, Level::INFO, "{table}");
        }

        Ok(())
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
                    match WireUserHeaders::from_bytes(user_headers.clone())
                        .map_err(|_| iggy_common::IggyError::InvalidHeaderKey)
                        .and_then(|w| user_headers_from_wire(&w))
                    {
                        Ok(headers) => headers
                            .iter()
                            .map(|(k, v)| (k.clone(), v.kind()))
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
                Cell::new(format!("Header: {}\n{}", key.to_string_value(), kind))
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
                                    .filter(|v| v.kind() == *kind)
                                    .map(|v| v.to_string_value())
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
            self.topic_id, self.stream_id
        )
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        // Before the first poll, so a file that does not open fails the command before
        // `--auto-commit` stores an offset.
        let mut output = match self.output_file.as_deref() {
            Some(path) => Some(OutputFile::open(path).await?),
            None => None,
        };
        let mut strategy = self.strategy;
        let mut polled_count = 0;
        // One reply holds at most `max_bytes`, so the command reads on until it has
        // `--message-count` messages or gets an empty batch.
        while polled_count < self.options.count {
            let start = std::time::Instant::now();
            let polled_messages = client
                .poll_messages(
                    &self.stream_id,
                    &self.topic_id,
                    self.partition_id,
                    &self.consumer,
                    &strategy,
                    PollOptions {
                        count: self.options.count - polled_count,
                        ..self.options
                    },
                )
                .await
                .with_context(|| {
                    format!(
                        "Problem polling messages to topic with ID: {} and stream with ID: {}",
                        self.topic_id, self.stream_id
                    )
                })?;
            let elapsed = IggyDuration::new(start.elapsed());

            // An empty first batch is still printed, so the operator sees that nothing matched.
            if polled_count == 0 {
                event!(target: PRINT_TARGET, Level::INFO,
                    "Polled messages from topic with ID: {} and stream with ID: {} (from partition with ID: {})",
                    self.topic_id,
                    self.stream_id,
                    polled_messages.partition_id,
                );
            } else if polled_messages.messages.is_empty() {
                break;
            }
            self.print_batch(&polled_messages.messages, elapsed, output.as_mut())
                .await?;
            if polled_messages.messages.is_empty() {
                break;
            }
            polled_count += polled_messages.messages.len() as u32;
            strategy = polled_messages.continuation();
        }

        Ok(())
    }
}

/// The `--output-file`, open for the whole command.
struct OutputFile<'a> {
    path: &'a str,
    writer: BufWriter<File>,
}

impl<'a> OutputFile<'a> {
    async fn open(path: &'a str) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await
            .with_context(|| format!("Problem opening file for writing: {path}"))?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Appends `messages` and flushes them. A file reports a failed write only at a later write
    /// or a flush, so the flush fails the command before it polls again.
    async fn store(&mut self, messages: &[IggyMessage]) -> std::io::Result<IggyByteSize> {
        let mut saved_size = IggyByteSize::default();
        for message in messages {
            let message = message.to_bytes();
            saved_size += IggyByteSize::from(message.len() as u64);
            self.writer.write_all(&message).await?;
        }
        self.writer.flush().await?;
        Ok(saved_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(payloads: &[&'static str]) -> Vec<IggyMessage> {
        payloads
            .iter()
            .map(|payload| IggyMessage::from(*payload))
            .collect()
    }

    fn stored_bytes(batches: &[&[IggyMessage]]) -> Vec<u8> {
        batches
            .iter()
            .flat_map(|batch| batch.iter())
            .flat_map(|message| message.to_bytes())
            .collect()
    }

    #[tokio::test]
    async fn given_batches_when_stored_should_be_in_the_file_in_order_before_it_closes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("messages.bin");
        let path = path.to_str().unwrap();
        let (first, second) = (batch(&["one", "two"]), batch(&["three"]));
        let mut output = OutputFile::open(path).await.unwrap();

        output.store(&first).await.unwrap();
        output.store(&second).await.unwrap();

        assert_eq!(
            tokio::fs::read(path).await.unwrap(),
            stored_bytes(&[&first, &second]),
            "each batch is in the file once it is stored"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn given_a_full_device_when_storing_a_batch_should_fail() {
        let mut output = OutputFile::open("/dev/full").await.unwrap();

        let error = output
            .store(&batch(&["lost"]))
            .await
            .expect_err("a write that the device refused must fail the command");

        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    }
}
