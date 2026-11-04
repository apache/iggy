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
use futures_util::StreamExt;
use iggy::clients::client::IggyClient;
use iggy::prelude::{IggyConsumer, ReadError, ReceivedMessage};
use iggy_binary_protocol::WireUserHeaders;
use iggy_common::Client;
use iggy_common::{
    CommitPolicy, Consumer, HeaderKey, HeaderKind, Identifier, IggyByteSize, IggyDuration,
    IggyMessage, IggyTimestamp, PollOptions, PollingStrategy, Sizeable,
    wire_conversions::user_headers_from_wire,
};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time;
use tracing::{Level, event};

/// Idle time after which a group poll without `--follow` stops. A new group member can take
/// several seconds to receive its first message.
const GROUP_IDLE_WAIT: Duration = Duration::from_secs(5);

pub struct PollMessagesCmd {
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: Option<u32>,
    consumer: Consumer,
    strategy: PollingStrategy,
    options: PollOptions,
    show_headers: bool,
    output_file: Option<String>,
    consumer_group: Option<String>,
    follow: bool,
}

impl PollMessagesCmd {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        message_count: u32,
        auto_commit: bool,
        offset: Option<u64>,
        first: bool,
        last: bool,
        next: bool,
        consumer: Identifier,
        show_headers: bool,
        output_file: Option<String>,
        consumer_group: Option<String>,
        follow: bool,
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
        // Follow mode keeps the SDK batch size, so a backlog is not read one message per trip.
        let count = if follow {
            PollOptions::default().count
        } else {
            message_count
        };
        Self {
            stream_id,
            topic_id,
            partition_id,
            consumer: Consumer::new(consumer),
            strategy,
            options: PollOptions {
                count,
                commit,
                ..PollOptions::default()
            },
            show_headers,
            output_file,
            consumer_group,
            follow,
        }
    }

    fn build_consumer_group(
        &self,
        client: &IggyClient,
        group: &str,
    ) -> anyhow::Result<IggyConsumer> {
        // The builder takes names only; stream and topic are set after the placeholders.
        let consumer = client
            .consumer_group(group, group, group)
            .with_context(|| format!("Invalid consumer group name: {group}"))?
            .stream(self.stream_id.clone())
            .topic(self.topic_id.clone())
            .polling_strategy(self.strategy)
            .poll_options(self.options)
            .do_not_create_consumer_group_if_not_exists()
            .build();
        Ok(consumer)
    }

    async fn poll_consumer_group(&self, client: &IggyClient, group: &str) -> anyhow::Result<()> {
        let mut consumer = self.build_consumer_group(client, group)?;
        let polled = self.receive_group_messages(&mut consumer, group).await;
        consumer
            .shutdown()
            .await
            .with_context(|| format!("Problem leaving consumer group: {group}"))?;
        polled
    }

    async fn receive_group_messages(
        &self,
        consumer: &mut IggyConsumer,
        group: &str,
    ) -> anyhow::Result<()> {
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        tokio::select! {
            _ = &mut interrupt => return Ok(()),
            init = consumer.init() => init
                .with_context(|| format!("Problem joining consumer group: {group}"))?,
        }

        let mut output = match self.output_file.as_deref() {
            Some(path) => Some(OutputFile::open(path).await?),
            None => None,
        };
        if self.follow && output.is_none() {
            event!(target: PRINT_TARGET, Level::INFO, "Offset\tTimestamp\tID\tLength\tPayload");
        }
        let count = self.options.count as usize;
        let idle = (!self.follow).then_some(GROUP_IDLE_WAIT);
        let mut messages = Vec::with_capacity(count);
        let start = Instant::now();
        loop {
            if !self.follow && messages.len() >= count {
                break;
            }
            let next = tokio::select! {
                _ = &mut interrupt => break,
                next = Self::next_message(consumer, idle) => next,
            };
            let Some(received) = next else {
                break;
            };
            let message = received
                .with_context(|| format!("Problem polling consumer group: {group}"))?
                .message;
            if self.follow {
                self.print_follow_message(&message, output.as_mut()).await?;
            } else {
                messages.push(message);
            }
        }

        if !self.follow {
            let elapsed = IggyDuration::new(start.elapsed());
            self.print_batch(&messages, elapsed, output.as_mut())
                .await?;
        }
        Ok(())
    }

    async fn print_follow_message(
        &self,
        message: &IggyMessage,
        output: Option<&mut OutputFile<'_>>,
    ) -> anyhow::Result<()> {
        match output {
            Some(output) => {
                let output_file = output.path;
                output
                    .store(std::slice::from_ref(message))
                    .await
                    .with_context(|| format!("Problem writing message to file: {output_file}"))?;
            }
            None => {
                let line = self.follow_line(message)?;
                event!(target: PRINT_TARGET, Level::INFO, "{line}");
            }
        }
        Ok(())
    }

    fn follow_line(&self, message: &IggyMessage) -> anyhow::Result<String> {
        let timestamp =
            IggyTimestamp::from(message.header.timestamp).to_local_string("%Y-%m-%d %H:%M:%S%.6f");
        let mut fields = vec![
            message.header.offset.to_string(),
            timestamp,
            message.header.id.to_string(),
            message.payload.len().to_string(),
            String::from_utf8_lossy(&message.payload).to_string(),
        ];
        if self.show_headers {
            let headers = message
                .user_headers_map()
                .context("Failed to parse user headers")?
                .unwrap_or_default();
            fields.extend(headers.iter().map(|(key, value)| {
                format!("{}={}", key.to_string_value(), value.to_string_value())
            }));
        }
        Ok(fields.join("\t"))
    }

    async fn next_message(
        consumer: &mut IggyConsumer,
        idle: Option<Duration>,
    ) -> Option<Result<ReceivedMessage, ReadError>> {
        match idle {
            None => consumer.next().await,
            Some(idle) => time::timeout(idle, consumer.next()).await.ok().flatten(),
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
        let partition_id = self
            .partition_id
            .context("Partition ID is required without --consumer-group")?;
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
                    partition_id,
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

    async fn execute_with_iggy_client(&mut self, client: &IggyClient) -> anyhow::Result<()> {
        match self.consumer_group.clone() {
            Some(group) => self.poll_consumer_group(client, &group).await,
            None => self.execute_cmd(client).await,
        }
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

    #[test]
    fn given_follow_message_when_formatted_should_be_one_tab_separated_row() {
        let cmd = PollMessagesCmd::new(
            Identifier::numeric(1).unwrap(),
            Identifier::numeric(1).unwrap(),
            None,
            1,
            false,
            None,
            true,
            false,
            false,
            Identifier::default(),
            false,
            None,
            Some("group".to_owned()),
            true,
        );

        let line = cmd.follow_line(&IggyMessage::from("hello")).unwrap();

        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[3], "5");
        assert_eq!(fields[4], "hello");
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
