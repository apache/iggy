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
    AutoCommit, CommitAfter, CommitPolicy, Consumer, HeaderKey, HeaderKind, Identifier,
    IggyByteSize, IggyDuration, IggyMessage, IggyTimestamp, NonZeroIggyDuration, PollOptions,
    PollingStrategy, Sizeable, wire_conversions::user_headers_from_wire,
};
use std::collections::HashSet;
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time;
use tracing::{Level, event};

/// Idle time after which a group poll without `--follow` stops. A new group member can take
/// several seconds to receive its first message.
const GROUP_IDLE_WAIT: Duration = Duration::from_secs(5);
/// Lets the server hold an idle group poll, so a new message shows without a client retry.
const GROUP_POLL_WAIT: Duration = Duration::from_secs(1);
/// Stored with `--auto-commit` at this interval, and again on shutdown.
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_secs(1);
const ROW_HEADER: &str = "Offset\tPartition\tTimestamp\tID\tLength\tPayload";

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
    auto_commit: bool,
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
            auto_commit,
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
            .poll_options(PollOptions {
                count: self.options.count,
                max_wait: IggyDuration::new(GROUP_POLL_WAIT),
                commit: self.group_commit_policy(),
                ..PollOptions::default()
            })
            .do_not_create_consumer_group_if_not_exists()
            .build();
        Ok(consumer)
    }

    /// A position is stored only for printed messages, so a run that is killed re-delivers
    /// messages instead of skipping them.
    fn group_commit_policy(&self) -> CommitPolicy {
        if !self.auto_commit {
            return CommitPolicy::Manual;
        }
        CommitPolicy::Auto(AutoCommit {
            interval: NonZeroIggyDuration::new(GROUP_COMMIT_INTERVAL).ok(),
            every: None,
            after: CommitAfter::Marked,
        })
    }

    async fn poll_consumer_group(&self, client: &IggyClient, group: &str) -> anyhow::Result<()> {
        // Opened before the join, so a bad path costs no join and no rebalance.
        let mut output = match self.output_file.as_deref() {
            Some(path) => Some(OutputFile::open(path).await?),
            None => None,
        };
        let mut consumer = self.build_consumer_group(client, group)?;
        let polled = self
            .receive_group_messages(&mut consumer, group, output.as_mut())
            .await;
        // Ctrl-C during the leave exits at once. The first Ctrl-C replaced the default SIGINT
        // action, so without this a second one would do nothing.
        let stopped = tokio::select! {
            stopped = consumer.shutdown() => stopped,
            _ = tokio::signal::ctrl_c() => std::process::exit(130),
        };
        // The poll error is the one to report. A failed leave is only logged by the SDK.
        polled?;
        stopped.with_context(|| format!("Problem stopping consumer group: {group}"))
    }

    async fn receive_group_messages(
        &self,
        consumer: &mut IggyConsumer,
        group: &str,
        mut output: Option<&mut OutputFile<'_>>,
    ) -> anyhow::Result<()> {
        let interrupt = tokio::signal::ctrl_c();
        tokio::pin!(interrupt);
        tokio::select! {
            _ = &mut interrupt => return Ok(()),
            init = consumer.init() => init
                .with_context(|| format!("Problem joining consumer group: {group}"))?,
        }

        if output.is_none() {
            write_line(ROW_HEADER).context("Problem writing to standard output")?;
        }
        let idle = (!self.follow).then_some(GROUP_IDLE_WAIT);
        let start = Instant::now();
        let mut last_arrival = start;
        let mut polled_count = 0usize;
        let mut polled_size = IggyByteSize::default();
        while self.follow || polled_count < self.options.count as usize {
            // Rows already read reach the file before the wait, so a long idle keeps none back.
            if let Some(output) = output.as_mut() {
                output
                    .flush()
                    .await
                    .with_context(|| format!("Problem writing message to file: {}", output.path))?;
            }
            let next = tokio::select! {
                _ = &mut interrupt => break,
                next = Self::next_message(consumer, idle) => next,
            };
            let Some(received) = next else {
                break;
            };
            let received = match received {
                Ok(received) => received,
                Err(error) => {
                    if consumer.read_error_ends_consumption(&error).await {
                        return Err(error)
                            .with_context(|| format!("Problem polling consumer group: {group}"));
                    }
                    tracing::error!("Error while polling consumer group {group}: {error}");
                    continue;
                }
            };
            let partition_id = received.partition_id;
            let checkpoint = received.checkpoint();
            let message = received.message;
            match output.as_mut() {
                Some(output) => {
                    let saved = output
                        .store(std::slice::from_ref(&message))
                        .await
                        .with_context(|| {
                            format!("Problem writing message to file: {}", output.path)
                        })?;
                    polled_size += saved;
                }
                None => {
                    write_line(&row(self.show_headers, partition_id, &message))
                        .context("Problem writing to standard output")?;
                    polled_size += message.get_size_bytes();
                }
            }
            if self.auto_commit {
                consumer
                    .mark(checkpoint)
                    .context("Problem marking polled message")?;
            }
            polled_count += 1;
            last_arrival = Instant::now();
        }

        if let Some(output) = output.as_mut() {
            output
                .flush()
                .await
                .with_context(|| format!("Problem writing message to file: {}", output.path))?;
        }
        if !self.follow {
            log_polled(
                polled_count,
                polled_size,
                IggyDuration::new(last_arrival.duration_since(start)),
            );
        }
        Ok(())
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

        log_polled(messages.len(), polled_size, elapsed);

        if let Some(output) = output {
            let output_file = output.path;
            event!(target: PRINT_TARGET, Level::INFO, "Storing messages to {output_file} binary file");

            let saved_size = output
                .store(messages)
                .await
                .with_context(|| format!("Problem writing message to file: {output_file}"))?;
            output
                .flush()
                .await
                .with_context(|| format!("Problem writing message to file: {output_file}"))?;

            let saved_size_str = saved_size.as_human_string();
            let message_count_message = messages_label(messages.len());
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
                            .ok()
                            .flatten()
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

    /// Follow mode prints only rows on stdout, so the banner goes to stderr.
    fn stdout_is_data(&self) -> bool {
        self.follow
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

fn messages_label(count: usize) -> String {
    match count {
        1 => "1 message".into(),
        count => format!("{count} messages"),
    }
}

fn log_polled(count: usize, size: IggyByteSize, elapsed: IggyDuration) {
    let message_count_message = messages_label(count);
    event!(target: PRINT_TARGET, Level::INFO, "Polled {message_count_message} of total size {size}, it took {}", elapsed.as_human_time_string());
}

/// One message as a tab-separated row. Fields are escaped so a tab or newline in a payload
/// cannot split the row.
fn row(show_headers: bool, partition_id: u32, message: &IggyMessage) -> String {
    let mut fields = vec![
        message.header.offset.to_string(),
        partition_id.to_string(),
        IggyTimestamp::from(message.header.timestamp).to_local_string("%Y-%m-%d %H:%M:%S%.6f"),
        message.header.id.to_string(),
        message.payload.len().to_string(),
        String::from_utf8_lossy(&message.payload).into_owned(),
    ];
    if show_headers {
        match message.user_headers_map() {
            Ok(Some(headers)) => fields.extend(headers.iter().map(|(key, value)| {
                format!("{}={}", key.to_string_value(), value.to_string_value())
            })),
            Ok(None) => {}
            Err(error) => tracing::error!("Failed to parse user headers, error: {error}"),
        }
    }
    fields
        .iter()
        .map(|field| escape(field))
        .collect::<Vec<_>>()
        .join("\t")
}

fn escape(field: &str) -> String {
    field
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn write_line(line: &str) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{line}")?;
    stdout.flush()
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

    /// Buffers `messages`. A failed write surfaces at the next [`Self::flush`].
    async fn store(&mut self, messages: &[IggyMessage]) -> std::io::Result<IggyByteSize> {
        let mut saved_size = IggyByteSize::default();
        for message in messages {
            let message = message.to_bytes();
            saved_size += IggyByteSize::from(message.len() as u64);
            self.writer.write_all(&message).await?;
        }
        Ok(saved_size)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush().await
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
        output.flush().await.unwrap();

        assert_eq!(
            tokio::fs::read(path).await.unwrap(),
            stored_bytes(&[&first, &second]),
            "each batch is in the file once it is stored"
        );
    }

    #[test]
    fn given_message_when_formatted_should_be_one_tab_separated_row() {
        let line = row(false, 3, &IggyMessage::from("hello"));

        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[1], "3");
        assert_eq!(fields[4], "5");
        assert_eq!(fields[5], "hello");
    }

    #[test]
    fn given_payload_with_tab_and_newline_when_formatted_should_escape_them() {
        let line = row(false, 0, &IggyMessage::from("a\tb\nc"));

        assert_eq!(line.split('\t').count(), 6);
        assert!(line.ends_with("a\\tb\\nc"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn given_a_full_device_when_flushing_a_batch_should_fail() {
        let mut output = OutputFile::open("/dev/full").await.unwrap();
        output.store(&batch(&["lost"])).await.unwrap();

        let error = output
            .flush()
            .await
            .expect_err("a write that the device refused must fail the command");

        assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    }
}
