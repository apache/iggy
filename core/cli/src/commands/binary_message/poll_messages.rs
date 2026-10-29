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
use iggy::prelude::{AutoCommit, AutoCommitWhen, IggyConsumer, IggyError, ReceivedMessage};
use iggy_binary_protocol::WireUserHeaders;
use iggy_common::Client;
use iggy_common::{
    Consumer, HeaderKey, HeaderKind, Identifier, IggyByteSize, IggyDuration, IggyMessage,
    IggyTimestamp, PollMessages, PollingStrategy, Sizeable,
    wire_conversions::user_headers_from_wire,
};
use std::collections::HashSet;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time;
use tracing::{Level, event};

/// Idle time after which a group poll without `--follow` stops.
const GROUP_IDLE_WAIT: Duration = Duration::from_secs(5);

pub struct PollMessagesCmd {
    poll_messages: PollMessages,
    show_headers: bool,
    output_file: Option<String>,
    consumer_group: Option<String>,
    follow: bool,
}

fn message_count_text(count: usize) -> String {
    match count {
        1 => "1 message".into(),
        count => format!("{count} messages"),
    }
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
        Self {
            poll_messages: PollMessages {
                consumer: Consumer::new(consumer),
                stream_id,
                topic_id,
                partition_id,
                strategy,
                count: message_count,
                auto_commit,
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
        let auto_commit = if self.poll_messages.auto_commit {
            AutoCommit::When(AutoCommitWhen::ConsumingEachMessage)
        } else {
            AutoCommit::Disabled
        };
        // Stream and topic are overwritten below, since the builder only takes names.
        let consumer = client
            .consumer_group(group, group, group)
            .with_context(|| format!("Invalid consumer group name: {group}"))?
            .stream(self.poll_messages.stream_id.clone())
            .topic(self.poll_messages.topic_id.clone())
            .polling_strategy(self.poll_messages.strategy)
            .batch_length(self.poll_messages.count)
            .auto_commit(auto_commit)
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

        let count = self.poll_messages.count as usize;
        let idle = (!self.follow).then_some(GROUP_IDLE_WAIT);
        let mut messages = Vec::with_capacity(count);
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
                self.output_messages(std::slice::from_ref(&message)).await?;
            } else {
                messages.push(message);
            }
        }

        if !self.follow {
            event!(target: PRINT_TARGET, Level::INFO, "Polled {} from consumer group: {group}", message_count_text(messages.len()));
            self.output_messages(&messages).await?;
        }
        Ok(())
    }

    async fn next_message(
        consumer: &mut IggyConsumer,
        idle: Option<Duration>,
    ) -> Option<Result<ReceivedMessage, IggyError>> {
        match idle {
            None => consumer.next().await,
            Some(idle) => time::timeout(idle, consumer.next()).await.ok().flatten(),
        }
    }

    async fn output_messages(&self, messages: &[IggyMessage]) -> anyhow::Result<()> {
        if let Some(output_file) = &self.output_file {
            event!(target: PRINT_TARGET, Level::INFO, "Storing messages to {output_file} binary file");

            let mut saved_size = IggyByteSize::default();
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(output_file)
                .await
                .with_context(|| format!("Problem opening file for writing: {output_file}"))?;

            for message in messages.iter() {
                let message = message.to_bytes();
                saved_size += IggyByteSize::from(message.len() as u64);
                file.write_all(&message)
                    .await
                    .with_context(|| format!("Problem writing message to file: {output_file}"))?;
            }

            let saved_size_str = saved_size.as_human_string();
            let message_count_message = message_count_text(messages.len());
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

        let message_count_message = message_count_text(polled_messages.messages.len());
        event!(target: PRINT_TARGET, Level::INFO, "Polled {message_count_message} of total size {polled_size}, it took {}", elapsed.as_human_time_string());

        self.output_messages(&polled_messages.messages).await
    }

    async fn execute_with_iggy_client(&mut self, client: &IggyClient) -> anyhow::Result<()> {
        match self.consumer_group.clone() {
            Some(group) => self.poll_consumer_group(client, &group).await,
            None => self.execute_cmd(client).await,
        }
    }
}
