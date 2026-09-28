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

use crate::actors::{
    ApiLabel, BatchMetrics, BenchmarkInit,
    consumer::client::{
        BenchmarkConsumerClient,
        interface::{BenchmarkConsumerConfig, ConsumerClient, clear_consumer_offsets},
    },
};

use crate::utils::ClientFactory;
use futures_util::StreamExt;
use iggy::prelude::*;
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};
use tracing::warn;

const TOPIC_ID: &str = "topic-1";

pub struct HighLevelConsumerClient {
    client_factory: Arc<dyn ClientFactory>,
    config: BenchmarkConsumerConfig,
    /// `IggyConsumer` only borrows the transport, so the owning client has to
    /// outlive it: dropping the client aborts the SDK heartbeat task, and the
    /// consumer stops being pinged for the rest of the run. Boxed because the
    /// low-level client is the smaller of `TypedBenchmarkConsumer`'s two
    /// variants, and inlining the client here spreads them past what
    /// `clippy::large_enum_variant` allows.
    client: Option<Box<IggyClient>>,
    consumer: Option<IggyConsumer>,
}

impl HighLevelConsumerClient {
    pub fn new(client_factory: Arc<dyn ClientFactory>, config: BenchmarkConsumerConfig) -> Self {
        Self {
            client_factory,
            config,
            client: None,
            consumer: None,
        }
    }

    /// The name the SDK registers for this actor, which is also its identity inside a group.
    /// Building the consumer and clearing its offsets have to agree on it.
    fn consumer_name(&self) -> String {
        self.config.consumer_group_id.map_or_else(
            || format!("hl_consumer_{}", self.config.consumer_id),
            |cg_id| format!("cg_{cg_id}"),
        )
    }

    /// The consumer the SDK registers for this actor, rebuilt from the config: the built
    /// `IggyConsumer` keeps its own copy, and the offset reset has to name the same one.
    fn consumer_identifier(&self) -> Result<Consumer, IggyError> {
        let name = self.consumer_name();
        Ok(match self.config.consumer_group_id {
            Some(_) => Consumer::group(name.as_str().try_into()?),
            None => Consumer::new(name.as_str().try_into()?),
        })
    }

    fn build_consumer(&self, client: &IggyClient) -> Result<IggyConsumer, IggyError> {
        let name = self.consumer_name();
        let stream_id_str = &self.config.stream_id;
        let builder = if self.config.consumer_group_id.is_some() {
            client
                .consumer_group(&name, stream_id_str, TOPIC_ID)?
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .create_consumer_group_if_not_exists()
                .auto_join_consumer_group()
        } else {
            client
                .consumer(&name, stream_id_str, TOPIC_ID, 0)?
                .polling_strategy(PollingStrategy::offset(0))
                .auto_commit(AutoCommit::Disabled)
        };
        Ok(builder
            .batch_length(self.config.messages_per_batch.get())
            .build())
    }
}

impl ConsumerClient for HighLevelConsumerClient {
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        let consumer = self.consumer.as_mut().expect("Consumer not initialized");

        let batch_start = Instant::now();
        let mut batch_messages = 0;
        let mut batch_user_bytes = 0;
        let mut batch_total_bytes = 0;

        while batch_messages < self.config.messages_per_batch.get() {
            let timeout_result = timeout(Duration::from_secs(1), consumer.next()).await;

            match timeout_result {
                Ok(Some(message_result)) => match message_result {
                    Ok(received_message) => {
                        batch_messages += 1;
                        batch_user_bytes += received_message.message.payload.len() as u64;
                        batch_total_bytes +=
                            received_message.message.get_size_bytes().as_bytes_u64();

                        if batch_messages >= self.config.messages_per_batch.get() {
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error receiving message: {}", err);
                    }
                },
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }

        if batch_messages == 0 {
            Ok(None)
        } else {
            Ok(Some(BatchMetrics {
                messages: batch_messages,
                user_data_bytes: batch_user_bytes,
                total_bytes: batch_total_bytes,
                latency: batch_start.elapsed(),
            }))
        }
    }

    async fn reset_offsets(&mut self) -> Result<(), IggyError> {
        let client = self.client.as_deref().expect("Client not initialized");

        let mut replacement = self.build_consumer(client)?;

        // Dropping the old consumer stops its polling. A bare drop does not leave the
        // group, so the re-join below is the same member and keeps its partitions.
        self.consumer = None;

        if self.config.consumer_group_id.is_some() {
            clear_consumer_offsets(
                client,
                &self.consumer_identifier()?,
                replacement.stream(),
                replacement.topic(),
            )
            .await?;
        }

        replacement.init().await?;
        self.consumer = Some(replacement);
        Ok(())
    }
}

impl BenchmarkInit for HighLevelConsumerClient {
    async fn setup(&mut self) -> Result<(), IggyError> {
        let client = self.client_factory.create_authenticated_client().await?;
        let mut consumer = self.build_consumer(&client)?;
        consumer.init().await?;
        self.consumer = Some(consumer);
        self.client = Some(Box::new(client));
        Ok(())
    }
}

impl ApiLabel for HighLevelConsumerClient {
    const API_LABEL: &'static str = "high-level";
}

impl BenchmarkConsumerClient for HighLevelConsumerClient {}
