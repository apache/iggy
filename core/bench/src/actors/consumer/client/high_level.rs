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
        interface::{BenchmarkConsumerConfig, ConsumerClient},
    },
};

use crate::utils::ClientFactory;
use futures_util::StreamExt;
use iggy::prelude::*;
use std::sync::Arc;
use tokio::time::{Instant, timeout};
use tracing::{error, warn};

const TOPIC_ID: &str = "topic-1";
const PARTITION_ID: u32 = 0;
const PREFETCH_BATCHES: u32 = 16;

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

    fn build_consumer(&self, client: &IggyClient) -> Result<IggyConsumer, IggyError> {
        let stream_id_str = &self.config.stream_id;
        let options = PollOptions {
            count: self.config.messages_per_batch.get(),
            ..self.config.poll_options
        };
        let builder = if let Some(cg_id) = self.config.consumer_group_id {
            client
                .consumer_group(&format!("cg_{cg_id}"), stream_id_str, TOPIC_ID)?
                .polling_strategy(PollingStrategy::next())
                .poll_options(PollOptions {
                    commit: CommitPolicy::OnRead,
                    ..options
                })
                .create_consumer_group_if_not_exists()
        } else {
            client
                .consumer(
                    &format!("hl_consumer_{}", self.config.consumer_id),
                    stream_id_str,
                    TOPIC_ID,
                )?
                .partitions([PARTITION_ID])
                .polling_strategy(PollingStrategy::offset(0))
                .poll_options(options)
        };
        Ok(builder
            .prefetch_messages(options.count.saturating_mul(PREFETCH_BATCHES))
            .prefetch_bytes(options.max_bytes.saturating_mul(PREFETCH_BATCHES))
            .build())
    }
}

impl ConsumerClient for HighLevelConsumerClient {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "The borrowed consumer retains reservations until its buffered messages are drained."
    )]
    async fn consume_batch(&mut self) -> Result<Option<BatchMetrics>, IggyError> {
        let consumer = self.consumer.as_mut().expect("Consumer not initialized");

        // One poll can wait up to its request timeout, so a shorter timeout would restart the
        // batch clock in the middle of a deliberate wait.
        let poll_timeout = self.config.poll_options.request_timeout().get_duration();
        let batch_start = Instant::now();
        let mut batch_messages = 0;
        let mut batch_user_bytes = 0;
        let mut batch_total_bytes = 0;

        while batch_messages < self.config.messages_per_batch.get() {
            let timeout_result = timeout(poll_timeout, consumer.next()).await;

            match timeout_result {
                Ok(Some(message_result)) => match message_result {
                    Ok(received_message) => {
                        batch_messages += 1;
                        batch_user_bytes += received_message.message.payload.len() as u64;
                        batch_total_bytes +=
                            received_message.message.get_size_bytes().as_bytes_u64();

                        let offset = received_message.message.header.offset;

                        if batch_messages >= self.config.messages_per_batch.get() {
                            // Group members commit on every poll. A second commit at a batch
                            // boundary inside a poll trails the last polled offset and holds a
                            // cooperative handoff open until the rebalance timeout.
                            if self.config.consumer_group_id.is_none()
                                && let Err(error) = consumer.store_offset(offset, None).await
                            {
                                error!("Failed to store offset: {offset}. {error}");
                            }
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
        let consumer = self.consumer.as_mut().expect("Consumer not initialized");
        let Some(topic) = client
            .get_topic(consumer.stream(), consumer.topic())
            .await?
        else {
            return Err(IggyError::TopicNameNotFound(
                TOPIC_ID.to_owned(),
                self.config.stream_id.clone(),
            ));
        };

        // A replacement consumer would leave the group and join it again, and each change
        // rebalances the members that already measure. The consumer exposes no assignment, so
        // it seeks every partition, and `seek` refuses one that it does not read.
        for partition in topic.partitions {
            match consumer
                .seek(partition.id, PollingStrategy::offset(0))
                .await
            {
                Ok(()) | Err(IggyError::InvalidCommand) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl BenchmarkInit for HighLevelConsumerClient {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "The initialized consumer moves into self and must retain its prefetch reservations."
    )]
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
