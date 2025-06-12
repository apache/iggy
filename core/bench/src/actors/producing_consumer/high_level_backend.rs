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

use std::time::Duration;

use futures_util::StreamExt;
use iggy::prelude::*;
use integration::test_server::login_root;
use tokio::time::{Instant, timeout};
use tracing::{debug, error, info, warn};

use crate::actors::{
    consumer::backend::ConsumedBatch,
    producer::backend::ProducedBatch,
    producing_consumer::backend::{BenchmarkProducingConsumerBackend, HighLevelBackend},
};

impl BenchmarkProducingConsumerBackend for HighLevelBackend {
    type MessagingContext = (IggyConsumer, IggyProducer);

    async fn setup(&self) -> Result<Self::MessagingContext, iggy::prelude::IggyError> {
        let topic_id: u32 = 1;
        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id_str = self.config.stream_id.to_string();
        let topic_id_str = topic_id.to_string();

        // Setup producer
        let iggy_producer = client
            .producer(&stream_id_str, &topic_id_str)?
            .create_stream_if_not_exists()
            .create_topic_if_not_exists(
                self.config.partitions_count,
                Some(1),
                IggyExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .build();
        iggy_producer.init().await?;

        // Setup consumer
        let mut iggy_consumer = if let Some(cg_id) = self.config.consumer_group_id {
            let consumer_group_name = format!("cg_{cg_id}");
            client
                .consumer_group(&consumer_group_name, &stream_id_str, &topic_id_str)?
                .batch_length(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
                .create_consumer_group_if_not_exists()
                .auto_join_consumer_group()
                .build()
        } else {
            client
                .consumer(
                    &format!("hl_consumer_{}", self.config.actor_id),
                    &stream_id_str,
                    &topic_id_str,
                    1,
                )?
                .polling_strategy(PollingStrategy::offset(0))
                .batch_length(self.config.messages_per_batch.get())
                .auto_commit(AutoCommit::Disabled)
                .build()
        };
        iggy_consumer.init().await?;

        Ok((iggy_consumer, iggy_producer))
    }

    #[allow(clippy::cognitive_complexity)]
    async fn warmup(
        &self,
        messaging_context: &mut Self::MessagingContext,
        batch_generator: &mut crate::utils::batch_generator::BenchmarkBatchGenerator,
    ) -> Result<(), iggy::prelude::IggyError> {
        let (consumer, producer) = messaging_context;
        let warmup_end = Instant::now() + self.config.warmup_time.get_duration();
        let mut last_warning_time: Option<Instant> = None;
        let mut skipped_warnings_count: u32 = 0;

        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "ProducingConsumer #{}, part of consumer group #{}, → warming up for {}...",
                self.config.actor_id, cg_id, self.config.warmup_time
            );
        } else {
            info!(
                "ProducingConsumer #{} → warming up for {}...",
                self.config.actor_id, self.config.warmup_time
            );
        }

        while Instant::now() < warmup_end {
            let batch = batch_generator.generate_owned_batch();
            if batch.messages.is_empty() {
                continue;
            }

            producer.send(batch.messages).await?;

            match consumer.next().await {
                Some(Ok(_message)) => {}
                Some(Err(err)) => {
                    warn!(
                        "ProducingConsumer #{} (warmup) → got error while polling message: {err}, aborting...",
                        self.config.actor_id
                    );
                    break;
                }
                None => {
                    let should_warn =
                        last_warning_time.is_none_or(|t| t.elapsed() >= Duration::from_secs(1));

                    if should_warn {
                        warn!(
                            "ProducingConsumer #{} (warmup) → no message received, retrying... ({} warnings skipped)",
                            self.config.actor_id, skipped_warnings_count
                        );
                        last_warning_time = Some(Instant::now());
                        skipped_warnings_count = 0;
                    } else {
                        skipped_warnings_count += 1;
                    }
                }
            }
        }
        Ok(())
    }

    async fn produce_batch(
        &self,
        messaging_context: &mut Self::MessagingContext,
        batch_generator: &mut crate::utils::batch_generator::BenchmarkBatchGenerator,
    ) -> Result<Option<crate::actors::producer::backend::ProducedBatch>, iggy::prelude::IggyError>
    {
        let (_consumer, producer) = messaging_context;
        let batch = batch_generator.generate_owned_batch();
        if batch.messages.is_empty() {
            return Ok(None);
        }
        let message_count = u32::try_from(batch.messages.len()).unwrap();
        let user_data_bytes = batch.user_data_bytes;
        let total_bytes = batch.total_bytes;

        let before_send = Instant::now();

        producer.send(batch.messages).await?;

        let latency = before_send.elapsed();

        Ok(Some(ProducedBatch {
            messages: message_count,
            user_data_bytes,
            total_bytes,
            latency,
        }))
    }

    #[allow(clippy::cognitive_complexity)]
    async fn consume_batch(
        &self,
        messaging_context: &mut Self::MessagingContext,
    ) -> Result<Option<crate::actors::consumer::backend::ConsumedBatch>, iggy::prelude::IggyError>
    {
        let (consumer, _) = messaging_context;
        let batch_start = Instant::now();
        let mut batch_messages = 0;
        let mut batch_user_bytes = 0;
        let mut batch_total_bytes = 0;

        while batch_messages < self.config.messages_per_batch.get() {
            // Use timeout to avoid getting stuck waiting for messages
            let timeout_result = timeout(Duration::from_secs(1), consumer.next()).await;

            match timeout_result {
                Ok(Some(message_result)) => match message_result {
                    Ok(received_message) => {
                        batch_messages += 1;
                        batch_user_bytes += received_message.message.payload.len() as u64;
                        batch_total_bytes +=
                            received_message.message.get_size_bytes().as_bytes_u64();

                        let offset = received_message.message.header.offset;
                        if batch_messages >= self.config.messages_per_batch.get() {
                            info!(
                                "Batch of {} messages consumed, last_offset: {}, current_offset: {}",
                                batch_messages,
                                received_message.message.header.offset,
                                received_message.current_offset
                            );

                            if let Err(error) = consumer.store_offset(offset, None).await {
                                error!("Failed to store offset: {offset}. {error}");
                                continue;
                            }
                            debug!("Offset: {offset} stored successfully");
                            break;
                        }
                    }
                    Err(err) => {
                        warn!("Error receiving message: {}", err);
                    }
                },
                Ok(None) => {
                    debug!("Consumer stream ended during batching");
                    break;
                }
                Err(_) => {
                    debug!(
                        "Timeout waiting for messages, stopping batch at {} messages",
                        batch_messages
                    );
                    break;
                }
            }
        }

        if batch_messages == 0 {
            Ok(None)
        } else {
            Ok(Some(ConsumedBatch {
                messages: batch_messages,
                user_data_bytes: batch_user_bytes,
                total_bytes: batch_total_bytes,
                latency: batch_start.elapsed(),
            }))
        }
    }

    fn log_setup_info(&self) {
        let actor_id = self.config.actor_id;
        let stream_id = self.config.stream_id;
        let partitions = self.config.partitions_count;
        let batch_size = self.config.messages_per_batch;
        let message_size = self.config.message_size;

        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch (each {} bytes) on stream {} across {} partition(s), as part of consumer group #{} using high-level API...",
                actor_id, batch_size, message_size, stream_id, partitions, cg_id,
            );
        } else {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch (each {} bytes) on stream {} across {} partition(s), using high-level API...",
                actor_id, batch_size, message_size, stream_id, partitions,
            );
        }
    }

    fn log_warmup_info(&self) {
        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "ProducingConsumer #{}, part of consumer group #{} → warming up for {}...",
                self.config.actor_id, cg_id, self.config.warmup_time
            );
        } else {
            info!(
                "ProducingConsumer #{} → warming up for {}...",
                self.config.actor_id, self.config.warmup_time
            );
        }
    }
}
