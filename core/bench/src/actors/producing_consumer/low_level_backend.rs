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

use iggy::prelude::*;
use integration::test_server::login_root;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::{
    actors::{
        consumer::backend::ConsumedBatch,
        producer::backend::ProducedBatch,
        producing_consumer::backend::{BenchmarkProducingConsumerBackend, LowLevelBackend},
    },
    benchmarks::common::create_consumer,
    utils::{batch_total_size_bytes, batch_user_size_bytes},
};

impl BenchmarkProducingConsumerBackend for LowLevelBackend {
    type MessagingContext = (
        IggyClient,
        Identifier,   // stream_id
        Identifier,   // topic_id
        Partitioning, // producer
        Option<u32>,  // partition_id
        Consumer,     // consumer
        u64,          // offset
    );
    async fn setup(&self) -> Result<Self::MessagingContext, IggyError> {
        let topic_id: u32 = 1;
        let default_partition_id: u32 = 1;

        let client = self.client_factory.create_client().await;
        let client = IggyClient::create(client, None, None);
        login_root(&client).await;

        let stream_id = self.config.stream_id.try_into().unwrap();
        let topic_id = topic_id.try_into().unwrap();

        let partitioning = match self.config.partitions_count {
            0 => panic!("Partition count must be greater than 0"),
            1 => Partitioning::partition_id(default_partition_id),
            _ => Partitioning::balanced(),
        };

        let partition_id = if self.config.consumer_group_id.is_some() {
            None
        } else {
            Some(default_partition_id)
        };

        let consumer = create_consumer(
            &client,
            self.config.consumer_group_id.as_ref(),
            &stream_id,
            &topic_id,
            self.config.actor_id,
        )
        .await;

        Ok((
            client,
            stream_id,
            topic_id,
            partitioning,
            partition_id,
            consumer,
            0,
        ))
    }

    #[allow(clippy::cognitive_complexity)]
    async fn warmup(
        &self,
        messaging_context: &mut Self::MessagingContext,
        batch_generator: &mut crate::utils::batch_generator::BenchmarkBatchGenerator,
    ) -> Result<(), IggyError> {
        let (client, stream_id, topic_id, partitioning, partition_id, consumer_obj, offset) =
            messaging_context;

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
            let batch = batch_generator.generate_batch();
            client
                .send_messages(stream_id, topic_id, partitioning, &mut batch.messages)
                .await?;

            let (strategy, auto_commit) = match self.config.polling_kind {
                PollingKind::Offset => (PollingStrategy::offset(*offset), false),
                PollingKind::Next => (PollingStrategy::next(), true),
                other => panic!("Unsupported polling kind for warmup: {other:?}"),
            };

            let polled_messages = client
                .poll_messages(
                    stream_id,
                    topic_id,
                    *partition_id,
                    consumer_obj,
                    &strategy,
                    u32::try_from(batch.messages.len()).unwrap_or(u32::MAX),
                    auto_commit,
                )
                .await?;

            if polled_messages.messages.is_empty() {
                let should_warn =
                    last_warning_time.is_none_or(|t| t.elapsed() >= Duration::from_secs(1));

                if should_warn {
                    warn!(
                        "ProducingConsumer #{} (warmup) → expected {} messages but got {}, retrying... ({} warnings skipped)",
                        self.config.actor_id,
                        self.config.messages_per_batch,
                        polled_messages.messages.len(),
                        skipped_warnings_count
                    );
                    last_warning_time = Some(Instant::now());
                    skipped_warnings_count = 0;
                } else {
                    skipped_warnings_count += 1;
                }

                continue;
            }

            *offset += batch.messages.len() as u64;
        }

        Ok(())
    }

    async fn produce_batch(
        &self,
        messaging_context: &mut Self::MessagingContext,
        batch_generator: &mut crate::utils::batch_generator::BenchmarkBatchGenerator,
    ) -> Result<Option<crate::actors::producer::backend::ProducedBatch>, IggyError> {
        let (client, stream_id, topic_id, partitioning, _partition_id, _consumer, _offset) =
            messaging_context;

        let batch = batch_generator.generate_batch();
        let message_count = u32::try_from(batch.messages.len()).unwrap();
        let before_send = std::time::Instant::now();
        client
            .send_messages(stream_id, topic_id, partitioning, &mut batch.messages)
            .await?;

        Ok(Some(ProducedBatch {
            messages: message_count,
            user_data_bytes: batch.user_data_bytes,
            total_bytes: batch.total_bytes,
            latency: before_send.elapsed(),
        }))
    }

    async fn consume_batch(
        &self,
        messaging_context: &mut Self::MessagingContext,
    ) -> Result<Option<crate::actors::consumer::backend::ConsumedBatch>, IggyError> {
        let (client, stream_id, topic_id, _partitioning, partition_id, consumer, offset) =
            messaging_context;

        let (strategy, auto_commit) = match self.config.polling_kind {
            PollingKind::Offset => (PollingStrategy::offset(*offset), false),
            PollingKind::Next => (PollingStrategy::next(), true),
            ref other => panic!("Unsupported polling kind for benchmark: {other:?}"),
        };
        let before_poll = Instant::now();
        let polled_messages = client
            .poll_messages(
                stream_id,
                topic_id,
                *partition_id,
                consumer,
                &strategy,
                self.config.messages_per_batch.max(),
                auto_commit,
            )
            .await?;

        if polled_messages.messages.is_empty() {
            return Ok(None);
        }

        let latency = if self.config.origin_timestamp_latency_calculation {
            let now = IggyTimestamp::now().as_micros();
            Duration::from_micros(now - polled_messages.messages[0].header.origin_timestamp)
        } else {
            before_poll.elapsed()
        };

        let user_data_bytes = batch_user_size_bytes(&polled_messages);
        let total_bytes = batch_total_size_bytes(&polled_messages);
        *offset += polled_messages.messages.len() as u64;

        Ok(Some(ConsumedBatch {
            messages: u32::try_from(polled_messages.messages.len()).unwrap(),
            user_data_bytes,
            total_bytes,
            latency,
        }))
    }

    fn log_setup_info(&self) {
        let actor_id = self.config.actor_id;
        let stream_id = self.config.stream_id;
        let partitions = self.config.partitions_count;
        let batch_size = self.config.messages_per_batch;
        let message_size = self.config.message_size;

        if let Some(cg_id) = self.config.consumer_group_id {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch (each {} bytes) on stream {} across {} partition(s), as part of consumer group #{} using low-level API...",
                actor_id, batch_size, message_size, stream_id, partitions, cg_id,
            );
        } else {
            info!(
                "ProducingConsumer #{} → sending & polling {} messages per batch (each {} bytes) on stream {} across {} partition(s), using low-level API...",
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
